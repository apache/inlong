//
// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements.  See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to You under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License.  You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package dataproxy

import (
	"context"
	"strconv"
	"sync"
	"time"

	"github.com/apache/inlong/inlong-sdk/dataproxy-sdk-twins/dataproxy-sdk-golang/logger"
	"github.com/apache/inlong/inlong-sdk/dataproxy-sdk-twins/dataproxy-sdk-golang/syncx"
	"github.com/apache/inlong/inlong-sdk/dataproxy-sdk-twins/dataproxy-sdk-golang/util"
	"go.uber.org/atomic"
)

type HTTPBatchReq struct {
	GroupID   string
	StreamID  string
	Bodies    []string
	DataTime  int64
	DataReqs  []*sendDataReq
	BatchTime time.Time
	Retries   int
}

type HTTPWorker struct {
	client         *HTTPClient
	index          int
	indexStr       string
	options        *Options
	log            logger.Logger
	metrics        *metrics
	state          atomic.Int32
	dataChan       chan *sendDataReq
	dataSemaphore  syncx.Semaphore
	pendingBatches map[string]*HTTPBatchReq
	retryBatches   chan *HTTPBatchReq
	batchTicker    *time.Ticker
	stop           chan struct{}
	closeOnce      sync.Once
}

// NewHTTPWorker creates a new HTTPWorker instance
func NewHTTPWorker(client *HTTPClient, index int, options *Options, log logger.Logger, metrics *metrics) *HTTPWorker {
	w := &HTTPWorker{
		client:         client,
		index:          index,
		indexStr:       strconv.Itoa(index),
		options:        options,
		log:            log,
		metrics:        metrics,
		dataChan:       make(chan *sendDataReq, options.MaxPendingMessages),
		dataSemaphore:  syncx.NewSemaphore(int32(options.MaxPendingMessages)),
		pendingBatches: make(map[string]*HTTPBatchReq),
		retryBatches:   make(chan *HTTPBatchReq, options.MaxPendingMessages),
		batchTicker:    time.NewTicker(options.BatchingMaxPublishDelay),
		stop:           make(chan struct{}),
	}

	w.state.Store(int32(stateReady))
	go w.run()
	return w
}

// Send a message synchronously
func (w *HTTPWorker) Send(ctx context.Context, msg Message) error {
	var err error
	isDone := atomic.NewBool(false)
	doneCh := make(chan struct{})

	w.SendAsync(ctx, msg, func(msg Message, e error) {
		if isDone.CompareAndSwap(false, true) {
			err = e
			close(doneCh)
		}
	})

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-doneCh:
		return err
	}
}

// SendAsync sends a message asynchronously
func (w *HTTPWorker) SendAsync(ctx context.Context, msg Message, callback Callback) {
	req := &sendDataReq{
		ctx:              ctx,
		msg:              msg,
		callback:         callback,
		flushImmediately: false,
		publishTime:      time.Now(),
		metrics:          w.metrics,
		workerID:         w.indexStr,
	}

	if len(msg.Payload) == 0 {
		if callback != nil {
			callback(msg, errBadLog)
		}
		return
	}

	// worker has been closed
	if workerState(w.state.Load()) != stateReady {
		if callback != nil {
			callback(msg, errProducerClosed)
		}
		return
	}

	if w.options.BlockIfQueueIsFull {
		if !w.dataSemaphore.Acquire(ctx) {
			if callback != nil {
				callback(msg, errContextExpired)
			}
			return
		}
	} else {
		if !w.dataSemaphore.TryAcquire() {
			if callback != nil {
				callback(msg, errSendQueueIsFull)
			}
			return
		}
	}

	req.semaphore = w.dataSemaphore

	select {
	case w.dataChan <- req:
		w.metrics.incPending(w.indexStr)
	case <-ctx.Done():
		w.dataSemaphore.Release()
		if callback != nil {
			callback(msg, ctx.Err())
		}
	}
}

// run main loop for the HTTP worker
func (w *HTTPWorker) run() {
	defer func() {
		w.batchTicker.Stop()
		close(w.dataChan)
		close(w.retryBatches)
	}()

	for {
		select {
		case <-w.stop:
			w.flushAllPendingBatches()
			return
		case req, ok := <-w.dataChan:
			if !ok {
				return
			}
			w.handleSendData(req)
		case batch, ok := <-w.retryBatches:
			if !ok {
				return
			}
			w.handleRetry(batch)
		case <-w.batchTicker.C:
			w.handleBatchTimeout()
		}
	}
}

func (w *HTTPWorker) handleRetry(batch *HTTPBatchReq) {
	w.log.Debugf("Retrying HTTP batch %s, attempt %d", batch.StreamID, batch.Retries)
	w.sendBatch(batch)
}

// handleSendData handles sending data requests
func (w *HTTPWorker) handleSendData(req *sendDataReq) {
	batch, ok := w.pendingBatches[req.msg.StreamID]
	if !ok {
		batch = &HTTPBatchReq{
			GroupID:   w.options.GroupID,
			StreamID:  req.msg.StreamID,
			Bodies:    make([]string, 0, w.options.BatchingMaxMessages),
			DataTime:  time.Now().UnixMilli(),
			DataReqs:  make([]*sendDataReq, 0, w.options.BatchingMaxMessages),
			BatchTime: time.Now(),
			Retries:   0,
		}
		w.pendingBatches[req.msg.StreamID] = batch
	}

	batch.Bodies = append(batch.Bodies, string(req.msg.Payload))
	batch.DataReqs = append(batch.DataReqs, req)

	if req.flushImmediately ||
		len(batch.Bodies) >= w.options.BatchingMaxMessages ||
		w.calculateBatchSize(batch) >= w.options.BatchingMaxSize {
		w.sendBatch(batch)
		delete(w.pendingBatches, batch.StreamID)
	}
}

// sendBatch send a batch of messages to the HTTP server
func (w *HTTPWorker) sendBatch(batch *HTTPBatchReq) {
	if batch.Retries > w.options.MaxRetries {
		w.finishBatch(batch, errSendTimeout)
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), w.options.HTTPTimeout)
	defer cancel()

	err := w.client.SendBatch(ctx, batch)
	if err == nil {
		w.finishBatch(batch, nil)
		return
	}

	w.log.Warnf("HTTP send failed: %v", err)
	batch.Retries++
	w.backoffRetry(batch)
}

func (w *HTTPWorker) backoffRetry(batch *HTTPBatchReq) {
	if batch.Retries > w.options.MaxRetries {
		w.finishBatch(batch, errSendTimeout)
		return
	}

	// check if the worker is still ready to process
	if workerState(w.state.Load()) != stateReady {
		w.finishBatch(batch, errProducerClosed)
		return
	}

	go func() {
		// use exponential backoff for retry
		backoff := util.ExponentialBackoff{
			InitialInterval: 100 * time.Millisecond,
			MaxInterval:     5 * time.Second,
			Multiplier:      2.0,
			Randomization:   0.1,
		}

		waitTime := backoff.Next(batch.Retries)
		w.log.Debugf("HTTP retry %d after %v for batch %s", batch.Retries, waitTime, batch.StreamID)

		select {
		case <-time.After(waitTime):
			// check if the worker is still ready to process
			if workerState(w.state.Load()) != stateReady {
				w.finishBatch(batch, errProducerClosed)
				return
			}

			// let the worker handle the retry
			select {
			case w.retryBatches <- batch:
				w.metrics.incRetry(w.indexStr)
			default:
				w.finishBatch(batch, errSendQueueIsFull)
			}
		case <-w.stop:
			w.finishBatch(batch, errProducerClosed)
		}
	}()
}

// finishBatch finishes the batch processing
func (w *HTTPWorker) finishBatch(batch *HTTPBatchReq, err error) {
	for _, req := range batch.DataReqs {
		if req.callback != nil {
			req.callback(req.msg, err)
		}
		if req.semaphore != nil {
			req.semaphore.Release()
		}
		w.metrics.decPending(req.workerID)
		if err == nil {
			w.metrics.incMessage(errOK.strCode)
		} else {
			w.metrics.incError(getErrorCode(err))
		}
	}
}

// handleBatchTimeout handles timeout for pending batches
func (w *HTTPWorker) handleBatchTimeout() {
	for streamID, batch := range w.pendingBatches {
		if time.Since(batch.BatchTime) > w.options.BatchingMaxPublishDelay {
			w.sendBatch(batch)
			delete(w.pendingBatches, streamID)
		}
	}
}

// calculateBatchSize calculates the size of a batch
func (w *HTTPWorker) calculateBatchSize(batch *HTTPBatchReq) int {
	size := 0
	for _, body := range batch.Bodies {
		size += len(body)
	}
	return size
}

// flushAllPendingBatches flushes all pending batches
func (w *HTTPWorker) flushAllPendingBatches() {
	for streamID, batch := range w.pendingBatches {
		w.sendBatch(batch)
		delete(w.pendingBatches, streamID)
	}
}

// Available checks if the worker is available to send data
func (w *HTTPWorker) Available() bool {
	return w.dataSemaphore.Available() > 0 && workerState(w.state.Load()) == stateReady
}

// Close closes the HTTP worker, stopping it from processing any further requests
func (w *HTTPWorker) Close() {
	w.closeOnce.Do(func() {
		w.state.Store(int32(stateClosing))
		close(w.stop)
	})
}
