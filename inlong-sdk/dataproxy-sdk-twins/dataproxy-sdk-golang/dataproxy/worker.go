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
	"errors"
	"math/rand"
	"runtime/debug"
	"strconv"
	"time"

	"github.com/google/uuid"

	"github.com/panjf2000/gnet/v2"
	"go.uber.org/atomic"

	"github.com/apache/inlong/inlong-sdk/dataproxy-sdk-twins/dataproxy-sdk-golang/bufferpool"
	"github.com/apache/inlong/inlong-sdk/dataproxy-sdk-twins/dataproxy-sdk-golang/logger"
	"github.com/apache/inlong/inlong-sdk/dataproxy-sdk-twins/dataproxy-sdk-golang/syncx"
	"github.com/apache/inlong/inlong-sdk/dataproxy-sdk-twins/dataproxy-sdk-golang/util"
)

const (
	defaultHeartbeatInterval = 60
	defaultMapCleanInterval  = 20
	defaultMapCleanThreshold = 500000
)

// retryableServerErrorCodes defines which server error codes should trigger retry
// and whether connection switch is needed.
var retryableServerErrorCodes = map[int]bool{
	2: true, // SERVICE_CLOSED
}

type workerState int32

const (
	// worker states
	stateInit = iota
	stateReady
	stateClosing
	stateClosed
)

var (
	errOK               = &errNo{code: 0, strCode: "0", message: "OK", serverErrCode: -1}
	errSendTimeout      = &errNo{code: 10001, strCode: "10001", message: "message send timeout", serverErrCode: -1}
	errSendFailed       = &errNo{code: 10002, strCode: "10002", message: "message send failed", serverErrCode: -1} //nolint:unused
	errProducerClosed   = &errNo{code: 10003, strCode: "10003", message: "producer already been closed", serverErrCode: -1}
	errSendQueueIsFull  = &errNo{code: 10004, strCode: "10004", message: "producer send queue is full", serverErrCode: -1}
	errContextExpired   = &errNo{code: 10005, strCode: "10005", message: "message context expired", serverErrCode: -1}
	errNewConnFailed    = &errNo{code: 10006, strCode: "10006", message: "new conn failed", serverErrCode: -1}
	errConnWriteFailed  = &errNo{code: 10007, strCode: "10007", message: "conn write failed", serverErrCode: -1}
	errConnReadFailed   = &errNo{code: 10008, strCode: "10008", message: "conn read failed", serverErrCode: -1}
	errLogTooLong       = &errNo{code: 10009, strCode: "10009", message: "input log is too long", serverErrCode: -1} //nolint:unused
	errBadLog           = &errNo{code: 10010, strCode: "10010", message: "input log is invalid", serverErrCode: -1}
	errServerError      = &errNo{code: 10011, strCode: "10011", message: "server error", serverErrCode: -1} //nolint:unused
	errServerPanic      = &errNo{code: 10012, strCode: "10012", message: "server panic", serverErrCode: -1}
	workerBusy          = &errNo{code: 10013, strCode: "10013", message: "worker is busy", serverErrCode: -1}
	errNoMatchReq4Rsp   = &errNo{code: 10014, strCode: "10014", message: "no match unacknowledged request for response", serverErrCode: -1}
	errConnClosedByPeer = &errNo{code: 10015, strCode: "10015", message: "conn closed by peer", serverErrCode: -1}
	errUnknown          = &errNo{code: 20001, strCode: "20001", message: "unknown", serverErrCode: -1}
)

type errNo struct {
	code          int
	strCode       string
	message       string
	serverErrCode int // server error code from server response, -1 means not a server error
}

func (e *errNo) Error() string {
	return e.message
}

// GetServerErrorCode extracts server error code from error.
func GetServerErrorCode(err error) int {
	if err == nil {
		return 0
	}
	var t *errNo
	switch {
	case errors.As(err, &t):
		return t.getServerErrCode()
	default:
		return -1
	}
}

//nolint:unused
func (e *errNo) getCode() int {
	return e.code
}

func (e *errNo) getStrCode() string {
	return e.strCode
}

func (e *errNo) getServerErrCode() int {
	return e.serverErrCode
}

func getErrorCode(err error) string {
	if err == nil {
		return errOK.getStrCode()
	}

	var t *errNo
	switch {
	case errors.As(err, &t):
		return t.getStrCode()
	default:
		return errUnknown.getStrCode()
	}
}

type worker struct {
	client             *client                  // parent client
	index              int                      // worker id
	indexStr           string                   // worker id string
	options            *Options                 // config options
	state              atomic.Int32             // worker state
	log                logger.Logger            // debug logger
	conn               atomic.Value             // connection used to send data
	cmdChan            chan interface{}         // command channel
	dataChan           chan *sendDataReq        // data channel
	dataSemaphore      syncx.Semaphore          // semaphore used to handle message queueing
	pendingBatches     map[string]*batchReq     // pending batches
	unackedBatches     map[string]*batchReq     // sent but not acknowledged batches
	sendFailedBatches  chan *sendFailedBatchReq // send failed batches channel
	updateConnChan     chan error               // update conn channel
	retryBatches       chan *batchReq           // retry batches  channel
	responseBatches    chan *batchRsp           // batch response channel
	batchTimeoutTicker *time.Ticker             // batch timeout ticker
	sendTimeoutTicker  *time.Ticker             // send timeout ticker
	heartbeatTicker    *time.Ticker             // heartbeat ticker
	mapCleanTicker     *time.Ticker             // map clean ticker, clean the unackedBatches map periodically
	updateConnTicker   *time.Ticker             // update connection ticker, change connection periodically
	unackedBatchCount  int                      // sent but not acknowledged batches counter, used to clean the unackedBatches map periodically
	metrics            *metrics                 // metrics
	bufferPool         bufferpool.BufferPool    // buffer pool
	bytePool           bufferpool.BytePool      // byte pool
	stop               chan struct{}            // stop the worker
}

func newWorker(cli *client, index int, opts *Options) (*worker, error) {
	sendTimeout := opts.SendTimeout / 2
	if sendTimeout == 0 {
		sendTimeout = 5 * time.Second
	}

	w := &worker{
		index:              index,
		indexStr:           strconv.Itoa(index),
		client:             cli,
		options:            opts,
		cmdChan:            make(chan interface{}),
		dataChan:           make(chan *sendDataReq, opts.MaxPendingMessages),
		dataSemaphore:      syncx.NewSemaphore(int32(opts.MaxPendingMessages)),
		pendingBatches:     make(map[string]*batchReq),
		unackedBatches:     make(map[string]*batchReq),
		sendFailedBatches:  make(chan *sendFailedBatchReq, opts.MaxPendingMessages),
		updateConnChan:     make(chan error, 64),
		retryBatches:       make(chan *batchReq, opts.MaxPendingMessages),
		responseBatches:    make(chan *batchRsp, opts.MaxPendingMessages),
		batchTimeoutTicker: time.NewTicker(opts.BatchingMaxPublishDelay),
		sendTimeoutTicker:  time.NewTicker(sendTimeout),
		heartbeatTicker:    time.NewTicker(defaultHeartbeatInterval * time.Second),
		mapCleanTicker:     time.NewTicker(defaultMapCleanInterval * time.Second),
		updateConnTicker:   time.NewTicker(time.Duration(30+rand.Intn(50)) * time.Second), // update connection randomly
		metrics:            cli.metrics,
		bufferPool:         opts.BufferPool,
		bytePool:           opts.BytePool,
		log:                opts.Logger,
		stop:               make(chan struct{}),
	}

	// set to init state
	w.setState(stateInit)

	// get a connection
	conn, err := cli.getConn()
	if err != nil {
		return nil, err
	}
	w.log.Debug("use conn: ", conn.RemoteAddr().String())
	w.setConn(conn)

	// start the worker
	w.start()
	// update to ready state
	w.setState(stateReady)

	return w, nil
}

func (w *worker) available() bool {
	return w.dataSemaphore.Available() > 0
}

func (w *worker) start() {
	go func() {
		defer func() {
			if rec := recover(); rec != nil {
				w.log.Error("panic:", rec)
				w.log.Error(string(debug.Stack()))
				w.metrics.incError(errServerPanic.getStrCode())
			}
		}()

		for {
			select {
			case <-w.stop:
				return
			case req, ok := <-w.cmdChan:
				if !ok {
					continue
				}
				switch r := req.(type) {
				case *closeReq:
					w.handleClose(r)
				}
			case req, ok := <-w.dataChan:
				if !ok {
					continue
				}
				w.handleSendData(req)
			case <-w.batchTimeoutTicker.C:
				// handle batch timeout
				w.handleBatchTimeout()
			case <-w.sendTimeoutTicker.C:
				// handle send timeout
				w.handleSendTimeout()
			case <-w.mapCleanTicker.C:
				// clean unackedBatches periodically, in golang, map will get bigger and bigger
				w.handleCleanMap()
			case <-w.heartbeatTicker.C:
				// send heartbeat periodically
				w.handleSendHeartbeat()
			case <-w.updateConnTicker.C:
				// update connection periodically
				w.handleUpdateConn()
			case e, ok := <-w.updateConnChan:
				if !ok {
					continue
				}
				// update conn
				w.updateConn(nil, e)
			case batch, ok := <-w.sendFailedBatches:
				// handle send failed batches
				if !ok {
					continue
				}
				w.handleSendFailed(batch)
			case batch, ok := <-w.retryBatches:
				// handle retry batches
				if !ok {
					continue
				}
				w.handleRetry(batch, true)
			case rsp, ok := <-w.responseBatches:
				// handle responses
				if !ok {
					continue
				}
				w.handleRsp(rsp)
			}
		}
	}()
}

func (w *worker) doSendAsync(ctx context.Context, msg Message, callback Callback, flushImmediately bool) {
	req := reqPool.Get().(*sendDataReq)
	*req = sendDataReq{
		pool:             reqPool,
		ctx:              ctx,
		msg:              msg,
		callback:         callback,
		flushImmediately: flushImmediately,
		publishTime:      time.Now(),
		metrics:          w.metrics,
		workerID:         w.indexStr,
	}

	if len(msg.Payload) == 0 {
		req.done(errBadLog, "")
		return
	}

	// worker is closed
	if w.getState() != stateReady {
		req.done(errProducerClosed, "")
		return
	}

	// use a semaphore to trace if the data channel is full
	if w.options.BlockIfQueueIsFull {
		if !w.dataSemaphore.Acquire(ctx) {
			w.log.Warn("queue is full, worker index:", w.index)
			req.done(errContextExpired, "")
			return
		}
	} else {
		if !w.dataSemaphore.TryAcquire() {
			w.log.Warn("queue is full, worker index:", w.index)
			req.done(errSendQueueIsFull, "")
			return
		}
	}

	// save the semaphore, when the request is done, release semaphore
	req.semaphore = w.dataSemaphore
	w.dataChan <- req
	w.metrics.incPending(w.indexStr)
}

func (w *worker) send(ctx context.Context, msg Message) error {
	var err error

	// trace if the message is done
	isDone := atomic.NewBool(false)
	doneCh := make(chan struct{})

	w.doSendAsync(ctx, msg, func(msg Message, e error) {
		if isDone.CompareAndSwap(false, true) {
			err = e       // save the error
			close(doneCh) // notify that the message is done
		}
	}, true)

	// wait for send done
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-doneCh:
		return err
	}
}

func (w *worker) sendAsync(ctx context.Context, msg Message, callback Callback) {
	w.doSendAsync(ctx, msg, callback, false)
}

func (w *worker) buildBatchID() string {
	u, err := uuid.NewRandom()
	if err != nil {
		return w.indexStr + ":" + strconv.FormatInt(time.Now().UnixNano(), 10)
	}
	return u.String()
}

func (w *worker) handleSendData(req *sendDataReq) {
	// only the messages that with the same stream ID can be sent in a batch, we use the stream ID as the key
	batch, ok := w.pendingBatches[req.msg.StreamID]
	if !ok {
		streamID := req.msg.StreamID
		batch = batchPool.Get().(*batchReq)
		dataReqs := batch.dataReqs
		if dataReqs == nil {
			dataReqs = make([]*sendDataReq, 0, w.options.BatchingMaxMessages)
		}
		*batch = batchReq{
			pool:       batchPool,
			workerID:   w.indexStr,
			batchID:    w.buildBatchID(),
			groupID:    w.options.GroupID,
			streamID:   streamID,
			dataReqs:   dataReqs,
			batchTime:  time.Now(),
			retries:    0,
			bufferPool: w.bufferPool,
			bytePool:   w.bytePool,
			metrics:    w.metrics,
			addColumns: w.options.addColumnStr,
		}
		w.pendingBatches[streamID] = batch
	}

	// batch is a pointer, update it directly
	batch.append(req)

	// no need to flush immediately, not reach the batching max message number/size, just wait
	if !req.flushImmediately &&
		len(batch.dataReqs) < w.options.BatchingMaxMessages &&
		batch.dataSize < w.options.BatchingMaxSize {
		return
	}

	// send and delete from the pending batches map
	w.sendBatch(batch, true)
	delete(w.pendingBatches, batch.streamID)
}

func (w *worker) sendBatch(b *batchReq, retryOnFail bool) {
	// check if we have exceeded the max retry
	if b.retries > w.options.MaxRetries {
		b.done(errSendTimeout)
		return
	}

	b.lastSendTime = time.Now()
	b.encode()

	// error callback
	onErr := func(c gnet.Conn, e error, inCallback bool) {
		defer func() {
			if rec := recover(); rec != nil {
				w.log.Error("panic:", rec)
				w.log.Error(string(debug.Stack()))
				w.metrics.incError(errServerPanic.getStrCode())
			}
		}()

		w.metrics.incError(errConnWriteFailed.getStrCode())
		w.log.Error("send batch failed, err: ", e, ", inCallback: ", inCallback, ", logNum:", len(b.dataReqs))

		// close already
		if w.getState() == stateClosed {
			b.done(errConnWriteFailed)
			return
		}

		// important：when AsyncWrite() call succeed, the batch will be put into w.unackedBatches,now it failed, we need
		// to delete from w.unackedBatches, as onErr() is call concurrently in different goroutine, we can not delete it
		// from this callback directly, or will be panic, so we put into the w.sendFailedBatches channel, and it will be
		// deleted and retried in handleSendFailed() one by one
		if inCallback {
			// can not call w.updateConn() in callback, updateConn() may open new conn, which will call gent.Client.Dial()
			// gent.Client.Dial() and this callback are run in a same goroutine, it will be blocked
			w.updateConnAsync(errConnWriteFailed)
			w.sendFailedBatches <- &sendFailedBatchReq{batch: b, retry: retryOnFail}
			return
		}

		// in a same goroutine, retry it directly
		// network error, change a new connection
		w.updateConn(c, errConnWriteFailed)
		if retryOnFail {
			// w.retryBatches <- b
			w.backoffRetry(context.Background(), b)
		} else {
			b.done(errConnWriteFailed)
		}
	}

	// very important：'cause we use gnet, we must call AsyncWrite to send data in goroutines that are different from gnet.OnTraffic() callback
	conn := w.getConn()
	if b.retries > 0 {
		w.log.Debug("retry batch to conn:", conn.RemoteAddr(), ", workerID:", w.index, ", batchID:", b.batchID, ", logNum:", len(b.dataReqs))
	}
	err := conn.AsyncWrite(b.buffer.Bytes(), func(c gnet.Conn, e error) error {
		if e != nil {
			onErr(c, e, true) // error callback
		}
		return nil
	})

	if err != nil {
		onErr(conn, err, false) // error callback
		return
	}

	// important：as AsyncWrite() is async，AsyncWrite() call succeed doesn't mean send succeed, however, we put the batch
	// into unackedBatches in advance, if AsyncWrite send failed finally, the batch will be deleted from unackedBatches
	// in the callback onErr(), if AsyncWrite() send succeed finally, the batch will be deleted from unackedBatches when
	// the response is received or timeout
	w.unackedBatchCount++
	w.unackedBatches[b.batchID] = b
}

func (w *worker) handleSendFailed(b *sendFailedBatchReq) {
	// send failed, delete the batch from unackedBatches, when retried, it will be pushed back
	delete(w.unackedBatches, b.batch.batchID)
	if b.retry {
		w.backoffRetry(context.Background(), b.batch)
	} else {
		b.batch.done(errConnWriteFailed)
	}
}

func (w *worker) backoffRetry(ctx context.Context, batch *batchReq) {
	if batch.retries >= w.options.MaxRetries {
		batch.done(errSendTimeout)
		return
	}

	// it is closed already
	if w.getState() == stateClosed {
		batch.done(errSendTimeout)
		return
	}

	batch.retries++
	go func() {
		defer func() {
			if rec := recover(); rec != nil {
				w.log.Error("panic:", rec)
				w.log.Error(string(debug.Stack()))
				w.metrics.incError(errServerPanic.getStrCode())
			}
		}()

		// use ExponentialBackoff
		backoff := util.ExponentialBackoff{
			InitialInterval: w.options.RetryInitialInterval,
			MaxInterval:     10 * time.Second,
			Multiplier:      2.0,
			Randomization:   0.2,
		}

		waitTime := backoff.Next(batch.retries)

		select {
		case <-time.After(waitTime):
			// check if the worker is closed again
			if w.getState() == stateClosed {
				batch.done(errSendTimeout)
				return
			}

			// put the batch into the retry channel
			w.log.Debug("put to retry...")
			w.retryBatches <- batch
		case <-ctx.Done():
			// in the case the process exit, just end up the batch sending routine
			batch.done(errSendTimeout)
		}
	}()
}

func (w *worker) handleRetry(batch *batchReq, retryOnFail bool) {
	// retry
	w.log.Debug("retry batch...", ", workerID:", w.index, ", batchID:", batch.batchID)
	w.metrics.incRetry(w.indexStr)
	w.sendBatch(batch, retryOnFail)
}

func (w *worker) handleBatchTimeout() {
	for _, batch := range w.pendingBatches {
		if time.Since(batch.batchTime) > w.options.BatchingMaxPublishDelay {
			w.sendBatch(batch, true)
			delete(w.pendingBatches, batch.streamID)
		}
	}
}

func (w *worker) handleSendTimeout() {
	// here may be ineffective
	for batchID, batch := range w.unackedBatches {
		if time.Since(batch.lastSendTime) > w.options.SendTimeout {
			w.log.Warn("worker[", w.index, "] send timeout, resend it now:", batch.batchID, "batchID:", batchID,
				",last send time:", batch.lastSendTime.UnixMilli(), ", now:", time.Now().UnixMilli(), "timeout option:", w.options.SendTimeout)
			//
			// w.retryBatches <- batch
			w.backoffRetry(context.Background(), batch)
			// as retry will put it back to unackedBatches, we delete it here
			delete(w.unackedBatches, batchID)
			w.metrics.incTimeout(w.indexStr)
		}
	}
}

func (w *worker) handleCleanMap() {
	// clean it when we write the map more than 500000 times
	if w.unackedBatchCount < defaultMapCleanThreshold {
		return
	}

	// create a new map and copy the data from the old map
	newMap := make(map[string]*batchReq)
	for k, v := range w.unackedBatches {
		newMap[k] = v
	}

	// update the map with the new map
	w.unackedBatches = newMap
	// reset the counter
	w.unackedBatchCount = 0
}

func (w *worker) handleSendHeartbeat() {
	hb := heartbeatReq{}
	bb := w.bufferPool.Get()
	bytes := hb.encode(bb)

	onErr := func(c gnet.Conn, e error, inCallback bool) {
		w.metrics.incError(errConnWriteFailed.getStrCode())
		w.log.Error("send heartbeat failed, err:", e)
		if inCallback {
			// can not call w.updateConn() in callback, updateConn() may open new conn, which will call gent.Client.Dial()
			// gent.Client.Dial() and this callback are run in a same goroutine, it will be blocked
			w.updateConnAsync(errConnWriteFailed)
		} else {
			w.updateConn(c, errConnWriteFailed)
		}
	}

	// very important：'cause we use gnet, we must call AsyncWrite to send data in goroutines that are different from gnet.OnTraffic() callback
	conn := w.getConn()
	err := conn.AsyncWrite(bytes, func(c gnet.Conn, e error) error {
		if e != nil {
			onErr(c, e, true)
		}
		// recycle the buffer
		w.bufferPool.Put(bb)
		return nil
	})

	if err != nil {
		onErr(conn, err, false)
		// recycle the buffer
		w.bufferPool.Put(bb)
	}
}

func (w *worker) onRsp(rsp *batchRsp) {
	// close already
	if w.getState() == stateClosed {
		return
	}
	w.responseBatches <- rsp
}

func (w *worker) handleRsp(rsp *batchRsp) {
	batchID := rsp.batchID
	batch, ok := w.unackedBatches[batchID]
	if !ok {
		w.metrics.incError(errNoMatchReq4Rsp.strCode)
		return
	}

	// call batch.done to release the resources it holds
	var err = error(nil)
	if rsp.errCode != 0 {
		// Check if connection switch is needed
		needSwitchConn, isRetryable := retryableServerErrorCodes[rsp.errCode]
		if needSwitchConn && w.client != nil {
			w.log.Warn("server error detected, switching connection, errCode:", rsp.errCode,
				", batchID:", batch.batchID)
			w.updateConn(nil, nil)
		}

		// Check if retry is needed
		if w.options.RetryOnServerError && isRetryable && batch.retries < w.options.MaxRetries {
			delete(w.unackedBatches, batchID)

			w.log.Warn("server error, will retry, errCode:", rsp.errCode,
				", batchID:", batch.batchID, ", retries:", batch.retries)

			w.backoffRetry(context.Background(), batch)
			return
		}

		err = &errNo{
			code:    10011,
			strCode: "10011",
			message: "server error: errCode=" + strconv.Itoa(rsp.errCode) +
				", workerID=" + rsp.workerID +
				", batchID=" + rsp.batchID +
				", groupID=" + rsp.groupID +
				", streamID=" + rsp.streamID +
				", dt=" + rsp.dt,
			serverErrCode: rsp.errCode,
		}
		w.log.Error("send succeed but got error code:", rsp.errCode)
	}
	batch.done(err)
	delete(w.unackedBatches, batchID)
}

func (w *worker) close() {
	// closed already
	if w.getState() != stateReady {
		return
	}

	req := &closeReq{
		doneCh: make(chan struct{}),
	}
	// new a close request and put it to the command channel
	w.cmdChan <- req

	// wait for the close request done
	<-req.doneCh
	close(w.stop)
}

func (w *worker) handleClose(req *closeReq) {
	if !w.casState(stateReady, stateClosing) {
		close(req.doneCh)
		return
	}

	// stop the batch timeout ticker, all the pending message will be sent immediately
	w.batchTimeoutTicker.Stop()
	// stop the map clean ticker
	w.mapCleanTicker.Stop()
	// w.sendTimeoutTicker not stop, sending timeout still need to be handled
	//
	// stop the connection updating ticker
	w.updateConnTicker.Stop()

	// consume the message in w.dataChan channel, first, we close dataChan in a new goroutine
	// when no message remains, the for loop will break
	go func() {
		close(w.dataChan)
	}()
	for s := range w.dataChan {
		w.handleSendData(s)
	}

	// now, all the messages that are not sent are in w.pendingBatches, consume them, send them immediately, and just send
	// only one time, without retrying
	for streamID, batch := range w.pendingBatches {
		delete(w.pendingBatches, streamID)
		w.sendBatch(batch, false) // no retry anymore
	}

	// as we are sending asynchronously, the message that are sent and failed need to update w.retryBatches, we can't
	// close it now
	for i := 0; i < len(w.retryBatches); i++ {
		r := <-w.retryBatches
		w.handleRetry(r, false) // no retry anymore
	}

	// now, only w.unackedBatches still has batches, these batches haven't received a response, assign a callback to them,
	// when all these batches receive a response or timeout, close all the resources. 'case there is no other goroutine is
	// updating w.unackedBatches, it safe to update it here.

	// get the left unacknowledged batches
	left := atomic.NewInt32(int32(len(w.unackedBatches)))
	w.log.Debug("worker:", w.index, "unacked:", left.Load())

	closeAll := func() {
		// stop the send timeout ticker
		w.sendTimeoutTicker.Stop()
		// release the connection
		w.client.putConn(w.getConn(), nil)
		// close the command channel
		close(w.cmdChan)
		// update the worker state to stateClosed, from now on, w.retryBatches/w.sendFailedBatches/w.responseBatches can't
		// be written anymore, or it will be panic, so, when before updating these channels, we need to check if the state
		// is stateClosed, if it is, stop updating
		w.setState(stateClosed)
		// close the retry channel
		close(w.retryBatches)
		// close the send failed channel
		close(w.sendFailedBatches)
		// close the update conn chan
		close(w.updateConnChan)
		// close the response chan
		close(w.responseBatches)
		// close the done channel of the close request to notify the close is done
		close(req.doneCh)
	}

	// no left batches, just close and return
	if left.Load() <= 0 {
		w.log.Debug("no batch left, close now")
		closeAll()
		return
	}

	for id, batch := range w.unackedBatches {
		// update the left batches, add a callback to it
		batch.callback = func() {
			// when receive a response or timeout, decrease the counter by 1, when left <=0, indicates that all are done
			l := left.Add(-1)
			if l <= 0 {
				w.log.Debug("left batches all done, close now")
				closeAll()
			}
		}
		// store it back to the map
		w.unackedBatches[id] = batch
	}
}

func (w *worker) handleUpdateConn() {
	w.updateConn(nil, nil)
}

func (w *worker) updateConnAsync(err error) {
	// 已经处于关闭状态
	if w.getState() == stateClosed {
		return
	}

	select {
	case w.updateConnChan <- err:
	default:
	}
}

func (w *worker) updateConn(old gnet.Conn, err error) {
	newConn, newErr := w.client.getConn()
	if newErr != nil {
		w.log.Error("get new conn error:", newErr)
		w.metrics.incError(errNewConnFailed.getStrCode())
		return
	}

	oldConn := old
	if oldConn == nil {
		oldConn = w.getConn()
	}

	ok := w.casConn(oldConn, newConn)
	if ok {
		// put back to pool only if there is no error
		if err == nil {
			w.client.putConn(oldConn, err)
		} else { // nolint:staticcheck
			// if there are some errors, there are basically conn closed by peer，
			// gnet will call Client.OnClose() to delete it from the pool,
			// it won't be wrong even though we do not put it back here
		}
		w.metrics.incUpdateConn(getErrorCode(err))
	} else {
		w.client.putConn(newConn, nil)
	}
}

func (w *worker) setConn(conn gnet.Conn) {
	w.conn.Store(conn)
}

func (w *worker) getConn() gnet.Conn {
	return w.conn.Load().(gnet.Conn)
}

func (w *worker) onConnClosed(conn gnet.Conn, err error) {
	oldConn := w.conn.Load().(gnet.Conn)
	if oldConn == conn {
		w.updateConnAsync(err)
	}
}

func (w *worker) casConn(oldConn, newConn gnet.Conn) bool {
	return w.conn.CompareAndSwap(oldConn, newConn)
}

func (w *worker) setState(state workerState) {
	w.state.Swap(int32(state))
}

func (w *worker) getState() workerState {
	return workerState(w.state.Load())
}

func (w *worker) casState(oldState, newState workerState) bool {
	return w.state.CompareAndSwap(int32(oldState), int32(newState))
}
