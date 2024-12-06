// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package client

import (
	"context"
	"strings"
	"sync"
	"time"

	"github.com/apache/inlong/inlong-tubemq/tubemq-client-twins/tubemq-client-go/errs"
	"github.com/apache/inlong/inlong-tubemq/tubemq-client-twins/tubemq-client-go/log"
	"github.com/apache/inlong/inlong-tubemq/tubemq-client-twins/tubemq-client-go/metadata"
	"github.com/apache/inlong/inlong-tubemq/tubemq-client-twins/tubemq-client-go/protocol"
	"github.com/apache/inlong/inlong-tubemq/tubemq-client-twins/tubemq-client-go/util"
)

type heartbeatMetadata struct {
	numConnections int
	timer          *time.Timer
}

type heartbeatManager struct {
	producer   *producer
	consumer   *consumer
	heartbeats map[string]*heartbeatMetadata
	mu         sync.Mutex
}

func newHBManager(consumer *consumer) *heartbeatManager {
	return &heartbeatManager{
		consumer:   consumer,
		heartbeats: make(map[string]*heartbeatMetadata),
	}
}

func (h *heartbeatManager) registerMaster(address string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	hm, ok := h.heartbeats[address]
	log.Infof("register master heartbeat address:%v, heartbeat:%+v", address, hm)

	var heartbeatInterval time.Duration
	var heartbeatFunc func()
	if h.producer != nil {
		heartbeatInterval = h.producer.config.Heartbeat.Interval / 2
		heartbeatFunc = h.producerHB2Master
	}

	if !ok {
		h.heartbeats[address] = &heartbeatMetadata{
			numConnections: 1,
			timer:          time.AfterFunc(heartbeatInterval, heartbeatFunc),
		}
		return
	}
	hm.numConnections++
}

// deleteHeartbeat delete heartbeat of the given address.
func (h *heartbeatManager) deleteHeartbeat(address string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	hm, ok := h.heartbeats[address]
	if !ok {
		return
	}
	hm.numConnections--
	if hm.numConnections <= 0 {
		delete(h.heartbeats, address)
	}
}

func (h *heartbeatManager) registerBroker(broker *metadata.Node) {
	h.mu.Lock()
	defer h.mu.Unlock()
	hm, ok := h.heartbeats[broker.GetAddress()]
	if !ok {
		h.heartbeats[broker.GetAddress()] = &heartbeatMetadata{
			numConnections: 1,
			timer:          time.AfterFunc(h.consumer.config.Heartbeat.Interval, func() { h.consumerHB2Broker(broker) }),
		}
		return
	}
	hm.numConnections++
}

func (h *heartbeatManager) producerHB2Master() {
	m := &metadata.Metadata{}
	node := &metadata.Node{}
	node.SetHost(util.GetLocalHost())
	node.SetAddress(h.producer.master.Address)
	m.SetNode(node)

	auth := &protocol.AuthenticateInfo{}
	if h.producer.needGenMasterCertificateInfo(true) {
		util.GenMasterAuthenticateToken(auth, h.producer.config.Net.Auth.UserName, h.producer.config.Net.Auth.Password)
	}

	h.producer.unreportedTimes++
	if h.producer.unreportedTimes > h.producer.config.Producer.MaxPubInfoInterval {
		m.SetReportTimes(true)
		h.producer.unreportedTimes = 0
	}

	rsp, err := h.sendHeartbeatP2M(m)
	if err == nil {
		h.producer.masterHBRetry = 0
		h.processHBResponseM2P(rsp)
		h.resetMasterHeartbeat()
		return
	}
	h.producer.masterHBRetry++
	h.resetMasterHeartbeat()
}

func (h *heartbeatManager) resetMasterHeartbeat() {
	h.mu.Lock()
	defer h.mu.Unlock()
	var address string
	if h.producer != nil {
		address = h.producer.master.Address
	} else {
		address = h.consumer.master.Address
	}
	hm := h.heartbeats[address]
	hm.timer.Reset(h.nextHeartbeatInterval())
}

func (h *heartbeatManager) sendHeartbeatP2M(m *metadata.Metadata) (*protocol.HeartResponseM2P, error) {
	ctx, cancel := context.WithTimeout(context.Background(), h.producer.config.Net.ReadTimeout)
	defer cancel()
	rsp, err := h.producer.client.HeartRequestP2M(ctx, m, h.producer.clientID, h.producer.brokerCheckSum, h.producer.publishTopics)
	return rsp, err
}

func (h *heartbeatManager) sendHeartbeatC2M(m *metadata.Metadata) (*protocol.HeartResponseM2C, error) {
	ctx, cancel := context.WithTimeout(context.Background(), h.consumer.config.Net.ReadTimeout)
	defer cancel()
	rsp, err := h.consumer.client.HeartRequestC2M(ctx, m, h.consumer.subInfo, h.consumer.rmtDataCache)
	return rsp, err
}

func (h *heartbeatManager) processHBResponseM2P(rsp *protocol.HeartResponseM2P) {
	h.producer.masterHBRetry = 0
	topicInfos := rsp.GetTopicInfos()
	brokerInfos := rsp.GetBrokerInfos()

	// update broker meta
	h.producer.updateBrokerInfoList(brokerInfos)

	// update topic meta
	h.producer.updateTopicConfigure(topicInfos)
}

func (h *heartbeatManager) nextHeartbeatInterval() time.Duration {
	var interval time.Duration
	if h.producer != nil {
		interval = h.producer.config.Heartbeat.Interval
		if h.producer.masterHBRetry >= h.producer.config.Heartbeat.MaxRetryTimes {
			interval = h.producer.config.Heartbeat.AfterFail
		}
	} else {
		interval = h.consumer.config.Heartbeat.Interval
		if h.consumer.masterHBRetry >= h.consumer.config.Heartbeat.MaxRetryTimes {
			interval = h.consumer.config.Heartbeat.AfterFail
		}
	}
	return interval
}

func (h *heartbeatManager) consumerHB2Broker(broker *metadata.Node) {
	h.mu.Lock()
	defer h.mu.Unlock()

	partitions := h.consumer.rmtDataCache.GetPartitionByBroker(broker)
	if len(partitions) == 0 {
		h.resetBrokerTimer(broker)
		return
	}

	rsp, err := h.sendHeartbeatC2B(broker)
	if err != nil {
		log.Warnf("[Heartbeat2Broker] request network to failure %s", err.Error())
		h.resetBrokerTimer(broker)
		return
	}
	partitionKeys := make([]string, 0, len(partitions))
	if rsp.GetErrCode() == errs.RetCertificateFailure {
		for _, partition := range partitions {
			partitionKeys = append(partitionKeys, partition.GetPartitionKey())
		}
		log.Warnf("[Heartbeat2Broker] request (%s) CertificateFailure", broker.GetAddress())
		h.consumer.rmtDataCache.RemovePartition(partitionKeys)
	}
	if rsp.GetSuccess() && rsp.GetHasPartFailure() {
		for _, fi := range rsp.GetFailureInfo() {
			pos := strings.Index(fi, ":")
			if pos == -1 {
				continue
			}
			partition, err := metadata.NewPartition(fi[pos+1:])
			if err != nil {
				continue
			}
			log.Tracef("[Heartbeat2Broker] found partition(%s) hb failure!", partition.GetPartitionKey())
			partitionKeys = append(partitionKeys, partition.GetPartitionKey())
		}
		h.consumer.rmtDataCache.RemovePartition(partitionKeys)
	}
	log.Tracef("[Heartbeat2Broker] out hb response process, add broker(%s) timer!", broker.GetAddress())
	h.resetBrokerTimer(broker)
}

func (h *heartbeatManager) sendHeartbeatC2B(broker *metadata.Node) (*protocol.HeartBeatResponseB2C, error) {
	m := &metadata.Metadata{}
	m.SetReadStatus(h.consumer.getConsumeReadStatus(false))
	m.SetNode(broker)
	sub := &metadata.SubscribeInfo{}
	sub.SetGroup(h.consumer.config.Consumer.Group)
	m.SetSubscribeInfo(sub)
	auth := h.consumer.genBrokerAuthenticInfo(true)
	h.consumer.subInfo.SetAuthorizedInfo(auth)
	ctx, cancel := context.WithTimeout(context.Background(), h.consumer.config.Net.ReadTimeout)
	defer cancel()
	rsp, err := h.consumer.client.HeartbeatRequestC2B(ctx, m, h.consumer.subInfo, h.consumer.rmtDataCache)
	return rsp, err
}

func (h *heartbeatManager) resetBrokerTimer(broker *metadata.Node) {
	interval := h.consumer.config.Heartbeat.Interval
	partitions := h.consumer.rmtDataCache.GetPartitionByBroker(broker)
	if len(partitions) == 0 {
		delete(h.heartbeats, broker.GetAddress())
	} else {
		hm := h.heartbeats[broker.GetAddress()]
		hm.timer.Reset(interval)
	}
}

func (h *heartbeatManager) close() {
	h.mu.Lock()
	defer h.mu.Unlock()

	for _, heartbeat := range h.heartbeats {
		heartbeat.timer.Stop()
	}
}
