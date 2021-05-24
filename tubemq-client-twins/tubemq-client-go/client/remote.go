/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// Package client defines the api and information
// which can be exposed to user.
package client

import (
	"sync"

	"github.com/apache/incubator-inlong/tubemq-client-twins/tubemq-client-go/metadata"
)

// RmtDataCache represents the data returned from TubeMQ.
type RmtDataCache struct {
	consumerID       string
	groupName        string
	underGroupCtrl   bool
	defFlowCtrlID    int64
	groupFlowCtrlID  int64
	subscribeInfo    []*metadata.SubscribeInfo
	rebalanceResults []*metadata.ConsumerEvent
	mu               sync.Mutex
	brokerToPartitions map[*metadata.Node][]*metadata.Partition
}

// GetUnderGroupCtrl returns the underGroupCtrl.
func (r *RmtDataCache) GetUnderGroupCtrl() bool {
	return r.underGroupCtrl
}

// GetDefFlowCtrlID returns the defFlowCtrlID.
func (r *RmtDataCache) GetDefFlowCtrlID() int64 {
	return r.defFlowCtrlID
}

// GetGroupFlowCtrlID returns the groupFlowCtrlID.
func (r *RmtDataCache) GetGroupFlowCtrlID() int64 {
	return r.groupFlowCtrlID
}

// GetSubscribeInfo returns the subscribeInfo.
func (r *RmtDataCache) GetSubscribeInfo() []*metadata.SubscribeInfo {
	return r.subscribeInfo
}

// PollEventResult polls the first event result from the rebalanceResults.
func (r *RmtDataCache) PollEventResult() *metadata.ConsumerEvent {
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.rebalanceResults) > 0 {
		event := r.rebalanceResults[0]
		r.rebalanceResults = r.rebalanceResults[1:]
		return event
	}
	return nil
}

// GetPartitionByBroker returns the
func (r *RmtDataCache) GetPartitionByBroker(node *metadata.Node) []*metadata.Partition {
	if partitions, ok := r.brokerToPartitions[node]; ok {
		return partitions
	}
	return nil
}
