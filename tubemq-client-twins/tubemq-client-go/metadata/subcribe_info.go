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

package metadata

import (
	"fmt"
	"strings"
)

// SubscribeInfo represents the metadata of the subscribe info.
type SubscribeInfo struct {
	group      string
	consumerID string
	partition  *Partition
}

// GetGroup returns the group name.
func (s *SubscribeInfo) GetGroup() string {
	return s.group
}

// GetConsumerID returns the consumer id.
func (s *SubscribeInfo) GetConsumerID() string {
	return s.consumerID
}

// GetPartition returns the partition.
func (s *SubscribeInfo) GetPartition() *Partition {
	return s.partition
}

// String returns the contents of SubscribeInfo as a string.
func (s *SubscribeInfo) String() string {
	return fmt.Sprintf("%s@%s-%s", s.consumerID, s.group, s.partition.String())
}

// NewSubscribeInfo constructs a SubscribeInfo from a given string.
// If the given is invalid, it will return error.
func NewSubscribeInfo(subscribeInfo string) (*SubscribeInfo, error) {
	consumerInfo := strings.Split(subscribeInfo, "#")[0]
	partition, err := NewPartition(subscribeInfo[strings.Index(subscribeInfo, "#")+1:])
	if err != nil {
		return nil, err
	}
	return &SubscribeInfo{
		group:      strings.Split(consumerInfo, "@")[1],
		consumerID: strings.Split(consumerInfo, "@")[0],
		partition:  partition,
	}, nil
}

// SetPartition sets the partition.
func (s *SubscribeInfo) SetPartition(partition *Partition) {
	s.partition = partition
}

// SetGroup sets the group.
func (s *SubscribeInfo) SetGroup(group string) {
	s.group = group
}

// SetConsumerID sets the consumerID.
func (s *SubscribeInfo) SetConsumerID(id string) {
	s.consumerID = id
}
