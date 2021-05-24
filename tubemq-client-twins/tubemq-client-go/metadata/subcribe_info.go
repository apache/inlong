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
)

// SubscribeInfo represents the metadata of the subscribe info.
type SubscribeInfo struct {
	group         string
	consumerID    string
	partition     *Partition
	qryPriorityID int32
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

// GetQryPriorityID returns the QryPriorityID.
func (s *SubscribeInfo) GetQryPriorityID() int32 {
	return s.qryPriorityID
}

// String returns the contents of SubscribeInfo as a string.
func (s *SubscribeInfo) String() string {
	return fmt.Sprintf("%s@%s-%s", s.consumerID, s.group, s.partition.String())
}
