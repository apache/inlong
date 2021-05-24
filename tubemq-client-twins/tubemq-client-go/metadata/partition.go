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
	"strconv"
)

// Partition represents the metadata of a partition.
type Partition struct {
	topic        string
	Broker       *Node
	partitionID  int32
	partitionKey string
	offset       int64
	lastConsumed bool
}

// GetLastConsumed returns lastConsumed of a partition.
func (p *Partition) GetLastConsumed() bool {
	return p.lastConsumed
}

// GetPartitionID returns the partition id of a partition.
func (p *Partition) GetPartitionID() int32 {
	return p.partitionID
}

// GetPartitionKey returns the partition key of a partition.
func (p *Partition) GetPartitionKey() string {
	return p.partitionKey
}

// GetTopic returns the topic of the partition subscribed to.
func (p *Partition) GetTopic() string {
	return p.topic
}

// String returns the metadata of a Partition as a string.
func (p *Partition) String() string {
	return p.Broker.String() + "#" + p.topic + "@" + strconv.Itoa(int(p.partitionID))
}
