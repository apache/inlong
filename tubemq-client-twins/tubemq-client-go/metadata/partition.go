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
	"strings"
)

// Partition represents the metadata of a partition.
type Partition struct {
	topic        string
	broker       *Node
	partitionID  int32
	partitionKey string
	offset       int64
	lastConsumed bool
}

func NewPartition(partition string) (*Partition, error) {
	var b *Node
	var topic string
	var partitionID int
	var err error
	pos := strings.Index(partition, "#")
	if pos != -1 {
		broker := strings.TrimSpace(partition[:pos])
		b, err = NewNode(true, broker)
		if err != nil {
			return nil, err
		}
		p := strings.TrimSpace(partition[pos+1:])
		pos = strings.Index(p, ":")
		if pos != -1 {
			topic = strings.TrimSpace(p[0:pos])
			partitionID, err = strconv.Atoi(strings.TrimSpace(p[pos+1:]))
			if err != nil {
				return nil, err
			}
		}
	}
	return &Partition{
		topic:       topic,
		broker:      b,
		partitionID: int32(partitionID),
	}, nil
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

func (p *Partition) GetBroker() *Node {
	return p.broker
}

// String returns the metadata of a Partition as a string.
func (p *Partition) String() string {
	return p.broker.String() + "#" + p.topic + "@" + strconv.Itoa(int(p.partitionID))
}

func (p *Partition) SetLastConsumed(lastConsumed bool) {
	p.lastConsumed = lastConsumed
}
