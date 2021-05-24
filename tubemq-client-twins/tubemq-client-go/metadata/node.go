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

// Node represents the metadata of a node.
type Node struct {
	id      uint32
	host    string
	port    uint32
	address string
}

// GetID returns the id of a node.
func (n *Node) GetID() uint32 {
	return n.id
}

// GetPort returns the port of a node.
func (n *Node) GetPort() uint32 {
	return n.port
}

// GetHost returns the hostname of a node.
func (n *Node) GetHost() string {
	return n.host
}

// GetAddress returns the address of a node.
func (n *Node) GetAddress() string {
	return n.address
}

// String returns the metadata of a node as a string.
func (n *Node) String() string {
	return strconv.Itoa(int(n.id)) + ":" + n.host + ":" + strconv.Itoa(int(n.port))
}
