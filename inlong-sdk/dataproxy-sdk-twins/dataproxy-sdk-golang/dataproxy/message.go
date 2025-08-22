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
	"strings"
)

// Callback is the callback function signature of the DataProxy producer
type Callback func(message Message, err error)

// Message is the message to send
type Message struct {
	GroupID  string            // InLong group ID
	StreamID string            // InLong stream ID
	Payload  []byte            // the content of the message
	Headers  map[string]string // message headers, won't be sent to the server right now
	MetaData interface{}       // any data you want, won't be sent to the server, but you can get it in the callback
}

// IsValid checks if the message is valid
func (m *Message) IsValid() bool {
	if strings.ContainsAny(m.GroupID, " \t\n\r") || strings.ContainsAny(m.StreamID, " \t\n\r") || m.GroupID == "" || m.StreamID == "" || len(m.Payload) == 0 {
		return false
	}

	return true
}
