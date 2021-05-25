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

package config

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestParseAddress(t *testing.T) {
	address := "127.0.0.1:9092,127.0.0.1:9093?topic=Topic&group=Group&tlsEnable=false&msgNotFoundWait=10000&heartbeatMaxRetryTimes=6"
	c, err := ParseAddress(address)
	assert.Nil(t, err)
	assert.Equal(t, c.Consumer.masters, []string{"127.0.0.1:9092", "127.0.0.1:9093"})
	assert.Equal(t, c.Consumer.topic, "Topic")
	assert.Equal(t, c.Consumer.Group, "Group")
	assert.Equal(t, c.Consumer.msgNotFoundWait, 10000 * time.Millisecond)

	assert.Equal(t, c.Net.TLS.Enable, false)

	assert.Equal(t, c.Heartbeat.maxRetryTimes, 6)

	address = ""
	_, err = ParseAddress(address)
	assert.NotNil(t, err)

	address = "127.0.0.1:9092,127.0.0.1:9093?topic=Topic&ttt"
	_, err = ParseAddress(address)
	assert.NotNil(t, err)

	address = "127.0.0.1:9092,127.0.0.1:9093?topic=Topic&ttt=ttt"
	_, err = ParseAddress(address)
	assert.NotNil(t, err)
}



