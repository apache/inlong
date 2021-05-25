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

// package config defines the all the TubeMQ configuration options.
package config

import (
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"time"
)

// Config defines multiple configuration options.
type Config struct {
	// Net is the namespace for network-level properties used by Broker and Master.
	Net struct {
		// How long to wait for a response.
		ReadTimeout time.Duration
		// TLS based authentication with broker and master.
		TLS         struct {
			// Whether or not to use TLS.
			Enable bool
			// CACertFile for TLS.
			CACertFile string
			// TLSCertFile for TLS.
			TLSCertFile string
			// TLSKeyFile for TLS.
			TLSKeyFile string
			// TTSServerName for TLS.
			TLSServerName string
		}
	}

	// Consumer is the namespace for configuration related to consume messages,
	// used by the consumer
	Consumer struct {
		masters []string
		topic string
		offset int
		Group                    string
		boundConsume             bool
		sessionKey               string
		sourceCount              int
		selectBig                bool
		rollbackIfConfirmTimeout bool
		maxSubInfoReportInterval int
		maxPartCheckPeriod       time.Duration
		partCheckSlice           time.Duration
		msgNotFoundWait          time.Duration
		rebConfirmWait           time.Duration
		maxConfirmWait           time.Duration
		shutdownRebWait          time.Duration
	}

	// Heartbeat is the namespace for configuration related to heartbeat messages,
	// used by the consumer
	Heartbeat struct {
		interval      time.Duration
		maxRetryTimes int
		afterFail     time.Duration
	}
}

func newDefaultConfig() *Config {
	c := &Config{}

	c.Net.ReadTimeout = 15000 * time.Millisecond
	c.Net.TLS.Enable = false

	c.Consumer.boundConsume = false
	c.Consumer.sessionKey = ""
	c.Consumer.sourceCount = 0
	c.Consumer.selectBig = true
	c.Consumer.offset = 0
	c.Consumer.rollbackIfConfirmTimeout = true
	c.Consumer.maxSubInfoReportInterval = 6
	c.Consumer.maxPartCheckPeriod = 60000 * time.Millisecond
	c.Consumer.partCheckSlice = 300 * time.Millisecond
	c.Consumer.rebConfirmWait = 3000 * time.Millisecond
	c.Consumer.maxConfirmWait = 60000 * time.Millisecond
	c.Consumer.shutdownRebWait = 10000 * time.Millisecond

	c.Heartbeat.interval = 10000 * time.Millisecond
	c.Heartbeat.maxRetryTimes = 5
	c.Heartbeat.afterFail = 60000 * time.Millisecond

	return c
}

// ParseAddress parses the address to user-defined config.
func ParseAddress(address string) (config *Config, err error) {
	c := newDefaultConfig()

	tokens := strings.SplitN(address, "?", 2)
	if len(tokens) != 2 {
		return nil, fmt.Errorf("address format invalid: address: %v, token: %v", address, tokens)
	}

	c.Consumer.masters = strings.Split(tokens[0], ",")

	tokens = strings.Split(tokens[1], "&")
	if len(tokens) == 0 {
		return nil, fmt.Errorf("address formata invalid: masters: %v with empty params", config.Consumer.masters)
	}

	for _, token := range tokens {
		values := strings.SplitN(token, "=", 2)
		if len(values) != 2 {
			return nil, fmt.Errorf("address format invalid, key=value missing: %v", values)
		}

		values[1], _ = url.QueryUnescape(values[1])
		if err := getConfigFromToken(c, values); err != nil {
			return nil, err
		}
	}
	return c, nil
}

func getConfigFromToken(config *Config, values []string) error {
	var err error
	switch values[0] {
	case "readTimeout":
		config.Net.ReadTimeout, err = parseDuration(values[1])
	case "tlsEnable":
		config.Net.TLS.Enable, err = strconv.ParseBool(values[1])
	case "caCertFile":
		config.Net.TLS.CACertFile = values[1]
	case "tlsCertFile":
		config.Net.TLS.TLSCertFile = values[1]
	case "tlsKeyFile":
		config.Net.TLS.TLSKeyFile = values[1]
	case "tlsServerName":
		config.Net.TLS.TLSServerName = values[1]
	case "group":
		config.Consumer.Group = values[1]
	case "topic":
		config.Consumer.topic = values[1]
	case "offset":
		config.Consumer.offset, err = strconv.Atoi(values[1])
	case "boundConsume":
		config.Consumer.boundConsume, err = strconv.ParseBool(values[1])
	case "sessionKey":
		config.Consumer.sessionKey = values[1]
	case "sourceCount":
		config.Consumer.sourceCount, err = strconv.Atoi(values[1])
	case "selectBig":
		config.Consumer.selectBig, err = strconv.ParseBool(values[1])
	case "rollbackIfConfirmTimeout":
		config.Consumer.rollbackIfConfirmTimeout, err = strconv.ParseBool(values[1])
	case "maxSubInfoReportInterval":
		config.Consumer.maxSubInfoReportInterval, err = strconv.Atoi(values[1])
	case "maxPartCheckPeriod":
		config.Consumer.maxPartCheckPeriod, err = parseDuration(values[1])
	case "partCheckSlice":
		config.Consumer.partCheckSlice, err = parseDuration(values[1])
	case "msgNotFoundWait":
		config.Consumer.msgNotFoundWait, err = parseDuration(values[1])
	case "rebConfirmWait":
		config.Consumer.rebConfirmWait, err = parseDuration(values[1])
	case "maxConfirmWait":
		config.Consumer.maxConfirmWait, err = parseDuration(values[1])
	case "shutdownRebWait":
		config.Consumer.shutdownRebWait, err = parseDuration(values[1])
	case "heartbeatInterval":
		config.Heartbeat.interval, err = parseDuration(values[1])
	case "heartbeatMaxRetryTimes":
		config.Heartbeat.maxRetryTimes, err = strconv.Atoi(values[1])
	case "heartbeatAfterFail":
		config.Heartbeat.afterFail, err = parseDuration(values[1])
	default:
		return fmt.Errorf("address format invalid, unknown keys: %v", values[0])
	}
	if err != nil {
		return fmt.Errorf("address format invalid(%v) err:%s", values[0], err.Error())
	}
	return err
}

func parseDuration(val string) (time.Duration, error) {
	maxWait, err := strconv.Atoi(val)
	if err != nil {
		return 0, err
	}
	return time.Duration(maxWait) * time.Millisecond, err
}
