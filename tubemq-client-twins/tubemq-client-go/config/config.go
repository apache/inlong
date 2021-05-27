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
// Refer to: https://github.com/apache/incubator-inlong/blob/3249de37acf054a9c43677131cfbb09fc6d366d1/tubemq-client/src/main/java/org/apache/tubemq/client/config/ConsumerConfig.java
type Config struct {
	// Net is the namespace for network-level properties used by Broker and Master.
	Net struct {
		// ReadTimeout represents how long to wait for a response.
		ReadTimeout time.Duration
		// TLS based authentication with broker and master.
		TLS struct {
			// Enable represents whether or not to use TLS.
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
		// Auth represents the account based authentication with broker and master.
		Auth struct {
			// Enable represents whether or not to use authentication.
			Enable bool
			// Username for authentication.
			UserName string
			// Password for authentication.
			Password string
		}
	}

	// Consumer is the namespace for configuration related to consume messages,
	// used by the consumer
	Consumer struct {
		// Masters is the addresses of master.
		Masters []string
		// Topic of the consumption.
		Topic string
		// ConsumerPosition is the initial offset to use if no offset was previously committed.
		ConsumePosition int
		// Group is the consumer group name.
		Group string
		// BoundConsume represents whether or not to specify the offset.
		BoundConsume bool
		// SessionKey is defined by the client.
		// The session key will be the same in a batch.
		SessionKey string
		// SourceCount is the number of consumers in a batch.
		SourceCount int
		// SelectBig specifies if multiple consumers want to reset the offset of the same partition,
		// whether or not to use the biggest offset.
		// The server will use the biggest offset if set, otherwise the server will use the smallest offset.
		SelectBig bool
		// RollbackIfConfirmTimeout represents if the confirm request timeouts,
		// whether or not this batch of data should be considered as successful.
		// This batch of data will not be considered as successful if set.
		RollbackIfConfirmTimeout bool
		// MaxSubInfoReportInterval is maximum interval for the client to report subscription information.
		MaxSubInfoReportInterval int
		// MaxPartCheckPeriod is the maximum interval to check the partition.
		MaxPartCheckPeriod time.Duration
		// PartCheckSlice is the interval to check the partition.
		PartCheckSlice time.Duration
		// MsgNotFoundWait is the maximum wait time the offset of a partition has reached the maximum offset.
		MsgNotFoundWait time.Duration
		// RebConfirmWait represents how long to wait
		// when the server is rebalancing and the partition is being occupied by the client.
		RebConfirmWait time.Duration
		// MaxConfirmWait is the maximum wait time a partition consumption command is released.
		MaxConfirmWait time.Duration
		// ShutdownRebWait represents how long to wait when shutdown is called and the server is rebalancing.
		ShutdownRebWait time.Duration
	}

	// Heartbeat is the namespace for configuration related to heartbeat messages,
	// used by the consumer
	Heartbeat struct {
		// Interval represents how frequently to send heartbeat.
		Interval time.Duration
		// MaxRetryTimes is the total number of times to retry sending heartbeat.
		MaxRetryTimes int
		// AfterFail is the heartbeat timeout after a heartbeat failure.
		AfterFail time.Duration
	}
}

func newDefaultConfig() *Config {
	c := &Config{}

	c.Net.ReadTimeout = 15000 * time.Millisecond
	c.Net.TLS.Enable = false
	c.Net.Auth.Enable = false
	c.Net.Auth.UserName = ""
	c.Net.Auth.Password = ""

	c.Consumer.Group = ""
	c.Consumer.BoundConsume = false
	c.Consumer.SessionKey = ""
	c.Consumer.SourceCount = 0
	c.Consumer.SelectBig = true
	c.Consumer.ConsumePosition = 0
	c.Consumer.RollbackIfConfirmTimeout = true
	c.Consumer.MaxSubInfoReportInterval = 6
	c.Consumer.MaxPartCheckPeriod = 60000 * time.Millisecond
	c.Consumer.PartCheckSlice = 300 * time.Millisecond
	c.Consumer.MsgNotFoundWait = 400 * time.Millisecond
	c.Consumer.RebConfirmWait = 3000 * time.Millisecond
	c.Consumer.MaxConfirmWait = 60000 * time.Millisecond
	c.Consumer.ShutdownRebWait = 10000 * time.Millisecond

	c.Heartbeat.Interval = 10000 * time.Millisecond
	c.Heartbeat.MaxRetryTimes = 5
	c.Heartbeat.AfterFail = 60000 * time.Millisecond

	return c
}

// ParseAddress parses the address to user-defined config.
func ParseAddress(address string) (config *Config, err error) {
	c := newDefaultConfig()

	tokens := strings.SplitN(address, "?", 2)
	if len(tokens) != 2 {
		return nil, fmt.Errorf("address format invalid: address: %v, token: %v", address, tokens)
	}

	c.Consumer.Masters = strings.Split(tokens[0], ",")

	tokens = strings.Split(tokens[1], "&")
	if len(tokens) == 0 {
		return nil, fmt.Errorf("address formata invalid: Masters: %v with empty params", config.Consumer.Masters)
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
	case "CACertFile":
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
		config.Consumer.Topic = values[1]
	case "consumePosition":
		config.Consumer.ConsumePosition, err = strconv.Atoi(values[1])
	case "boundConsume":
		config.Consumer.BoundConsume, err = strconv.ParseBool(values[1])
	case "sessionKey":
		config.Consumer.SessionKey = values[1]
	case "sourceCount":
		config.Consumer.SourceCount, err = strconv.Atoi(values[1])
	case "selectBig":
		config.Consumer.SelectBig, err = strconv.ParseBool(values[1])
	case "rollbackIfConfirmTimeout":
		config.Consumer.RollbackIfConfirmTimeout, err = strconv.ParseBool(values[1])
	case "maxSubInfoReportInterval":
		config.Consumer.MaxSubInfoReportInterval, err = strconv.Atoi(values[1])
	case "maxPartCheckPeriod":
		config.Consumer.MaxPartCheckPeriod, err = parseDuration(values[1])
	case "partCheckSlice":
		config.Consumer.PartCheckSlice, err = parseDuration(values[1])
	case "msgNotFoundWait":
		config.Consumer.MsgNotFoundWait, err = parseDuration(values[1])
	case "rebConfirmWait":
		config.Consumer.RebConfirmWait, err = parseDuration(values[1])
	case "maxConfirmWait":
		config.Consumer.MaxConfirmWait, err = parseDuration(values[1])
	case "shutdownRebWait":
		config.Consumer.ShutdownRebWait, err = parseDuration(values[1])
	case "heartbeatInterval":
		config.Heartbeat.Interval, err = parseDuration(values[1])
	case "heartbeatMaxRetryTimes":
		config.Heartbeat.MaxRetryTimes, err = strconv.Atoi(values[1])
	case "heartbeatAfterFail":
		config.Heartbeat.AfterFail, err = parseDuration(values[1])
	case "authEnable":
		config.Net.Auth.Enable, err = strconv.ParseBool(values[1])
	case "authUserName":
		config.Net.Auth.UserName = values[1]
	case "authPassword":
		config.Net.Auth.Password = values[1]
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
