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

package tubeclient

import "errors"

// Consumer defines a basic contract that all consumers need to follow
type Consumer interface {
	// Subscribe defines a subscription to the topic
	Subscribe(topic string)
}

// PullConsumer implements Consumber with the pull consumption
type PullConsumer struct {
	config *Config
}

// New create a PullConsumer instance
func New(config *Config) *PullConsumer {
	if err := checkConfig(config); err != nil {
		panic(err)
	}

	// TODO: add log
	c := &PullConsumer{
		config: config,
	}

	return c
}

func checkConfig(config *Config) error {
	if config == nil {
		return errors.New("consumer config cannot be nil")
	}

	if config.Address == "" {
		return errors.New("consumer config err, address cannot be empty")
	}

	if config.Group == "" {
		return errors.New("consumer config err, group cannot be empty")
	}

	return nil
}