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

// Package rpc encapsulates all the rpc request to TubeMQ.
package rpc

import (
	"context"

	"github.com/apache/incubator-inlong/tubemq-client-twins/tubemq-client-go/client"
	"github.com/apache/incubator-inlong/tubemq-client-twins/tubemq-client-go/config"
	"github.com/apache/incubator-inlong/tubemq-client-twins/tubemq-client-go/metadata"
	"github.com/apache/incubator-inlong/tubemq-client-twins/tubemq-client-go/multiplexing"
	"github.com/apache/incubator-inlong/tubemq-client-twins/tubemq-client-go/protocol"
	"github.com/apache/incubator-inlong/tubemq-client-twins/tubemq-client-go/transport"
)

const (
	ReadService = 2
	AdminService = 4
)

// RPCClient is the rpc level client to interact with TubeMQ.
type RPCClient interface {
	// RegisterRequestC2B is the rpc request for a consumer to register to a broker.
	RegisterRequestC2B(metadata *metadata.Metadata, sub *client.SubInfo) (*protocol.RegisterResponseB2C, error)
	// UnregisterRequestC2B is the rpc request for a consumer to unregister to a broker.
	UnregisterRequestC2B(metadata metadata.Metadata, sub *client.SubInfo) (*protocol.RegisterResponseB2C, error)
	// GetMessageRequestC2B is the rpc request for a consumer to get message from a broker.
	GetMessageRequestC2B(metadata *metadata.Metadata, sub *client.SubInfo, r *client.RmtDataCache) (*protocol.GetMessageResponseB2C, error)
	// CommitOffsetRequestC2B is the rpc request for a consumer to commit offset to a broker.
	CommitOffsetRequestC2B(metadata *metadata.Metadata, sub *client.SubInfo) (*protocol.CommitOffsetResponseB2C, error)
	// HeartbeatRequestC2B is the rpc request for a consumer to send heartbeat to a broker.
	HeartbeatRequestC2B(metadata *metadata.Metadata, sub *client.SubInfo, r *client.RmtDataCache) (*protocol.HeartBeatResponseB2C, error)
	// RegisterRequestC2M is the rpc request for a consumer to register request to master.
	RegisterRequestC2M(metadata *metadata.Metadata, sub *client.SubInfo, r *client.RmtDataCache) (*protocol.RegisterResponseM2C, error)
	// HeartRequestC2M is the rpc request for a consumer to send heartbeat to master.
	HeartRequestC2M(metadata *metadata.Metadata, sub *client.SubInfo, r *client.RmtDataCache) (*protocol.HeartResponseM2C, error)
	// CloseRequestC2M is the rpc request for a consumer to be closed to master.
	CloseRequestC2M(metadata *metadata.Metadata, sub *client.SubInfo) (*protocol.CloseResponseM2C, error)
}

type rpcClient struct {
	pool   *multiplexing.Pool
	client *transport.Client
	ctx    context.Context
	config *config.Config
}

// New returns a default TubeMQ rpc Client
func New(pool *multiplexing.Pool, opts *transport.Options, ctx context.Context, config *config.Config) RPCClient {
	return &rpcClient{
		pool:   pool,
		client: transport.New(opts, pool),
		ctx:    ctx,
		config: config,
	}
}
