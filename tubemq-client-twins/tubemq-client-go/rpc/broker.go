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

package rpc

import (
	"context"

	"github.com/golang/protobuf/proto"

	"github.com/apache/incubator-inlong/tubemq-client-twins/tubemq-client-go/client"
	"github.com/apache/incubator-inlong/tubemq-client-twins/tubemq-client-go/codec"
	"github.com/apache/incubator-inlong/tubemq-client-twins/tubemq-client-go/errs"
	"github.com/apache/incubator-inlong/tubemq-client-twins/tubemq-client-go/metadata"
	"github.com/apache/incubator-inlong/tubemq-client-twins/tubemq-client-go/protocol"
)

const (
	Register   = 31
	Unregister = 32
)

const (
	BrokerProducerRegister = iota + 11
	BrokerProducerHeartbeat
	BrokerProducerSendMsg
	BrokerProducerClose
	BrokerConsumerRegister
	BrokerConsumerHeartbeat
	BrokerConsumerGetMsg
	BrokerConsumerCommit
	BrokerConsumerClose
)

// RegisterRequestC2B implements the RegisterRequestC2B interface according to TubeMQ RPC protocol.
func (c *rpcClient) RegisterRequestC2B(metadata *metadata.Metadata, sub *client.SubInfo) (*protocol.RegisterResponseB2C, error) {
	reqC2B := &protocol.RegisterRequestC2B{
		OpType:        proto.Int32(Register),
		ClientId:      proto.String(sub.GetClientID()),
		GroupName:     proto.String(metadata.GetSubscribeInfo().GetGroup()),
		TopicName:     proto.String(metadata.GetSubscribeInfo().GetPartition().GetTopic()),
		PartitionId:   proto.Int32(metadata.GetSubscribeInfo().GetPartition().GetPartitionID()),
		QryPriorityId: proto.Int32(metadata.GetSubscribeInfo().GetQryPriorityID()),
		ReadStatus:    proto.Int32(metadata.GetReadStatus()),
		AuthInfo:      sub.GetAuthorizedInfo(),
	}
	if sub.IsFiltered(metadata.GetSubscribeInfo().GetPartition().GetTopic()) {
		tfs := sub.GetTopicFilters()
		reqC2B.FilterCondStr = make([]string, 0, len(tfs[metadata.GetSubscribeInfo().GetPartition().GetTopic()]))
		for _, tf := range tfs[metadata.GetSubscribeInfo().GetPartition().GetTopic()] {
			reqC2B.FilterCondStr = append(reqC2B.FilterCondStr, tf)
		}
	}
	offset := sub.GetAssignedPartOffset(metadata.GetSubscribeInfo().GetPartition().GetPartitionKey())
	if offset != client.InValidOffset {
		reqC2B.CurrOffset = proto.Int64(offset)
	}
	data, err := proto.Marshal(reqC2B)
	if err != nil {
		return nil, errs.New(errs.RetMarshalFailure, err.Error())
	}
	req := codec.NewRPCRequest()
	req.RpcHeader = &protocol.RpcConnHeader{
		Flag: proto.Int32(0),
	}
	req.RequestHeader = &protocol.RequestHeader{
		ServiceType: proto.Int32(ReadService),
		ProtocolVer: proto.Int32(2),
	}
	req.RequestBody = &protocol.RequestBody{
		Method:  proto.Int32(BrokerConsumerRegister),
		Request: data,
		Timeout: proto.Int64(c.config.Net.ReadTimeout.Milliseconds()),
	}
	ctx, cancel := context.WithTimeout(context.Background(), c.config.Net.ReadTimeout)
	defer cancel()
	rsp, err := c.client.DoRequest(ctx, req)
	if v, ok := rsp.(*codec.TubeMQRPCResponse); ok {
		if v.ResponseException != nil {
			return nil, errs.New(errs.RetResponseException, v.ResponseException.String())
		}
		rspC2B := &protocol.RegisterResponseB2C{}
		err := proto.Unmarshal(v.ResponseBody.Data, rspC2B)
		if err != nil {
			return nil, errs.New(errs.RetUnMarshalFailure, err.Error())
		}
		return rspC2B, nil
	}
	return nil, errs.ErrAssertionFailure
}

// UnregisterRequestC2B implements the UnregisterRequestC2B interface according to TubeMQ RPC protocol.
func (c *rpcClient) UnregisterRequestC2B(metadata metadata.Metadata, sub *client.SubInfo) (*protocol.RegisterResponseB2C, error) {
	reqC2B := &protocol.RegisterRequestC2B{
		OpType:      proto.Int32(Unregister),
		ClientId:    proto.String(sub.GetClientID()),
		GroupName:   proto.String(metadata.GetSubscribeInfo().GetGroup()),
		TopicName:   proto.String(metadata.GetSubscribeInfo().GetPartition().GetTopic()),
		PartitionId: proto.Int32(metadata.GetSubscribeInfo().GetPartition().GetPartitionID()),
		ReadStatus:  proto.Int32(metadata.GetReadStatus()),
		AuthInfo:    sub.GetAuthorizedInfo(),
	}
	req := codec.NewRPCRequest()
	req.RpcHeader = &protocol.RpcConnHeader{
		Flag: proto.Int32(0),
	}
	data, err := proto.Marshal(reqC2B)
	if err != nil {
		return nil, errs.New(errs.RetMarshalFailure, err.Error())
	}
	req.RequestHeader = &protocol.RequestHeader{
		ServiceType: proto.Int32(ReadService),
		ProtocolVer: proto.Int32(2),
	}
	req.RequestBody = &protocol.RequestBody{
		Method:  proto.Int32(BrokerConsumerRegister),
		Request: data,
		Timeout: proto.Int64(c.config.Net.ReadTimeout.Milliseconds()),
	}
	ctx, cancel := context.WithTimeout(context.Background(), c.config.Net.ReadTimeout)
	defer cancel()
	rsp, err := c.client.DoRequest(ctx, req)
	if v, ok := rsp.(*codec.TubeMQRPCResponse); ok {
		if v.ResponseException != nil {
			return nil, errs.New(errs.RetResponseException, v.ResponseException.String())
		}
		rspC2B := &protocol.RegisterResponseB2C{}
		err := proto.Unmarshal(v.ResponseBody.Data, rspC2B)
		if err != nil {
			return nil, errs.New(errs.RetUnMarshalFailure, err.Error())
		}
		return rspC2B, nil
	}
	return nil, errs.ErrAssertionFailure
}

// GetMessageRequestC2B implements the GetMessageRequestC2B interface according to TubeMQ RPC protocol.
func (c *rpcClient) GetMessageRequestC2B(metadata *metadata.Metadata, sub *client.SubInfo, r *client.RmtDataCache) (*protocol.GetMessageResponseB2C, error) {
	reqC2B := &protocol.GetMessageRequestC2B{
		ClientId:           proto.String(sub.GetClientID()),
		PartitionId:        proto.Int32(metadata.GetSubscribeInfo().GetPartition().GetPartitionID()),
		GroupName:          proto.String(metadata.GetSubscribeInfo().GetGroup()),
		TopicName:          proto.String(metadata.GetSubscribeInfo().GetPartition().GetTopic()),
		EscFlowCtrl:        proto.Bool(r.GetUnderGroupCtrl()),
		LastPackConsumed:   proto.Bool(metadata.GetSubscribeInfo().GetPartition().GetLastConsumed()),
		ManualCommitOffset: proto.Bool(false),
	}
	req := codec.NewRPCRequest()
	req.RpcHeader = &protocol.RpcConnHeader{
		Flag: proto.Int32(0),
	}
	data, err := proto.Marshal(reqC2B)
	if err != nil {
		return nil, errs.New(errs.RetMarshalFailure, err.Error())
	}
	req.RequestHeader = &protocol.RequestHeader{
		ServiceType: proto.Int32(ReadService),
		ProtocolVer: proto.Int32(2),
	}
	req.RequestBody = &protocol.RequestBody{
		Method:  proto.Int32(BrokerConsumerGetMsg),
		Request: data,
		Timeout: proto.Int64(c.config.Net.ReadTimeout.Milliseconds()),
	}
	ctx, cancel := context.WithTimeout(context.Background(), c.config.Net.ReadTimeout)
	defer cancel()
	rsp, err := c.client.DoRequest(ctx, req)
	if v, ok := rsp.(*codec.TubeMQRPCResponse); ok {
		if v.ResponseException != nil {
			return nil, errs.New(errs.RetResponseException, v.ResponseException.String())
		}
		rspC2B := &protocol.GetMessageResponseB2C{}
		err := proto.Unmarshal(v.ResponseBody.Data, rspC2B)
		if err != nil {
			return nil, errs.New(errs.RetUnMarshalFailure, err.Error())
		}
		return rspC2B, nil
	}
	return nil, errs.ErrAssertionFailure
}

// CommitOffsetRequestC2B implements the CommitOffsetRequestC2B interface according to TubeMQ RPC protocol.
func (c *rpcClient) CommitOffsetRequestC2B(metadata *metadata.Metadata, sub *client.SubInfo) (*protocol.CommitOffsetResponseB2C, error) {
	reqC2B := &protocol.CommitOffsetRequestC2B{
		ClientId:         proto.String(sub.GetClientID()),
		TopicName:        proto.String(metadata.GetSubscribeInfo().GetPartition().GetTopic()),
		PartitionId:      proto.Int32(metadata.GetSubscribeInfo().GetPartition().GetPartitionID()),
		GroupName:        proto.String(metadata.GetSubscribeInfo().GetGroup()),
		LastPackConsumed: proto.Bool(metadata.GetSubscribeInfo().GetPartition().GetLastConsumed()),
	}
	req := codec.NewRPCRequest()
	req.RpcHeader = &protocol.RpcConnHeader{
		Flag: proto.Int32(10),
	}
	data, err := proto.Marshal(reqC2B)
	if err != nil {
		return nil, errs.New(errs.RetMarshalFailure, err.Error())
	}
	req.RequestHeader = &protocol.RequestHeader{
		ServiceType: proto.Int32(ReadService),
		ProtocolVer: proto.Int32(2),
	}
	req.RequestBody = &protocol.RequestBody{
		Method:  proto.Int32(BrokerConsumerHeartbeat),
		Request: data,
		Timeout: proto.Int64(c.config.Net.ReadTimeout.Milliseconds()),
	}
	ctx, cancel := context.WithTimeout(context.Background(), c.config.Net.ReadTimeout)
	defer cancel()
	rsp, err := c.client.DoRequest(ctx, req)
	if v, ok := rsp.(*codec.TubeMQRPCResponse); ok {
		if v.ResponseException != nil {
			return nil, errs.New(errs.RetResponseException, v.ResponseException.String())
		}
		rspC2B := &protocol.CommitOffsetResponseB2C{}
		err := proto.Unmarshal(v.ResponseBody.Data, rspC2B)
		if err != nil {
			return nil, errs.New(errs.RetUnMarshalFailure, err.Error())
		}
		return rspC2B, nil
	}
	return nil, errs.ErrAssertionFailure
}

// HeartbeatRequestC2B implements the HeartbeatRequestC2B interface according to TubeMQ RPC protocol.
func (c *rpcClient) HeartbeatRequestC2B(metadata *metadata.Metadata, sub *client.SubInfo, r *client.RmtDataCache) (*protocol.HeartBeatResponseB2C, error) {
	reqC2B := &protocol.HeartBeatRequestC2B{
		ClientId:      proto.String(sub.GetClientID()),
		GroupName:     proto.String(metadata.GetSubscribeInfo().GetGroup()),
		ReadStatus:    proto.Int32(metadata.GetReadStatus()),
		QryPriorityId: proto.Int32(metadata.GetSubscribeInfo().GetQryPriorityID()),
		AuthInfo:      sub.GetAuthorizedInfo(),
	}
	partitions := r.GetPartitionByBroker(metadata.GetNode())
	reqC2B.PartitionInfo = make([]string, 0, len(partitions))
	for _, partition := range partitions {
		reqC2B.PartitionInfo = append(reqC2B.PartitionInfo, partition.String())
	}
	data, err := proto.Marshal(reqC2B)
	if err != nil {
		return nil, errs.New(errs.RetMarshalFailure, err.Error())
	}
	req := codec.NewRPCRequest()
	req.RequestHeader = &protocol.RequestHeader{
		ServiceType: proto.Int32(ReadService),
		ProtocolVer: proto.Int32(2),
	}
	req.RequestBody = &protocol.RequestBody{
		Method:  proto.Int32(BrokerConsumerHeartbeat),
		Request: data,
		Timeout: proto.Int64(c.config.Net.ReadTimeout.Milliseconds()),
	}
	req.RpcHeader = &protocol.RpcConnHeader{
		Flag: proto.Int32(0),
	}
	ctx, cancel := context.WithTimeout(context.Background(), c.config.Net.ReadTimeout)
	defer cancel()
	rsp, err := c.client.DoRequest(ctx, req)
	if v, ok := rsp.(*codec.TubeMQRPCResponse); ok {
		if v.ResponseException != nil {
			return nil, errs.New(errs.RetResponseException, v.ResponseException.String())
		}
		rspC2B := &protocol.HeartBeatResponseB2C{}
		err := proto.Unmarshal(v.ResponseBody.Data, rspC2B)
		if err != nil {
			return nil, errs.New(errs.RetUnMarshalFailure, err.Error())
		}
		return rspC2B, nil
	}
	return nil, errs.ErrAssertionFailure
}
