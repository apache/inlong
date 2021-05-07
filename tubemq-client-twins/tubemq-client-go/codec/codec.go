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

// Package codec defines the encoding and decoding logic between TubeMQ.
// If the protocol of encoding and decoding is changed, only this package
// will need to be changed.
package codec

import (
	"github.com/apache/incubator-inlong/tubemq-client-twins/tubemq-client-go/protocol"
)

// Response is the abstraction of the transport response.
type Response interface {
	// GetSerialNo returns the `serialNo` of the corresponding request.
	GetSerialNo() uint32
	// GetBuffer returns the body of the response.
	GetBuffer() []byte
}

// Decoder is the abstraction of the decoder which is used to decode the response.
type Decoder interface {
	// Decode will decode the response to frame head and body.
	Decode() (Response, error)
}

// RpcRequest represents the RPC request to TubeMQ.
type RpcRequest struct {
	RpcHeader     *protocol.RpcConnHeader
	RequestHeader *protocol.RequestHeader
	RequestBody   *protocol.RequestBody
}

// RpcResponse represents the RPC response from TubeMQ.
type RpcResponse struct {
	SerialNo          uint32
	RpcHeader         *protocol.RpcConnHeader
	ResponseHeader    *protocol.ResponseHeader
	ResponseBody      *protocol.RspResponseBody
	ResponseException *protocol.RspExceptionBody
}

// Codec represents the encoding and decoding interface.
type Codec interface {
	Encode(serialNo uint32, req *RpcRequest) ([]byte, error)
	Decode(rsp Response) (*RpcResponse, error)
}
