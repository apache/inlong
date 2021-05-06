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
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"io"

	"github.com/golang/protobuf/proto"

	"github.com/apache/incubator-inlong/tubemq-client-twins/tubemq-client-go/protocol"
)

const (
	RPCProtocolBeginToken uint32 = 0xFF7FF4FE
	RPCMaxBufferSize      int    = 8192
	frameHeadLen          uint32 = 12
	maxBufferSize         int    = 128 * 1024
	defaultMsgSize        int    = 4096
	dataLen               uint32 = 4
	listSizeLen           uint32 = 4
	serialNoLen           uint32 = 4
	beginTokenLen         uint32 = 4
)

// TransportResponse is the abstraction of the transport response.
type TransportResponse interface {
	// GetSerialNo returns the `serialNo` of the corresponding request.
	GetSerialNo() uint32
	// GetResponseBuf returns the body of the response.
	GetResponseBuf() []byte
}

// Decoder is the abstraction of the decoder which is used to decode the response.
type Decoder interface {
	// Decode will decode the response to frame head and body.
	Decode() (TransportResponse, error)
}

// TubeMQDecoder is the implementation of the decoder of response from TubeMQ.
type TubeMQDecoder struct {
	reader io.Reader
	msg    []byte
}

// New will return a default TubeMQDecoder.
func New(reader io.Reader) *TubeMQDecoder {
	bufferReader := bufio.NewReaderSize(reader, maxBufferSize)
	return &TubeMQDecoder{
		msg:    make([]byte, defaultMsgSize),
		reader: bufferReader,
	}
}

// Decode will decode the response from TubeMQ to TransportResponse according to
// the RPC protocol of TubeMQ.
func (t *TubeMQDecoder) Decode() (TransportResponse, error) {
	num, err := io.ReadFull(t.reader, t.msg[:frameHeadLen])
	if err != nil {
		return nil, err
	}
	if num != int(frameHeadLen) {
		return nil, errors.New("framer: read frame header num invalid")
	}
	token := binary.BigEndian.Uint32(t.msg[:beginTokenLen])
	if token != RPCProtocolBeginToken {
		return nil, errors.New("framer: read framer rpc protocol begin token not match")
	}
	serialNo := binary.BigEndian.Uint32(t.msg[beginTokenLen : beginTokenLen+serialNoLen])
	listSize := binary.BigEndian.Uint32(t.msg[beginTokenLen+serialNoLen : beginTokenLen+serialNoLen+listSizeLen])
	totalLen := int(frameHeadLen)
	size := make([]byte, 4)
	for i := 0; i < int(listSize); i++ {
		n, err := io.ReadFull(t.reader, size)
		if err != nil {
			return nil, err
		}
		if n != int(dataLen) {
			return nil, errors.New("framer: read invalid size")
		}

		s := int(binary.BigEndian.Uint32(size))
		if totalLen+s > len(t.msg) {
			data := t.msg[:totalLen]
			t.msg = make([]byte, totalLen+s)
			copy(t.msg, data[:])
		}

		num, err = io.ReadFull(t.reader, t.msg[totalLen:totalLen+s])
		if err != nil {
			return nil, err
		}
		if num != s {
			return nil, errors.New("framer: read invalid data")
		}
		totalLen += s
	}

	data := make([]byte, totalLen-int(frameHeadLen))
	copy(data, t.msg[frameHeadLen:totalLen])

	return TubeMQResponse{
		serialNo:    serialNo,
		responseBuf: data,
	}, nil
}

// TubeMQRequest is the implementation of TubeMQ request.
type TubeMQRequest struct {
	serialNo uint32
	req      []byte
}

// TubeMQResponse is the TubeMQ implementation of TransportResponse.
type TubeMQResponse struct {
	serialNo    uint32
	responseBuf []byte
}

// GetSerialNo will return the SerialNo of TubeMQResponse.
func (t TubeMQResponse) GetSerialNo() uint32 {
	return t.serialNo
}

// GetResponseBuf will return the body of TubeMQResponse.
func (t TubeMQResponse) GetResponseBuf() []byte {
	return t.responseBuf
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
	Decode([]byte) (*RpcResponse, error)
}

// TubeMQCodec is the default encoding and decoding interface for TubeMQ.
type TubeMQCodec struct{}

// Encode encodes the RpcRequest to bytes according to the TubeMQ RPC protocol.
func (t *TubeMQCodec) Encode(serialNo uint32, req *RpcRequest) ([]byte, error) {
	data, err := encodeRequest(req)
	if err != nil {
		return nil, err
	}

	contentLen := int(dataLen) + int(dataLen) + int(dataLen) + len(data)
	listSize := calcBlockCount(contentLen)

	buf := bytes.NewBuffer(make([]byte, 0, int(frameHeadLen)+listSize*(RPCMaxBufferSize)))

	if err := binary.Write(buf, binary.BigEndian, RPCProtocolBeginToken); err != nil {
		return nil, err
	}
	if err := binary.Write(buf, binary.BigEndian, serialNo); err != nil {
		return nil, err
	}
	if err := binary.Write(buf, binary.BigEndian, uint32(listSize)); err != nil {
		return nil, err
	}

	begin := 0
	for i := 0; i < listSize; i++ {
		blockLen := contentLen - i*RPCMaxBufferSize
		if blockLen > RPCMaxBufferSize {
			blockLen = RPCMaxBufferSize
		}
		if err := binary.Write(buf, binary.BigEndian, uint32(blockLen)); err != nil {
			return nil, err
		}
		if err := binary.Write(buf, binary.BigEndian, data[begin:begin+blockLen]); err != nil {
			return nil, err
		}
		begin += blockLen
	}
	return buf.Bytes(), nil
}

func encodeRequest(req *RpcRequest) ([]byte, error) {
	rpcHeader, err := writeDelimitedTo(req.RpcHeader)
	if err != nil {
		return nil, err
	}
	requestHeader, err := writeDelimitedTo(req.RequestHeader)
	if err != nil {
		return nil, err
	}
	requestBody, err := writeDelimitedTo(req.RequestBody)
	if err != nil {
		return nil, err
	}
	return append(append(rpcHeader, requestHeader...), requestBody...), nil
}

func calcBlockCount(contentSize int) int {
	blockCount := contentSize / RPCMaxBufferSize
	remained := contentSize % RPCMaxBufferSize
	if remained > 0 {
		blockCount++
	}
	return blockCount
}

func writeDelimitedTo(msg proto.Message) ([]byte, error) {
	dataLen := proto.EncodeVarint(uint64(proto.Size(msg)))
	data, err := proto.Marshal(msg)
	if err != nil {
		return nil, err
	}
	return append(dataLen, data...), nil
}

// Decode decodes the TransportResponse to RpcResponse according to the TubeMQ RPC protocol.
func (t *TubeMQCodec) Decode(response TransportResponse) (*RpcResponse, error) {
	data := response.GetResponseBuf()
	rpcHeader := &protocol.RpcConnHeader{}
	data, err := readDelimitedFrom(data, rpcHeader)
	if err != nil {
		return nil, err
	}
	rspHeader := &protocol.ResponseHeader{}
	data, err = readDelimitedFrom(data, rspHeader)
	if err != nil {
		return nil, err
	}

	if rspHeader.GetStatus() == protocol.ResponseHeader_SUCCESS {
		rspBody := &protocol.RspResponseBody{}
		data, err = readDelimitedFrom(data, rspBody)
		if err != nil {
			return nil, err
		}
		return &RpcResponse{
			SerialNo:       response.GetSerialNo(),
			RpcHeader:      rpcHeader,
			ResponseHeader: rspHeader,
			ResponseBody:   rspBody,
		}, nil
	}

	rspException := &protocol.RspExceptionBody{}
	data, err = readDelimitedFrom(data, rspException)
	if err != nil {
		return nil, err
	}
	return &RpcResponse{
		SerialNo:          response.GetSerialNo(),
		RpcHeader:         rpcHeader,
		ResponseHeader:    rspHeader,
		ResponseException: rspException,
	}, nil
}

func readDelimitedFrom(data []byte, msg proto.Message) ([]byte, error) {
	size, n := proto.DecodeVarint(data)
	if size == 0 && n == 0 {
		return nil, errors.New("decode: invalid data len")
	}

	if err := proto.Unmarshal(data[n:n+int(size)], msg); err != nil {
		return nil, err
	}

	return data[int(size)+n:], nil
}
