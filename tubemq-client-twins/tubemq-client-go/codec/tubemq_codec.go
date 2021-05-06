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
	"encoding/binary"
	"errors"
	"io"
)

const (
	// The default begin token of TubeMQ RPC protocol.
	RPCProtocolBeginToken uint32 = 0xFF7FF4FE
	// The default max buffer size the RPC response.
	RPCMaxBufferSize uint32 = 8192
	frameHeadLen     uint32 = 12
	maxBufferSize    int    = 128 * 1024
	defaultMsgSize   int    = 4096
	dataLen          uint32 = 4
	listSizeLen      uint32 = 4
	serialNoLen      uint32 = 4
	beginTokenLen    uint32 = 4
)

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

// Decode will decode the response from TubeMQ to Response according to
// the RPC protocol of TubeMQ.
func (t *TubeMQDecoder) Decode() (Response, error) {
	var num int
	var err error
	if num, err = io.ReadFull(t.reader, t.msg[:frameHeadLen]); err != nil {
		return nil, err
	}
	if num != int(frameHeadLen) {
		return nil, errors.New("framer: read frame header num invalid")
	}
	if binary.BigEndian.Uint32(t.msg[:beginTokenLen]) != RPCProtocolBeginToken {
		return nil, errors.New("framer: read framer rpc protocol begin token not match")
	}
	serialNo := binary.BigEndian.Uint32(t.msg[beginTokenLen : beginTokenLen+serialNoLen])
	listSize := binary.BigEndian.Uint32(t.msg[beginTokenLen+serialNoLen : beginTokenLen+serialNoLen+listSizeLen])
	totalLen := int(frameHeadLen)
	for i := 0; i < int(listSize); i++ {
		size := make([]byte, 4)
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
			t.msg = make([]byte, 0, max(2*len(t.msg), totalLen+s))
			copy(t.msg, data[:])
		}

		if num, err = io.ReadFull(t.reader, t.msg[totalLen:totalLen+s]); err != nil {
			return nil, err
		}
		if num != s {
			return nil, errors.New("framer: read invalid data")
		}
		totalLen += s
	}

	data := make([]byte, totalLen-int(frameHeadLen))
	copy(data, t.msg[frameHeadLen:totalLen])

	return &TubeMQResponse{
		serialNo: serialNo,
		Buffer:   data,
	}, nil
}

// TubeMQRequest is the implementation of TubeMQ request.
type TubeMQRequest struct {
	serialNo uint32
	req      []byte
}

// TubeMQResponse is the TubeMQ implementation of Response.
type TubeMQResponse struct {
	serialNo uint32
	Buffer   []byte
}

// GetSerialNo will return the SerialNo of Response.
func (t TubeMQResponse) GetSerialNo() uint32 {
	return t.serialNo
}

// GetResponseBuf will return the body of Response.
func (t TubeMQResponse) GetBuffer() []byte {
	return t.Buffer
}

func max(x, y int) int {
	if x < y {
		return y
	}
	return x
}
