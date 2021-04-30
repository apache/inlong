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

package codec

import (
	"bufio"
	"encoding/binary"
	"errors"
	"io"
)

const (
	RPCProtocolBeginToken uint32 = 0xFF7FF4FE
	RPCMaxBufferSize      uint32 = 8192
	frameHeadLen          uint32 = 8
	maxBufferSize         int    = 128 * 1024
	defaultMsgSize        int    = 4096
	dataLen               uint32 = 4
	listSizeLen           uint32 = 4
	serialNoLen           uint32 = 4
	beginTokenLen         uint32 = 4
)

type TransportResponse interface {
	GetSerialNo() uint32
	GetResponseBuf() []byte
}

type Decoder interface {
	Decode() (TransportResponse, error)
}

type TubeMQDecoder struct {
	reader io.Reader
	msg    []byte
}

func New(reader io.Reader) *TubeMQDecoder {
	bufferReader := bufio.NewReaderSize(reader, maxBufferSize)
	return &TubeMQDecoder{
		msg:    make([]byte, defaultMsgSize),
		reader: bufferReader,
	}
}

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
	num, err = io.ReadFull(t.reader, t.msg[frameHeadLen:frameHeadLen+listSizeLen])
	if num != int(listSizeLen) {
		return nil, errors.New("framer: read invalid list size num")
	}
	listSize := binary.BigEndian.Uint32(t.msg[frameHeadLen : frameHeadLen+listSizeLen])
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
		serialNo:    binary.BigEndian.Uint32(t.msg[beginTokenLen : beginTokenLen+serialNoLen]),
		responseBuf: data,
	}, nil
}

type TubeMQRequest struct {
	serialNo uint32
	req      []byte
}

type TubeMQResponse struct {
	serialNo    uint32
	responseBuf []byte
}

func (t TubeMQResponse) GetSerialNo() uint32 {
	return t.serialNo
}

func (t TubeMQResponse) GetResponseBuf() []byte {
	return t.responseBuf
}
