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

type Framer struct {
	reader io.Reader
	msg    []byte
}

func New(reader io.Reader) *Framer {
	bufferReader := bufio.NewReaderSize(reader, maxBufferSize)
	return &Framer{
		msg:    make([]byte, defaultMsgSize),
		reader: bufferReader,
	}
}

func (f *Framer) Decode() (*FrameResponse, error) {
	num, err := io.ReadFull(f.reader, f.msg[:frameHeadLen])
	if err != nil {
		return nil, err
	}
	if num != int(frameHeadLen) {
		return nil, errors.New("framer: read frame header num invalid")
	}
	token := binary.BigEndian.Uint32(f.msg[:beginTokenLen])
	if token != RPCProtocolBeginToken {
		return nil, errors.New("framer: read framer rpc protocol begin token not match")
	}
	num, err = io.ReadFull(f.reader, f.msg[frameHeadLen:frameHeadLen+listSizeLen])
	if num != int(listSizeLen) {
		return nil, errors.New("framer: read invalid list size num")
	}
	listSize := binary.BigEndian.Uint32(f.msg[frameHeadLen : frameHeadLen+listSizeLen])
	totalLen := int(frameHeadLen)
	size := make([]byte, 4)
	for i := 0; i < int(listSize); i++ {
		n, err := io.ReadFull(f.reader, size)
		if err != nil {
			return nil, err
		}
		if n != int(dataLen) {
			return nil, errors.New("framer: read invalid size")
		}

		s := int(binary.BigEndian.Uint32(size))
		if totalLen+s > len(f.msg) {
			data := f.msg[:totalLen]
			f.msg = make([]byte, totalLen+s)
			copy(f.msg, data[:])
		}

		num, err = io.ReadFull(f.reader, f.msg[totalLen:totalLen+s])
		if err != nil {
			return nil, err
		}
		if num != s {
			return nil, errors.New("framer: read invalid data")
		}
		totalLen += s
	}

	data := make([]byte, totalLen - int(frameHeadLen))
	copy(data, f.msg[frameHeadLen:totalLen])

	return &FrameResponse{
		serialNo:    binary.BigEndian.Uint32(f.msg[beginTokenLen : beginTokenLen+serialNoLen]),
		responseBuf: data,
	}, nil
}

type FrameRequest struct {
	requestID uint32
	req       []byte
}

type FrameResponse struct {
	serialNo    uint32
	responseBuf []byte
}

func (f *FrameResponse) GetSerialNo() uint32 {
	return f.serialNo
}

func (f *FrameResponse) GetResponseBuf() []byte {
	return f.responseBuf
}

type Codec struct{}
