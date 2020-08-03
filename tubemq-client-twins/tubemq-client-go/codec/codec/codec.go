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
	"bytes"
	"encoding/binary"
	"errors"

	"github.com/apache/incubator-tubemq/tubemq-client-twins/tubemq-client-go/constants"
)

// Codec defines the codec specification for data
type Codec interface {
	Encode(request RequestWrapper) ([]byte, error)
	Decode([]byte) ([]byte, error)
}

// DefaultCodec defines the default codec
var DefaultCodec = NewCodec()

// NewCodec returns a globally unique codec
var NewCodec = func () Codec {
	return &defaultCodec{}
}

type defaultCodec struct{}

func (d *defaultCodec) Encode(request RequestWrapper) ([]byte, error) {

	dataBlockCount := calculateTotalLen(request.Payload)
	totalLen := 32 + 32 + 32 + dataBlockCount * constants.Data_Block_Lenth
	buffer := bytes.NewBuffer(make([]byte, 0, totalLen))

	if err := binary.Write(buffer, binary.BigEndian, constants.Rpc_Protocol_Begin_Token); err != nil {
		return nil, err
	}

	if err := binary.Write(buffer, binary.BigEndian, request.SerialNo); err != nil {
		return nil, err
	}

	if err := binary.Write(buffer, binary.BigEndian, dataBlockCount); err != nil {
		return nil, err
	}

	if err := binary.Write(buffer, binary.BigEndian, request.Payload); err != nil {
		return nil, err
	}

	return buffer.Bytes(), nil
}

func (d *defaultCodec) Decode(payload []byte) ([]byte, error) {

	if len(payload) < constants.Frame_Header_Lenth {
		return []byte(""), errors.New("invalid data frame")
	}

	return payload[constants.Frame_Header_Lenth:], nil
}

func calculateTotalLen(payload []byte) int {
	if len(payload) % 8196 == 0 {
		return len(payload) / constants.Data_Block_Lenth
	}

	return len(payload) / constants.Data_Block_Lenth + 1
}