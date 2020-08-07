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
	"errors"
	"math"
	"sync"

	"github.com/golang/protobuf/proto"
)

// Serialization defines the serialization of data
type Serialization interface {
	Marshal(interface{}) ([]byte, error)
	Unmarshal([]byte, interface{}) error
}

const (
	Proto   = "proto"   // protobuf
	MsgPack = "msgpack" // msgpack
	Json    = "json"    // json
)

var serializationMap = make(map[string]Serialization)

// DefaultSerialization defines the default serialization
var DefaultSerialization = NewSerialization()

// NewSerialization returns a globally unique serialization
var NewSerialization = func() Serialization {
	return &pbSerialization{}
}

func init() {
	registerSerialization("proto", DefaultSerialization)
}

func registerSerialization(name string, serialization Serialization) {
	if serializationMap == nil {
		serializationMap = make(map[string]Serialization)
	}
	serializationMap[name] = serialization
}

// GetSerialization get a Serialization by a serialization name
func GetSerialization(name string) Serialization {
	if v, ok := serializationMap[name]; ok {
		return v
	}
	return DefaultSerialization
}

type pbSerialization struct{}

func (d *pbSerialization) Marshal(v interface{}) ([]byte, error) {
	if v == nil {
		return nil, errors.New("marshal nil interface{}")
	}
	if pm, ok := v.(proto.Marshaler); ok {
		// unmarshal itself, without buffer
		return pm.Marshal()
	}
	buffer := bufferPool.Get().(*cachedBuffer)
	protoMsg := v.(proto.Message)
	lastMarshaledSize := make([]byte, 0, buffer.lastMarshaledSize)
	buffer.SetBuf(lastMarshaledSize)
	buffer.Reset()

	if err := buffer.Marshal(protoMsg); err != nil {
		return nil, err
	}
	data := buffer.Bytes()
	buffer.lastMarshaledSize = upperLimit(len(data))
	buffer.SetBuf(nil)
	bufferPool.Put(buffer)

	return data, nil
}

func (d *pbSerialization) Unmarshal(data []byte, v interface{}) error {
	if data == nil || len(data) == 0 {
		return errors.New("unmarshal nil or empty bytes")
	}

	protoMsg := v.(proto.Message)
	protoMsg.Reset()

	if pu, ok := protoMsg.(proto.Unmarshaler); ok {
		// unmarshal itself, without buffer
		return pu.Unmarshal(data)
	}

	buffer := bufferPool.Get().(*cachedBuffer)
	buffer.SetBuf(data)
	err := buffer.Unmarshal(protoMsg)
	buffer.SetBuf(nil)
	bufferPool.Put(buffer)
	return err
}

func upperLimit(val int) uint32 {
	if val > math.MaxInt32 {
		return uint32(math.MaxInt32)
	}
	return uint32(val)
}

var bufferPool = &sync.Pool{
	New : func() interface {} {
		return &cachedBuffer {
			Buffer : proto.Buffer{},
			lastMarshaledSize : 16,
		}
	},
}

type cachedBuffer struct {
	proto.Buffer
	lastMarshaledSize uint32
}