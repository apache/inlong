//
// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements.  See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to You under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License.  You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package util

import (
	"log"
	"sync"

	"github.com/bwmarrin/snowflake"
	"github.com/google/uuid"
	"github.com/zentures/cityhash"
)

var (
	snowflakeOnce sync.Once
	snowflakeNode *snowflake.Node
	snowflakeErr  error
)

func newSnowFlakeNode() (*snowflake.Node, error) {
	ip, err := GetOneIP()
	if err != nil {
		return nil, err
	}
	id := IPtoUInt(ip)
	node, err := snowflake.NewNode(int64(id % 1024))
	if err != nil {
		return nil, err
	}
	return node, nil
}

// UInt64UUID generates an uint64 UUID
func UInt64UUID() (uint64, error) {
	guid, err := uuid.NewRandom()
	if err != nil {
		return 0, err
	}

	bytes := guid[:]
	length := len(bytes)
	return cityhash.CityHash64WithSeeds(bytes, uint32(length), 13329145742295551469, 7926974186468552394), nil
}

// SnowFlakeID generates a snowflake ID. If an error occurs, it logs it and exits.
// Deprecated: Use SafeSnowFlakeID instead.
func SnowFlakeID() string {
	id, err := SafeSnowFlakeID()
	if err != nil {
		log.Fatal(err)
	}
	return id
}

// SafeSnowFlakeID generates a snowflake ID. If an error occurs it returns it.
func SafeSnowFlakeID() (string, error) {
	snowflakeOnce.Do(func() {
		snowflakeNode, snowflakeErr = newSnowFlakeNode()
	})
	if snowflakeErr != nil {
		return "", snowflakeErr
	}
	return snowflakeNode.Generate().String(), nil
}
