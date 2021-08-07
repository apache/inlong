// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package log

import (
	"bufio"
	"io"
	"os"
	"regexp"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestDefaultLog(t *testing.T) {
	config := defaultConfig
	Errorf("HelloWorld")
	f, err := os.Open(config.LogPath)
	defer f.Close()
	assert.Nil(t, err)
	r := bufio.NewReader(f)
	lines := 0
	for {
		content, _, err := r.ReadLine()
		if err == io.EOF {
			break
		}
		assert.Nil(t, err)
		words := strings.Fields(string(content))
		assert.Equal(t, len(words), 5)
		assert.Regexp(t, regexp.MustCompile(`[\d]{4}-[\d]{2}-[\d]{2}`), words[0])
		assert.Regexp(t, regexp.MustCompile(`[\d]{2}:[\d]{2}:[\d]{2}`), words[1])
		assert.Equal(t, "ERROR", words[2])
		assert.Regexp(t, regexp.MustCompile(`[\w]+/[\w]+.go:[\d]+`), words[3])
		assert.Equal(t, "HelloWorld", words[4])
		lines++
	}
	assert.Equal(t, 1, lines)
	err = os.Remove(config.LogPath)
	assert.Nil(t, err)
}
