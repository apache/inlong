// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements.  See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to You under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License.  You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package util

import (
	"math"
	"math/rand"
	"time"
)

// ExponentialBackoff implements an exponential backoff strategy
type ExponentialBackoff struct {
	InitialInterval time.Duration
	MaxInterval     time.Duration
	Multiplier      float64
	Randomization   float64
}

// Next calculates the next interval with exponential backoff
func (b *ExponentialBackoff) Next(retryCount int) time.Duration {
	if retryCount <= 0 {
		return b.InitialInterval
	}

	interval := float64(b.InitialInterval) * math.Pow(b.Multiplier, float64(retryCount-1))
	if b.Randomization > 0 {
		interval = interval * (1 + b.Randomization*(rand.Float64()*2-1))
	}

	if interval > float64(b.MaxInterval) {
		interval = float64(b.MaxInterval)
	}

	return time.Duration(interval)
}
