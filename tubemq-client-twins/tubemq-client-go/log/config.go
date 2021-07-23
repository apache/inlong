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

package log

// OutputConfig defines the output config which can be reconfigured by user.
type OutputConfig struct {
	// LogPath is the path for the log.
	LogPath string
	// Level is the log level.
	Level string
	// MaxSize is the max size for a rolling log.
	MaxSize int
	// MaxBackups is the maximum number of old log files to retain.
	MaxBackups int
	// MaxAge is the maximum number of days to retain old log files based on the
	// timestamp encoded in their filename.
	MaxAge int
}

var defaultConfig = &OutputConfig{
	LogPath:    "../log/tubemq.log",
	MaxSize:    100,
	MaxBackups: 5,
	MaxAge:     3,
	Level:      "error",
}
