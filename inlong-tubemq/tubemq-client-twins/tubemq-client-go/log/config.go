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
	"errors"
	"os"
)

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
	Level:      "warn",
}

// SetLogLevel set log level
func SetLogLevel(level string) error {
	if _, exist := LevelNames[level]; !exist {
		return errors.New("the supported log levels are: trace, debug, info, warn, error, fatal. Please check your log level")
	}
	defaultConfig.Level = level
	return nil
}

// SetLogPath set log path
func SetLogPath(path string) error {
	err := verifyLogPath(path)
	if err != nil {
		return err
	}
	defaultConfig.LogPath = path
	return nil
}

func verifyLogPath(path string) error {
	// Check if file already exists
	if _, err := os.Stat(path); err == nil {
		return nil
	}

	// Attempt to create it
	var d []byte
	err := os.WriteFile(path, d, 0644)
	if err == nil {
		removeErr := os.Remove(path)
		if removeErr != nil {
			return removeErr
		} // And delete it
		return nil
	}
	return err
}
