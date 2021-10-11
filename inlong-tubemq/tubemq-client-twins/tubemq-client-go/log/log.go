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

// Package log defines the logger for the sdk.
package log

var traceEnabled = false

// EnableTrace enables trace level log.
func EnableTrace() {
	traceEnabled = true
}

var defaultLogger = newDefaultLogger()

func newDefaultLogger() Logger {
	return newZapLog(defaultConfig)
}

// NewLogger will return a configured zap logger.
func NewLogger(config *OutputConfig) Logger {
	defaultLogger = newZapLog(config)
	return defaultLogger
}

// SetLogger sets the default logger to the given logger.
func SetLogger(logger Logger) {
	defaultLogger = logger
}

// WithFields is the proxy to call the WithFields of defaultLogger.
func WithFields(fields ...string) Logger {
	return defaultLogger.WithFields(fields...)
}

// Trace logs to TRACE level log. Arguments are handled in the manner of fmt.Print.
func Trace(args ...interface{}) {
	if traceEnabled {
		defaultLogger.Trace(args...)
	}
}

// Tracef logs to TRACE level log. Arguments are handled in the manner of fmt.Printf.
func Tracef(format string, args ...interface{}) {
	if traceEnabled {
		defaultLogger.Tracef(format, args...)
	}
}

// Debug logs to DEBUG level log. Arguments are handled in the manner of fmt.Print.
func Debug(args ...interface{}) {
	defaultLogger.Debug(args...)
}

// Debugf logs to DEBUG level log. Arguments are handled in the manner of fmt.Printf.
func Debugf(format string, args ...interface{}) {
	defaultLogger.Debugf(format, args...)
}

// Info logs to INFO level log. Arguments are handled in the manner of fmt.Print.
func Info(args ...interface{}) {
	defaultLogger.Info(args...)
}

// Infof logs to INFO level log. Arguments are handled in the manner of fmt.Print.
func Infof(format string, args ...interface{}) {
	defaultLogger.Infof(format, args...)
}

// Warn logs to WARNING level log. Arguments are handled in the manner of fmt.Print.
func Warn(args ...interface{}) {
	defaultLogger.Warn(args...)
}

// Warnf logs to WARNING level log. Arguments are handled in the manner of fmt.Printf.
func Warnf(format string, args ...interface{}) {
	defaultLogger.Warnf(format, args...)
}

// Error logs to ERROR level log. Arguments are handled in the manner of fmt.Print.
func Error(args ...interface{}) {
	defaultLogger.Error(args...)
}

// Errorf logs to ERROR level log. Arguments are handled in the manner of fmt.Printf.
func Errorf(format string, args ...interface{}) {
	defaultLogger.Errorf(format, args...)
}

// Fatal logs to ERROR level log. Arguments are handled in the manner of fmt.Print.
// that all Fatal logs will exit with os.Exit(1).
func Fatal(args ...interface{}) {
	defaultLogger.Fatal(args...)
}

// Fatalf logs to ERROR level log. Arguments are handled in the manner of fmt.Printf.
func Fatalf(format string, args ...interface{}) {
	defaultLogger.Fatalf(format, args...)
}
