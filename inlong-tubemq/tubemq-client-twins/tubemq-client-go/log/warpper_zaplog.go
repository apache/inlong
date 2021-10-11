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
	"fmt"
	"time"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"gopkg.in/natefinch/lumberjack.v2"
)

type zapLog struct {
	logger *zap.Logger
}

var levelToZapLevel = map[Level]zapcore.Level{
	LevelTrace: zapcore.DebugLevel,
	LevelDebug: zapcore.DebugLevel,
	LevelInfo:  zapcore.InfoLevel,
	LevelWarn:  zapcore.WarnLevel,
	LevelError: zapcore.ErrorLevel,
	LevelFatal: zapcore.FatalLevel,
}

func newZapLog(c *OutputConfig) Logger {
	core := newFileCore(c)
	logger := zap.New(
		zapcore.NewTee(core),
		zap.AddCallerSkip(2),
		zap.AddCaller(),
	)
	return &zapLog{logger: logger}
}

func newFileCore(c *OutputConfig) zapcore.Core {
	w := zapcore.AddSync(&lumberjack.Logger{
		Filename:   c.LogPath,
		MaxSize:    c.MaxSize, // megabytes
		MaxBackups: c.MaxBackups,
		MaxAge:     c.MaxAge, // days
	})

	core := zapcore.NewCore(
		newDefaultEncoder(),
		w,
		levelToZapLevel[LevelNames[c.Level]],
	)
	return core
}

func newDefaultEncoder() zapcore.Encoder {
	encoderCfg := zapcore.EncoderConfig{
		TimeKey:        "T",
		LevelKey:       "L",
		NameKey:        "N",
		CallerKey:      "C",
		MessageKey:     "M",
		StacktraceKey:  "S",
		LineEnding:     zapcore.DefaultLineEnding,
		EncodeLevel:    zapcore.CapitalLevelEncoder,
		EncodeTime:     newDefaultTimeEncoder(),
		EncodeDuration: zapcore.StringDurationEncoder,
		EncodeCaller:   zapcore.ShortCallerEncoder,
	}
	encoder := zapcore.NewConsoleEncoder(encoderCfg)
	return encoder
}

func newDefaultTimeEncoder() zapcore.TimeEncoder {
	return func(t time.Time, enc zapcore.PrimitiveArrayEncoder) {
		enc.AppendByteString(defaultTimeFormat(t))
	}
}

func defaultTimeFormat(t time.Time) []byte {
	t = t.Local()
	year, month, day := t.Date()
	hour, minute, second := t.Clock()
	micros := t.Nanosecond() / 1000

	buf := make([]byte, 23)
	buf[0] = byte((year/1000)%10) + '0'
	buf[1] = byte((year/100)%10) + '0'
	buf[2] = byte((year/10)%10) + '0'
	buf[3] = byte(year%10) + '0'
	buf[4] = '-'
	buf[5] = byte((month)/10) + '0'
	buf[6] = byte((month)%10) + '0'
	buf[7] = '-'
	buf[8] = byte((day)/10) + '0'
	buf[9] = byte((day)%10) + '0'
	buf[10] = ' '
	buf[11] = byte((hour)/10) + '0'
	buf[12] = byte((hour)%10) + '0'
	buf[13] = ':'
	buf[14] = byte((minute)/10) + '0'
	buf[15] = byte((minute)%10) + '0'
	buf[16] = ':'
	buf[17] = byte((second)/10) + '0'
	buf[18] = byte((second)%10) + '0'
	buf[19] = '.'
	buf[20] = byte((micros/100000)%10) + '0'
	buf[21] = byte((micros/10000)%10) + '0'
	buf[22] = byte((micros/1000)%10) + '0'
	return buf
}

// ZapLogWrapper implements Logger interface
// based on the underlying zapLog.
type ZapLogWrapper struct {
	l *zapLog
}

// GetLogger returns the wrapped zap looger.
func (z *ZapLogWrapper) GetLogger() Logger {
	return z.l
}

// Trace logs to TRACE log, Arguments are handled in the manner of fmt.Print
func (z *ZapLogWrapper) Trace(args ...interface{}) {
	z.l.Trace(args...)
}

// Tracef logs to TRACE log, Arguments are handled in the manner of fmt.Printf
func (z *ZapLogWrapper) Tracef(format string, args ...interface{}) {
	z.l.Tracef(format, args...)
}

// Debug logs to DEBUG log, Arguments are handled in the manner of fmt.Print
func (z *ZapLogWrapper) Debug(args ...interface{}) {
	z.l.Debug(args...)
}

// Debugf logs to DEBUG log, Arguments are handled in the manner of fmt.Printf
func (z *ZapLogWrapper) Debugf(format string, args ...interface{}) {
	z.l.Debugf(format, args...)
}

// Info logs to INFO log, Arguments are handled in the manner of fmt.Print
func (z *ZapLogWrapper) Info(args ...interface{}) {
	z.l.Info(args...)
}

// Infof logs to INFO log, Arguments are handled in the manner of fmt.Printf
func (z *ZapLogWrapper) Infof(format string, args ...interface{}) {
	z.l.Infof(format, args...)
}

// Warn logs to WARNING log, Arguments are handled in the manner of fmt.Print
func (z *ZapLogWrapper) Warn(args ...interface{}) {
	z.l.Warn(args...)
}

// Warnf logs to WARNING log, Arguments are handled in the manner of fmt.Printf
func (z *ZapLogWrapper) Warnf(format string, args ...interface{}) {
	z.l.Warnf(format, args...)
}

// Error logs to ERROR log, Arguments are handled in the manner of fmt.Print
func (z *ZapLogWrapper) Error(args ...interface{}) {
	z.l.Error(args...)
}

// Errorf logs to ERROR log, Arguments are handled in the manner of fmt.Printf
func (z *ZapLogWrapper) Errorf(format string, args ...interface{}) {
	z.l.Errorf(format, args...)
}

// Fatal logs to FATAL log, Arguments are handled in the manner of fmt.Print
func (z *ZapLogWrapper) Fatal(args ...interface{}) {
	z.l.Fatal(args...)
}

// Fatalf logs to FATAL log, Arguments are handled in the manner of fmt.Printf
func (z *ZapLogWrapper) Fatalf(format string, args ...interface{}) {
	z.l.Fatalf(format, args...)
}

// Sync calls the zap defaultLogger's Sync method, flushing any buffered log entries.
// Applications should take care to call Sync before exiting.
func (z *ZapLogWrapper) Sync() error {
	return z.l.Sync()
}

// WithFields sets the user-defined data to the log and return a new Logger.
func (z *ZapLogWrapper) WithFields(fields ...string) Logger {
	return z.l.WithFields(fields...)
}

// WithFields sets the user-defined data to the log and return a new Logger.
func (l *zapLog) WithFields(fields ...string) Logger {
	zapfields := make([]zap.Field, len(fields)/2)
	for index := range zapfields {
		zapfields[index] = zap.String(fields[2*index], fields[2*index+1])
	}

	return &ZapLogWrapper{l: &zapLog{logger: l.logger.With(zapfields...)}}
}

// Trace logs to TRACE log, Arguments are handled in the manner of fmt.Print.
func (l *zapLog) Trace(args ...interface{}) {
	if l.logger.Core().Enabled(zapcore.DebugLevel) {
		l.logger.Debug(fmt.Sprint(args...))
	}
}

// Tracef logs to TRACE log, Arguments are handled in the manner of fmt.Printf.
func (l *zapLog) Tracef(format string, args ...interface{}) {
	if l.logger.Core().Enabled(zapcore.DebugLevel) {
		l.logger.Debug(fmt.Sprintf(format, args...))
	}
}

// Info logs to Debug log, Arguments are handled in the manner of fmt.Print
func (l *zapLog) Debug(args ...interface{}) {
	if l.logger.Core().Enabled(zapcore.DebugLevel) {
		l.logger.Debug(fmt.Sprint(args...))
	}
}

// Info logs to Debug log, Arguments are handled in the manner of fmt.Printf
func (l *zapLog) Debugf(format string, args ...interface{}) {
	if l.logger.Core().Enabled(zapcore.DebugLevel) {
		l.logger.Debug(fmt.Sprintf(format, args...))
	}
}

// Info logs to INFO log, Arguments are handled in the manner of fmt.Print
func (l *zapLog) Info(args ...interface{}) {
	if l.logger.Core().Enabled(zapcore.InfoLevel) {
		l.logger.Info(fmt.Sprint(args...))
	}
}

// Infof logs to INFO log, Arguments are handled in the manner of fmt.Printf
func (l *zapLog) Infof(format string, args ...interface{}) {
	if l.logger.Core().Enabled(zapcore.InfoLevel) {
		l.logger.Info(fmt.Sprintf(format, args...))
	}
}

// Warn logs to WARNING log, Arguments are handled in the manner of fmt.Print
func (l *zapLog) Warn(args ...interface{}) {
	if l.logger.Core().Enabled(zapcore.WarnLevel) {
		l.logger.Warn(fmt.Sprint(args...))
	}
}

// Warnf logs to WARNING log, Arguments are handled in the manner of fmt.Printf
func (l *zapLog) Warnf(format string, args ...interface{}) {
	if l.logger.Core().Enabled(zapcore.WarnLevel) {
		l.logger.Warn(fmt.Sprintf(format, args...))
	}
}

// Error logs to ERROR log, Arguments are handled in the manner of fmt.Print
func (l *zapLog) Error(args ...interface{}) {
	if l.logger.Core().Enabled(zapcore.ErrorLevel) {
		l.logger.Error(fmt.Sprint(args...))
	}
}

// Errorf logs to ERROR log, Arguments are handled in the manner of fmt.Printf
func (l *zapLog) Errorf(format string, args ...interface{}) {
	if l.logger.Core().Enabled(zapcore.ErrorLevel) {
		l.logger.Error(fmt.Sprintf(format, args...))
	}
}

// Fatal logs to FATAL log, Arguments are handled in the manner of fmt.Print
func (l *zapLog) Fatal(args ...interface{}) {
	if l.logger.Core().Enabled(zapcore.FatalLevel) {
		l.logger.Fatal(fmt.Sprint(args...))
	}
}

// Fatalf logs to FATAL log, Arguments are handled in the manner of fmt.Printf
func (l *zapLog) Fatalf(format string, args ...interface{}) {
	if l.logger.Core().Enabled(zapcore.FatalLevel) {
		l.logger.Fatal(fmt.Sprintf(format, args...))
	}
}

// Sync calls the zap defaultLogger's Sync method, flushing any buffered log entries.
// Applications should take care to call Sync before exiting.
func (l *zapLog) Sync() error {
	return l.logger.Sync()
}
