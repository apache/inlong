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

// package errs defines the TubeMQ error codes and TubeMQ error msg.
package errs

import (
	"fmt"
)

const (
	// RetMarshalFailure represents the error code of marshal error.
	RetMarshalFailure = 1
	// RetResponseException represents the error code of response exception.
	RetResponseException = 2
	// RetUnMarshalFailure represents the error code of unmarshal error.
	RetUnMarshalFailure = 3
	// RetAssertionFailure represents the error code of assertion error.
	RetAssertionFailure = 4
	// RetRequestFailure represents the error code of request error.
	RetRequestFailure = 5
)

// ErrAssertionFailure represents RetAssertionFailure error.
var ErrAssertionFailure = New(RetAssertionFailure, "AssertionFailure")

// Error provides a TubeMQ-specific error container
type Error struct {
	Code int32
	Msg  string
}

// Error() implements the Error interface.
func (e *Error) Error() string {
	return fmt.Sprintf("code: %d, msg:%s", e.Code, e.Msg)
}

// New returns a self-defined error.
func New(code int32, msg string) error {
	err := &Error{
		Code: code,
		Msg:  msg,
	}
	return err
}
