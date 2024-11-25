/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.inlong.manager.schedule.exception;

import lombok.Getter;

/**
 * Custom exception for DolphinScheduler operations.
 * Provides error codes, detailed messages, and optional nested exceptions.
 */
@Getter
public class DolphinScheduleException extends RuntimeException {

    // Common error codes
    public static final String UNIQUE_CHECK_FAILED = "UNIQUE_CHECK_FAILED";
    public static final String JSON_PARSE_ERROR = "JSON_PARSE_ERROR";
    public static final String DELETION_FAILED = "DELETION_FAILED";
    public static final String INVALID_HTTP_METHOD = "INVALID_HTTP_METHOD";
    public static final String HTTP_REQUEST_FAILED = "HTTP_REQUEST_FAILED";
    public static final String NETWORK_ERROR = "NETWORK_ERROR";
    public static final String UNEXPECTED_ERROR = "UNEXPECTED_ERROR";

    // Project-related error codes
    public static final String PROJECT_CREATION_FAILED = "PROJECT_CREATION_FAILED";

    // TaskCode-related error codes
    public static final String GEN_TASK_CODE_FAILED = "GEN_TASK_CODE_FAILED";

    // Process-related error codes
    public static final int PROCESS_DEFINITION_IN_USED_ERROR = 10163;
    public static final String PROCESS_DEFINITION_QUERY_FAILED = "PROCESS_DEFINITION_QUERY_FAILED";
    public static final String PROCESS_DEFINITION_CREATION_FAILED = "PROCESS_DEFINITION_CREATION_FAILED";
    public static final String PROCESS_DEFINITION_RELEASE_FAILED = "PROCESS_DEFINITION_RELEASE_FAILED";
    public static final String SCHEDULE_CREATION_FAILED = "SCHEDULE_CREATION_FAILED";
    public static final String SCHEDULE_ONLINE_FAILED = "SCHEDULE_ONLINE_FAILED";
    public static final String UNSUPPORTED_SCHEDULE_TYPE = "UNSUPPORTED_SCHEDULE_TYPE";

    private final String errorCode;
    private final String detailedMessage;

    /**
     * Constructor with message only.
     *
     * @param message The error message.
     */
    public DolphinScheduleException(String message) {
        this(null, message, null);
    }

    /**
     * Constructor with message and cause.
     *
     * @param message The error message.
     * @param cause   The underlying cause of the exception.
     */
    public DolphinScheduleException(String message, Throwable cause) {
        this(null, message, cause);
    }

    /**
     * Constructor with error code, message, and cause.
     *
     * @param errorCode       A specific error code for the exception.
     * @param detailedMessage A detailed error message providing additional context.
     * @param cause           The underlying cause of the exception (optional).
     */
    public DolphinScheduleException(String errorCode, String detailedMessage, Throwable cause) {
        super(detailedMessage, cause);
        this.errorCode = errorCode;
        this.detailedMessage = detailedMessage;
    }

    /**
     * Constructor with error code and message.
     *
     * @param errorCode       A specific error code for the exception.
     * @param detailedMessage A detailed error message providing additional context.
     */
    public DolphinScheduleException(String errorCode, String detailedMessage) {
        this(errorCode, detailedMessage, null);
    }

    @Override
    public String toString() {
        return "DolphinScheduleException{" +
                "errorCode='" + errorCode + '\'' +
                ", detailedMessage='" + detailedMessage + '\'' +
                ", cause=" + getCause() +
                '}';
    }
}
