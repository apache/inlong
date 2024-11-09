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

/**
 * Represents exceptions specific to the Airflow scheduling process.
 * Each exception is associated with a specific error code for better identification.
 */
public class AirflowScheduleException extends RuntimeException {

    /**
     * Enum to define all error codes associated with Airflow scheduling exceptions.
     */
    public enum AirflowErrorCode {
        INIT_CONNECTION_FAILED,
        TASK_DAG_SWITCH_FAILED,
        SCHEDULE_TASK_REGISTER_FAILED,
        SCHEDULE_TASK_UPDATE_FAILED,
        SCHEDULE_ENGINE_SHUTDOWN_FAILED,
        BUILD_REQUEST_BODY_FAILED,
        DAG_DUPLICATE
    }

    private AirflowErrorCode errorCode;

    public AirflowScheduleException(String message) {
        super(message);
    }
    public AirflowScheduleException(AirflowErrorCode errorCode, String message) {
        super(message);
        this.errorCode = errorCode;
    }

    public AirflowScheduleException(AirflowErrorCode errorCode, String message, Throwable cause) {
        super(message, cause);
        this.errorCode = errorCode;
    }

    public AirflowErrorCode getErrorCode() {
        return errorCode;
    }

    @Override
    public String toString() {
        return String.format("ErrorCode: %s, Message: %s", errorCode, getMessage());
    }
}
