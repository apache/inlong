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

package org.apache.inlong.manager.common.enums;

/**
 * Stream status enum
 */
public enum StreamStatus {

    NEW(100, "new"),
    CONFIG_ING(110, "in configure"),
    CONFIG_FAILED(120, "configuration failed"),
    CONFIG_SUCCESSFUL(130, "configuration successful"),

    CONFIG_OFFLINE_ING(141, "configuration is going offline"),
    CONFIG_OFFLINE_SUCCESSFUL(140, "configuration offline successful"),

    CONFIG_ONLINE_ING(151, "configuration is going online"),

    DELETING(41, "deleting"),
    DELETED(40, "deleted");

    private final Integer code;
    private final String description;

    StreamStatus(Integer code, String description) {
        this.code = code;
        this.description = description;
    }

    /**
     * Checks whether the given status allows updating operate.
     */
    public static boolean notAllowedUpdate(StreamStatus status) {
        return status == StreamStatus.CONFIG_ING || status == StreamStatus.CONFIG_OFFLINE_ING
                || status == StreamStatus.CONFIG_ONLINE_ING || status == StreamStatus.DELETING;
    }

    /**
     * Checks whether the given status allows deleting operate.
     */
    public static boolean notAllowedDelete(StreamStatus status) {
        return status == StreamStatus.CONFIG_ING
                || status == StreamStatus.CONFIG_ONLINE_ING
                || status == StreamStatus.CONFIG_OFFLINE_ING;
    }

    public static StreamStatus forCode(int code) {
        for (StreamStatus status : values()) {
            if (status.getCode() == code) {
                return status;
            }
        }
        throw new IllegalStateException(String.format("Illegal code=%s for StreamStatus", code));
    }

    public Integer getCode() {
        return code;
    }

    public String getDescription() {
        return description;
    }

}