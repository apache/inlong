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

package org.apache.inlong.common.enums;

/**
 * Indicator type of inlong audit
 */
public enum IndicatorType {

    SEND_SUCCESS(1, "SEND_SUCCESS", "Message send success"),
    RECEIVED_SUCCESS(2, "RECEIVED_SUCCESS", "Message received success"),
    SEND_FAILED(3, "SEND_FAILED", "Message send failed"),
    RECEIVED_FAILED(4, "RECEIVED_FAILED", "Message received failed"),
    SEND_RETRY(5, "SEND_RETRY", "Message send retry"),
    RECEIVED_RETRY(6, "RECEIVED_RETRY", "Message received retry"),
    SEND_DISCARD(7, "SEND_DISCARD", "Message send discard"),
    RECEIVED_DISCARD(8, "RECEIVED_DISCARD", "Message received discard"),

    UNKNOWN_TYPE(Integer.MAX_VALUE, "UNKNOWN_TYPE", "Unknown type");

    private final int code;
    private final String name;
    private final String desc;

    IndicatorType(int code, String name, String desc) {
        this.code = code;
        this.name = name;
        this.desc = desc;
    }

    public static IndicatorType valueOf(int value) {
        for (IndicatorType code : IndicatorType.values()) {
            if (code.getCode() == value) {
                return code;
            }
        }

        return UNKNOWN_TYPE;
    }

    public int getCode() {
        return code;
    }

    public String getName() {
        return name;
    }

    public String getDesc() {
        return desc;
    }

}
