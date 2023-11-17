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

import java.util.Objects;

/**
 * Enumeration class of encoding format of data output from DataProxy to MQ
 */
public enum MessageWrapType {

    RAW(0, "RAW", "The message body wrapped with nothing"),
    INLONG_MSG_V1(1, "INLONG_MSG_V1", "The message body wrapped with inlong msg v1, which is the PB protocol"),
    INLONG_MSG_V0(2, "INLONG_MSG_V0", "The message body wrapped with inlong msg v0, which is a six segment protocol"),
    UNKNOWN(99, "UNKNOWN", "Unknown message wrap type");

    MessageWrapType(int id, String name, String desc) {
        this.id = id;
        this.name = name;
        this.desc = desc;
    }

    public int getId() {
        return id;
    }

    public String getStrId() {
        return String.valueOf(id);
    }

    public String getName() {
        return name;
    }

    public String getDesc() {
        return desc;
    }

    public static MessageWrapType valueOf(int value) {
        for (MessageWrapType msgEncType : MessageWrapType.values()) {
            if (msgEncType.getId() == value) {
                return msgEncType;
            }
        }
        return UNKNOWN;
    }

    public static MessageWrapType forType(String type) {
        for (MessageWrapType msgEncType : MessageWrapType.values()) {
            if (Objects.equals(msgEncType.getName(), type)) {
                return msgEncType;
            }
        }
        return UNKNOWN;
    }

    private final int id;
    private final String name;
    private final String desc;
}
