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
package org.apache.tubemq.corebase.assign;

public enum AssignType {
    ASSIGN_UNDEFINED(0, "ASSIGN_UNDEFINED",
            "Non-specified consumption."),
    ASSIGN_ONLY_SET_VALUE(1, "ASSIGN_ONLY_SET_VALUE",
            "Only set consumption offset range of the specified partitions."),
    ASSIGN_LIMIT_PARTITION_SCOPE(2, "ASSIGN_LIMIT_PARTITION_SCOPE",
            "Only consume within the specified partitions and offset range.");

    private int code;
    private String token;
    private String description;

    AssignType(int code, String token, String description) {
        this.code = code;
        this.token = token;
        this.description = description;
    }

    public int getCode() {
        return code;
    }

    public String getToken() {
        return token;
    }

    public String getDescription() {
        return description;
    }

    public static AssignType valueOf(int code) {
        for (AssignType assignType : AssignType.values()) {
            if (assignType.getCode() == code) {
                return assignType;
            }
        }
        throw new IllegalArgumentException(String.format("unknown AssignType code %s", code));
    }
}
