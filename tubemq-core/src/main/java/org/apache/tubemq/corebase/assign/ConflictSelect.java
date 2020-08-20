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

public enum ConflictSelect {
    CS_LEFT_OR_RIGHT_BIGGER(0, "CS_LEFT_OR_RIGHT_BIGGER",
            "Select left-interval bigger value tuple or right-interval bigger value tuple if set conflict."),
    CS_LEFT_OR_RIGHT_SMALLER(1, "CS_LEFT_OR_RIGHT_SMALLER",
            "Select left-interval smaller value tuple or right-interval smaller value tuple if set conflict."),
    CS_RIGHT_OR_LEFT_BIGGER(2, "CS_RIGHT_OR_LEFT_BIGGER",
            "Select right-interval bigger value tuple or left-interval bigger value tuple if set conflict."),
    CS_RIGHT_OR_LEFT_SMALLER(3, "CS_RIGHT_OR_LEFT_SMALLER",
            "Select right-interval smaller value tuple or left-interval smaller value tuple if set conflict."),
    CS_LEFT_AND_RIGHT_BIGGER(4, "CS_LEFT_AND_RIGHT_BIGGER",
            "Mixed tuple from left and right interval bigger value if set conflict."),
    CS_LEFT_AND_RIGHT_SMALLER(5, "CS_LEFT_AND_RIGHT_SMALLER",
            "Mixed tuple from left and right interval smaller value if set conflict."),
    CS_LEFT_BIGGER_AND_RIGHT_SMALLER(6, "CS_LEFT_BIGGER_AND_RIGHT_SMALLER",
            "Mixed tuple from left-interval bigger and right-interval smaller value if set conflict."),
    CS_LEFT_SMALLER_AND_RIGHT_BIGGER(7, "CS_LEFT_SMALLER_AND_RIGHT_BIGGER",
            "Mixed tuple from left-interval smaller and right-interval bigger value if set conflict.");

    private int code;
    private String token;
    private String description;

    ConflictSelect(int code, String token, String description) {
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

    public static ConflictSelect valueOf(int code) {
        for (ConflictSelect confSelect : ConflictSelect.values()) {
            if (confSelect.getCode() == code) {
                return confSelect;
            }
        }
        throw new IllegalArgumentException(String.format("unknown ConflictSelect code %s", code));
    }
}
