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

public enum RangeType {
    //[-∞，+∞), in other words [last consume offset，last produce position)
    RANGE_SET_UNDEFINED(0, "RANGE_SET_LEFT_DEFINED",
            "Undefined interval, no range value set."),
    //[left offset，+∞), in other words [left offset，last produce position)
    RANGE_SET_LEFT_DEFINED(1, "RANGE_SET_LEFT_DEFINED",
            "Left-closed and set left-value, right-open interval."),
    //[-∞，right offset], in other words [last consume position，right position]
    RANGE_SET_RIGHT_DEFINED(2, "RANGE_SET_RIGHT_DEFINED",
            "Left-closed interval, right-closed and set right-value."),
    //[left position， right position]
    RANGE_SET_BOTH_DEFINED(3, "RANGE_SET_BOTH_DEFINED",
            "Left and right closed interval, set left-value and right-value.");

    private int code;
    private String token;
    private String description;

    RangeType(int code, String token, String description) {
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

    public static RangeType valueOf(int code) {
        for (RangeType rangeType : RangeType.values()) {
            if (rangeType.getCode() == code) {
                return rangeType;
            }
        }
        throw new IllegalArgumentException(String.format("unknown RangeType code %s", code));
    }
}
