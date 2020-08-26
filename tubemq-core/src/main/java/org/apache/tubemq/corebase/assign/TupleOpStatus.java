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

public enum TupleOpStatus {
    TUPLE_STATUS_UNDEFINED(0, "TUPLE_STATUS_UNDEFINED",
            "Undefined status."),
    TUPLE_STATUS_ACCEPTED(1, "TUPLE_STATUS_ACCEPTED",
            "Range-tuple value is accepted."),
    TUPLE_STATUS_ASSIGNED(2, "TUPLE_STATUS_ASSIGNED",
            "Range-tuple value is assigned."),
    TUPLE_STATUS_DISPATCHED(3, "TUPLE_STATUS_DISPATCHED",
            "Range-tuple value is dispatched."),
    TUPLE_STATUS_REACHED(4, "TUPLE_STATUS_REACHED",
            "Range-tuple value is reached."),
    TUPLE_STATUS_SET_LEFT(5, "TUPLE_STATUS_SET_LEFT",
            "Range-tuple left value is set."),
    TUPLE_STATUS_SET_RIGHT(6, "TUPLE_STATUS_SET_RIGHT",
            "Range-tuple right value is set."),
    TUPLE_STATUS_FINISHED(7, "TUPLE_STATUS_FINISHED",
            "Range-tuple value set finished.");

    private int code;
    private String token;
    private String description;

    TupleOpStatus(int code, String token, String description) {
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

    public static TupleOpStatus valueOf(int code) {
        for (TupleOpStatus opStatus : TupleOpStatus.values()) {
            if (opStatus.getCode() == code) {
                return opStatus;
            }
        }
        throw new IllegalArgumentException(
                String.format("unknown TupleOpStatus code %s", code));
    }
}
