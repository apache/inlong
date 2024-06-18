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

import lombok.Getter;

/**
 * Status for schedule info.
 * This is the transient status of the schedule info.
 * With specified operations, the status will change to corresponding value.
 *  Status                Operations
 *  NEW                   inlong group created with schedule info
 *  APPROVED              the new inlong group approved by admin
 *  REGISTERED            schedule info registered to schedule engine
 *  UPDATED               update schedule info for a group
 *  DELETED               delete a group
 * */
@Getter
public enum ScheduleStatus {

    NEW(100, "new"),
    APPROVED(101, "approved"),
    REGISTERED(102, "registered"),
    UPDATED(103, "updated"),
    DELETED(99, "deleted");

    private final Integer code;
    private final String description;

    ScheduleStatus(Integer code, String description) {
        this.code = code;
        this.description = description;
    }

    public static ScheduleStatus forCode(int code) {
        for (ScheduleStatus status : values()) {
            if (status.getCode() == code) {
                return status;
            }
        }
        throw new IllegalStateException(String.format("Illegal code=%s for ScheduleStatus", code));
    }

}
