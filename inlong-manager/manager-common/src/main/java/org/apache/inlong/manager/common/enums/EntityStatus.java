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

import com.google.common.collect.ImmutableSet;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import java.util.Map;
import java.util.Set;

/**
 * Entity status enum
 */
@Deprecated
public enum EntityStatus {

    UN_DELETED(0, "not deleted"),
    IS_DELETED(1, "already deleted"),

    DRAFT(0, "draft"),

    NORMAL(10, "normal"),
    FAILURE(20, "failure"),
    CANCELED(30, "canceled"),
    DELETED(40, "deleted"),

    // Inlong group related status
    GROUP_WAIT_SUBMIT(100, "waiting for submit"),
    GROUP_WAIT_APPROVAL(101, "waiting for approval"),
    GROUP_APPROVE_REJECTED(102, "approval rejected"),
    GROUP_APPROVE_PASSED(103, "approval passed"),
    GROUP_CONFIG_ING(110, "in configure"),
    GROUP_CONFIG_FAILED(120, "configuration failed"),
    GROUP_CONFIG_SUCCESSFUL(130, "configuration successful"),
    GROUP_SUSPEND(140, "suspend"),
    GROUP_RESTART(150, "restart"),

    // Inlong stream related status
    STREAM_NEW(100, "new"),
    STREAM_CONFIG_ING(110, "in configure"),
    STREAM_CONFIG_FAILED(120, "configuration failed"),
    STREAM_CONFIG_SUCCESSFUL(130, "configuration successful"),

    // Stream source related status
    SOURCE_NEW(100, "new"),
    SOURCE_CONFIG_ING(110, "in configure"),
    SOURCE_CONFIG_FAILED(120, "configuration failed"),
    SOURCE_CONFIG_SUCCESSFUL(130, "configuration successful"),

    // Stream sink related status
    SINK_NEW(100, "new"),
    SINK_CONFIG_ING(110, "in configure"),
    SINK_CONFIG_FAILED(120, "configuration failed"),
    SINK_CONFIG_SUCCESSFUL(130, "configuration successful"),

    // Stream source (or Agent) related status
    AGENT_DISABLE(99, "disable"),
    AGENT_NORMAL(101, "normal"),
    AGENT_FREEZE(102, "stopped"),
    AGENT_FAILURE(103, "failed"),

    // ADD(0), DEL(1), RETRY(2), BACKTRACK(3), FROZEN(4), ACTIVE(5), CHECK(6), REDOMETRIC(7), MAKEUP(8);
    AGENT_ADD(200, "wait add"),
    AGENT_DELETE(201, "wait delete");

    /**
     * The status of the inlong group that can initiate the approval process:
     * <p/>[GROUP_WAIT_SUBMIT] [GROUP_APPROVE_REJECTED] [GROUP_CONFIG_FAILED] [GROUP_CONFIG_SUCCESSFUL]
     */
    public static final Set<Integer> ALLOW_START_WORKFLOW_STATUS = ImmutableSet.of(
            GROUP_WAIT_SUBMIT.getCode(), GROUP_APPROVE_REJECTED.getCode(), GROUP_CONFIG_FAILED.getCode(),
            GROUP_CONFIG_SUCCESSFUL.getCode());

    /**
     * The status of the inlong group that can be modified:
     * <p/>[DRAFT] [GROUP_WAIT_SUBMIT] [GROUP_APPROVE_REJECTED] [GROUP_CONFIG_FAILED]
     * [GROUP_CONFIG_SUCCESSFUL] [GROUP_RESTART] [GROUP_SUSPEND] [GROUP_APPROVE_PASSED]
     *
     * <p/>[GROUP_CONFIG_ING] status cannot be modified
     */
    public static final Set<Integer> ALLOW_UPDATE_GROUP_STATUS = ImmutableSet.of(
            DRAFT.getCode(), GROUP_WAIT_SUBMIT.getCode(), GROUP_APPROVE_REJECTED.getCode(),
            GROUP_CONFIG_FAILED.getCode(), GROUP_CONFIG_SUCCESSFUL.getCode(),
            GROUP_RESTART.getCode(), GROUP_SUSPEND.getCode(), GROUP_APPROVE_PASSED.getCode());

    /**
     * The status of the service that can be deleted - all status
     * <p/>[DRAFT] [GROUP_WAIT_SUBMIT] [GROUP_APPROVE_REJECTED] [GROUP_CONFIG_ING] [GROUP_CONFIG_FAILED]
     * [GROUP_CONFIG_SUCCESSFUL] [GROUP_RESTART] [GROUP_SUSPEND] [GROUP_APPROVE_PASSED]
     *
     * <p/>[GROUP_WAIT_APPROVAL] [GROUP_APPROVE_PASSED] status cannot be deleted
     */
    public static final Set<Integer> ALLOW_DELETE_GROUP_STATUS = ImmutableSet.of(
            DRAFT.getCode(), GROUP_WAIT_SUBMIT.getCode(), GROUP_APPROVE_REJECTED.getCode(),
            GROUP_CONFIG_ING.getCode(), GROUP_CONFIG_FAILED.getCode(), GROUP_CONFIG_SUCCESSFUL.getCode(),
            GROUP_RESTART.getCode(), GROUP_SUSPEND.getCode(), GROUP_APPROVE_PASSED.getCode());

    /**
     * The inlong group can cascade to delete the status of the associated data:
     */
    public static final Set<Integer> ALLOW_DELETE_GROUP_CASCADE_STATUS = ImmutableSet.of(
            DRAFT.getCode(), GROUP_WAIT_SUBMIT.getCode());

    /**
     * Temporary inlong group status, adding, deleting and modifying operations are not allowed
     */
    public static final ImmutableSet<Integer> GROUP_TEMP_STATUS = ImmutableSet.of(
            GROUP_WAIT_APPROVAL.getCode(), GROUP_CONFIG_ING.getCode());

    private final Integer code;
    private final String description;

    EntityStatus(Integer code, String description) {
        this.code = code;
        this.description = description;
    }

    public Integer getCode() {
        return code;
    }

    public String getDescription() {
        return description;
    }

}
