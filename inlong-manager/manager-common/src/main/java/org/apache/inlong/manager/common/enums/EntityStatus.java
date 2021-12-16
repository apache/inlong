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
import java.util.Set;

/**
 * Entity status enum
 */
public enum EntityStatus {

    UN_DELETED(0, "not deleted"),
    IS_DELETED(1, "already deleted"),

    DRAFT(0, "draft"),

    NORMAL(10, "normal"),
    FAILURE(20, "failure"),
    CANCELED(30, "canceled"),
    DELETED(40, "deleted"),

    // Business related status
    BIZ_WAIT_SUBMIT(100, "waiting for submit"),
    BIZ_WAIT_APPROVAL(101, "waiting for approval"),
    BIZ_APPROVE_REJECTED(102, "approval rejected"),
    BIZ_APPROVE_PASSED(103, "approval passed"),
    BIZ_CONFIG_ING(110, "in configure"),
    BIZ_CONFIG_FAILED(120, "configuration failed"),
    BIZ_CONFIG_SUCCESSFUL(130, "configuration successful"),

    // Data stream related status
    DATA_STREAM_NEW(100, "new"),
    DATA_STREAM_CONFIG_ING(110, "in configure"),
    DATA_STREAM_CONFIG_FAILED(120, "configuration failed"),
    DATA_STREAM_CONFIG_SUCCESSFUL(130, "configuration successful"),

    // Data storage related status
    DATA_STORAGE_NEW(100, "new"),
    DATA_STORAGE_CONFIG_ING(110, "in configure"),
    DATA_STORAGE_CONFIG_FAILED(120, "configuration failed"),
    DATA_STORAGE_CONFIG_SUCCESSFUL(130, "configuration successful"),

    // Data source (or Agent) related status
    AGENT_DISABLE(99, "disable"),
    AGENT_NORMAL(101, "normal"),
    AGENT_FREEZE(102, "stopped"),
    AGENT_FAILURE(103, "failed"),

    // ADD(0), DEL(1), RETRY(2), BACKTRACK(3), FROZEN(4), ACTIVE(5), CHECK(6), REDOMETRIC(7), MAKEUP(8);
    AGENT_ADD(200, "wait add"),
    AGENT_DELETE(201, "wait delete"),

    ;

    /**
     * The status of the business that can initiate the approval process:
     * <p/>[BIZ_WAIT_SUBMIT] [BIZ_APPROVE_REJECTED] [BIZ_CONFIG_FAILED] [BIZ_CONFIG_SUCCESSFUL]
     */
    public static final Set<Integer> ALLOW_START_WORKFLOW_STATUS = ImmutableSet.of(
            BIZ_WAIT_SUBMIT.getCode(), BIZ_APPROVE_REJECTED.getCode(), BIZ_CONFIG_FAILED.getCode(),
            BIZ_CONFIG_SUCCESSFUL.getCode());

    /**
     * The status of the business that can be modified:
     * <p/>[DRAFT] [BIZ_WAIT_SUBMIT] [BIZ_APPROVE_REJECTED] [BIZ_CONFIG_FAILED] [BIZ_CONFIG_SUCCESSFUL]
     * <p/>[BIZ_CONFIG_ING] status cannot be modified
     */
    public static final Set<Integer> ALLOW_UPDATE_BIZ_STATUS = ImmutableSet.of(
            DRAFT.getCode(), BIZ_WAIT_SUBMIT.getCode(), BIZ_APPROVE_REJECTED.getCode(),
            BIZ_CONFIG_FAILED.getCode(), BIZ_CONFIG_SUCCESSFUL.getCode());

    /**
     * The status of the service that can be deleted - all status
     * <p/>[DRAFT] [BIZ_WAIT_SUBMIT] [BIZ_APPROVE_REJECTED] [BIZ_CONFIG_ING] [BIZ_CONFIG_FAILED] [BIZ_CONFIG_SUCCESSFUL]
     * <p/>[BIZ_WAIT_APPROVAL] [BIZ_APPROVE_PASSED] status cannot be deleted
     */
    public static final Set<Integer> ALLOW_DELETE_BIZ_STATUS = ImmutableSet.of(
            DRAFT.getCode(), BIZ_WAIT_SUBMIT.getCode(), BIZ_APPROVE_REJECTED.getCode(),
            BIZ_CONFIG_ING.getCode(), BIZ_CONFIG_FAILED.getCode(), BIZ_CONFIG_SUCCESSFUL.getCode());

    /**
     * The business can cascade to delete the status of the associated data:
     */
    public static final Set<Integer> ALLOW_DELETE_BIZ_CASCADE_STATUS = ImmutableSet.of(
            DRAFT.getCode(), BIZ_WAIT_SUBMIT.getCode());

    /**
     * Status of business approval
     */
    public static final Set<Integer> BIZ_APPROVE_PASS_STATUS = ImmutableSet.of(
            BIZ_CONFIG_FAILED.getCode(), BIZ_CONFIG_SUCCESSFUL.getCode());

    /**
     * Temporary business status, adding, deleting and modifying operations are not allowed
     */
    public static final ImmutableSet<Integer> BIZ_TEMP_STATUS = ImmutableSet.of(
            BIZ_WAIT_APPROVAL.getCode(), BIZ_CONFIG_ING.getCode());

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
