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

import java.util.Arrays;
import java.util.List;

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
    BIZ_WAIT_APPLYING(100, "waiting to applying"),
    BIZ_WAIT_APPROVE(101, "waiting to approve"),
    BIZ_APPROVE_REJECT(102, "approval reject"),
    BIZ_APPROVE_PASS(103, "approval pass"),
    BIZ_CONFIG_ING(110, "configuring"),
    BIZ_CONFIG_FAILURE(120, "failed to config"),
    BIZ_CONFIG_SUCCESS(130, "successfully config"),

    // Data stream related status
    DATA_STREAM_NEW(100, "new"),
    DATA_STREAM_CONFIG_ING(110, "configuring"),
    DATA_STREAM_CONFIG_FAILURE(120, "failed to config"),
    DATA_STREAM_CONFIG_SUCCESS(130, "successfully config"),

    // Data storage related status
    DATA_STORAGE_NEW(100, "new"),
    DATA_STORAGE_CONFIG_ING(110, "configuring"),
    DATA_STORAGE_CONFIG_FAILURE(120, "failed to config"),
    DATA_STORAGE_CONFIG_SUCCESS(130, "successfully config"),

    // Data source related status
    DATA_RESOURCE_NEW(200, "new"),
    DATA_RESOURCE_DELETE(201, "deleted"),

    // Agent related status
    AGENT_WAIT_CREATE(200, "wait create"),
    AGENT_WAIT_STOP(201, "wait stop"),
    AGENT_WAIT_START(203, "wait start"),
    AGENT_WAIT_DELETE(204, "wait delete"),
    AGENT_WAIT_UPDATE(205, "wait update"),

    AGENT_ISSUED_CREATE(300, "created and issued"),
    AGENT_ISSUED_STOP(302, "stop has been issued"),
    AGENT_ISSUED_START(303, "start has been issued"),
    AGENT_ISSUED_DELETE(304, "deletion has been issued"),
    AGENT_ISSUED_UPDATE(305, "modification has been issued"),

    DEPLOY_WAIT(311, "waiting to change deployment"),
    DEPLOY_ING(312, "deploying"),
    DEPLOY_FAILURE(313, "deployment failed"),
    DEPLOY_SUCCESS(314, "deployed successfully"),

    ISSUE_WAIT(321, "waiting to issue"),
    ISSUE_ING(322, "issuing"),
    ISSUE_FAILURE(323, "failed to issue"),
    ISSUE_SUCCESS(324, "successfully issued"),
    ;

    /**
     * The status of the business that can initiate the approval process:
     * <p/>[BIZ_WAIT_APPLYING] [BIZ_APPROVE_REJECT] [BIZ_CONFIG_FAILURE] [BIZ_CONFIG_SUCCESS]
     */
    public static final List<Integer> ALLOW_START_WORKFLOW_STATUS = Arrays.asList(
            BIZ_WAIT_APPLYING.getCode(), BIZ_APPROVE_REJECT.getCode(), BIZ_CONFIG_FAILURE.getCode(),
            BIZ_CONFIG_SUCCESS.getCode());

    /**
     * The status of the business that can be modified:
     * <p/>[DRAFT] [BIZ_WAIT_APPLYING] [BIZ_APPROVE_REJECT] [BIZ_CONFIG_FAILURE] [BIZ_CONFIG_SUCCESS]
     * <p/>[BIZ_CONFIG_ING] status cannot be modified
     */
    public static final List<Integer> ALLOW_UPDATE_BIZ_STATUS = Arrays.asList(
            DRAFT.getCode(), BIZ_WAIT_APPLYING.getCode(), BIZ_APPROVE_REJECT.getCode(),
            BIZ_CONFIG_FAILURE.getCode(), BIZ_CONFIG_SUCCESS.getCode());

    /**
     * The status of the service that can be deleted:
     * <p/>[DRAFT] [BIZ_WAIT_APPLYING] [BIZ_APPROVE_REJECT] [BIZ_CONFIG_FAILURE] [BIZ_CONFIG_SUCCESS]
     * <p/>[BIZ_CONFIG_FAILURE] [BIZ_CONFIG_SUCCESS] status cannot be deleted
     */
    public static final List<Integer> ALLOW_DELETE_BIZ_STATUS = Arrays.asList(
            DRAFT.getCode(), BIZ_WAIT_APPLYING.getCode(), BIZ_APPROVE_REJECT.getCode(),
            BIZ_CONFIG_FAILURE.getCode(), BIZ_CONFIG_SUCCESS.getCode());

    /**
     * The business can cascade to delete the status of the associated data:
     * <p/>[DRAFT] [BIZ_WAIT_APPLYING]
     */
    public static final List<Integer> ALLOW_DELETE_BIZ_CASCADE_STATUS = Arrays.asList(
            DRAFT.getCode(), BIZ_WAIT_APPLYING.getCode());

    /**
     * Status of business approval:
     * <p/>[BIZ_CONFIG_FAILURE] [BIZ_CONFIG_SUCCESS]
     */
    public static final List<Integer> BIZ_APPROVE_PASS_STATUS = Arrays.asList(
            BIZ_CONFIG_FAILURE.getCode(), BIZ_CONFIG_SUCCESS.getCode());

    /**
     * Temporary business status, adding, deleting and modifying operations are not allowed:
     * <p/>[BIZ_WAIT_APPROVE] [BIZ_CONFIG_ING]
     */
    public static final List<Integer> BIZ_TEMP_STATUS = Arrays.asList(
            BIZ_WAIT_APPROVE.getCode(), BIZ_CONFIG_ING.getCode());

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
