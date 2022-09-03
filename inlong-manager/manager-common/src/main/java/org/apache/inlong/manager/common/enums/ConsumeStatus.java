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
import com.google.common.collect.Lists;
import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import org.apache.inlong.manager.common.util.InlongCollectionUtils;
import org.apache.inlong.manager.common.util.Preconditions;

import java.util.Map;
import java.util.Set;
import java.util.function.Function;

/**
 * Inlong consume status
 */
@ApiModel("Inlong consume status")
public enum ConsumeStatus {

    @ApiModelProperty(value = "To be allocated: 10")
    WAIT_ASSIGN(10),

    @ApiModelProperty(value = "Pending approval: 11")
    WAIT_APPROVE(11),

    @ApiModelProperty(value = "Approval rejected: 20")
    REJECTED(20),

    @ApiModelProperty(value = "Approval and approval: 21")
    APPROVED(21),

    @ApiModelProperty(value = "Cancel application: 22")
    CANCELED(22),

    @ApiModelProperty(value = "Deleting: 41")
    DELETING(41),

    @ApiModelProperty(value = "Deleted: 40")
    DELETED(40),

    ;

    public static final Set<ConsumeStatus> ALLOW_SAVE_UPDATE_STATUS = ImmutableSet
            .of(WAIT_ASSIGN, REJECTED, CANCELED);

    public static final Set<ConsumeStatus> ALLOW_START_WORKFLOW_STATUS = ImmutableSet.of(WAIT_ASSIGN);

    private static final Map<Integer, ConsumeStatus> STATUS_MAP = InlongCollectionUtils.transformToImmutableMap(
            Lists.newArrayList(ConsumeStatus.values()),
            ConsumeStatus::getCode,
            Function.identity()
    );

    private final int code;

    ConsumeStatus(int code) {
        this.code = code;
    }

    public static ConsumeStatus fromStatus(int status) {
        ConsumeStatus consumeStatus = STATUS_MAP.get(status);
        Preconditions.checkNotNull(consumeStatus, "consume status is invalid for " + status);
        return consumeStatus;
    }

    public int getCode() {
        return code;
    }

}
