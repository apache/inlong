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

package org.apache.inlong.manager.pojo.node.cls;

import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.inlong.manager.common.enums.ErrorCodeEnum;
import org.apache.inlong.manager.common.exceptions.BusinessException;
import org.apache.inlong.manager.common.util.JsonUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.validation.constraints.NotNull;

/**
 * Cls data node info
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@ApiModel("Cls data node info")
public class ClsDataNodeDTO {
    private static final Logger LOGGER = LoggerFactory.getLogger(ClsDataNodeDTO.class);

    @ApiModelProperty("Max send thread count, default is 50")
    @Builder.Default
    private Integer maxSendThreadCount = 50;

    @ApiModelProperty("Max block time, default is 0 seconds")
    @Builder.Default
    private Integer maxBlockSec = 0;

    @ApiModelProperty("Max message batch count, default is 4096")
    @Builder.Default
    private Integer maxBatchCount = 4096;

    @ApiModelProperty("Linger, default is 2000ms")
    @Builder.Default
    private Integer lingerMs = 2000;

    @ApiModelProperty("Retry times, default is 10")
    @Builder.Default
    private Integer retries = 10;

    @ApiModelProperty("Max reserve attempts, default is 11")
    @Builder.Default
    private Integer maxReservedAttempts = 11;

    @ApiModelProperty("Base retry backoff time, default is 100ms")
    @Builder.Default
    private Integer baseRetryBackoffMs = 100;

    @ApiModelProperty("Max retry backoff time, default is 50ms")
    @Builder.Default
    private Integer maxRetryBackoffMs = 50;

    /**
     * Get the dto instance from the request
     */
    public static ClsDataNodeDTO getFromRequest(ClsDataNodeRequest request) {
        return ClsDataNodeDTO.builder()
                .baseRetryBackoffMs(request.getBaseRetryBackoffMs())
                .maxRetryBackoffMs(request.getMaxRetryBackoffMs())
                .maxReservedAttempts(request.getMaxReservedAttempts())
                .lingerMs(request.getLingerMs())
                .maxBatchCount(request.getMaxBatchCount())
                .maxBlockSec(request.getMaxBlockSec())
                .maxSendThreadCount(request.getMaxSendThreadCount())
                .retries(request.getRetries())
                .build();
    }

    /**
     * Get the dto instance from the JSON string.
     */
    public static ClsDataNodeDTO getFromJson(@NotNull String extParams) {
        try {
            return JsonUtils.parseObject(extParams, ClsDataNodeDTO.class);
        } catch (Exception e) {
            LOGGER.error("Failed to extract additional parameters for cls data node: ", e);
            throw new BusinessException(ErrorCodeEnum.GROUP_INFO_INCORRECT.getMessage());
        }
    }

}
