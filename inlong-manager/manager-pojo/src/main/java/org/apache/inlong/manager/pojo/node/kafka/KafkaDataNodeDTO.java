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

package org.apache.inlong.manager.pojo.node.kafka;

import org.apache.inlong.manager.common.enums.ErrorCodeEnum;
import org.apache.inlong.manager.common.exceptions.BusinessException;
import org.apache.inlong.manager.common.util.CommonBeanUtils;
import org.apache.inlong.manager.common.util.JsonUtils;

import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.commons.lang3.StringUtils;

import javax.validation.constraints.NotNull;

/**
 * Kafka data node info
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@ApiModel("Kafka data node info")
public class KafkaDataNodeDTO {

    @ApiModelProperty("kafka bootstrapServers")
    private String bootstrapServers;

    @ApiModelProperty("kafka client id")
    private String clientId;

    @ApiModelProperty(value = "kafka produce confirmation mechanism")
    private String acks;

    @ApiModelProperty("audit set name")
    private String auditSetName;

    /**
     * Get the dto instance from the request
     */
    public static KafkaDataNodeDTO getFromRequest(KafkaDataNodeRequest request, String extParams) {
        KafkaDataNodeDTO dto = StringUtils.isNotBlank(extParams)
                ? KafkaDataNodeDTO.getFromJson(extParams)
                : new KafkaDataNodeDTO();
        return CommonBeanUtils.copyProperties(request, dto, true);
    }

    /**
     * Get the dto instance from the JSON string.
     */
    public static KafkaDataNodeDTO getFromJson(@NotNull String extParams) {
        try {
            return JsonUtils.parseObject(extParams, KafkaDataNodeDTO.class);
        } catch (Exception e) {
            throw new BusinessException(ErrorCodeEnum.DATA_NODE_INFO_INCORRECT,
                    String.format("Failed to parse extParams for kafka node: %s", e.getMessage()));
        }
    }
}
