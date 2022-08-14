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

package org.apache.inlong.manager.pojo.consumption.pulsar;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.inlong.manager.common.enums.ErrorCodeEnum;
import org.apache.inlong.manager.common.exceptions.BusinessException;

import javax.validation.constraints.NotNull;

/**
 * Inlong group info for Pulsar
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@ApiModel("Inlong group info for Pulsar")
public class ConsumePulsarDTO {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper(); // thread safe

    @ApiModelProperty(value = "Consumption ID")
    private Integer consumptionId;

//    @ApiModelProperty(value = "Consumer group")
//    private String consumerGroup;

//    @ApiModelProperty(value = "Consumption target inlong group id")
//    private String inlongGroupId;

    @ApiModelProperty("Whether to delete, 0: not deleted, 1: deleted")
    private Integer isDeleted = 0;

    @ApiModelProperty("Whether to configure the dead letter queue, 0: do not configure, 1: configure")
    private Integer isDlq;

    @ApiModelProperty("The name of the dead letter queue Topic")
    private String deadLetterTopic;

    @ApiModelProperty("Whether to configure the retry letter queue, 0: do not configure, 1: configure")
    private Integer isRlq;

    @ApiModelProperty("The name of the retry letter queue topic")
    private String retryLetterTopic;

    /**
     * Get the dto instance from the request
     */
    public static ConsumePulsarDTO getFromRequest(ConsumePulsarRequest request) {
        return ConsumePulsarDTO.builder()
                .consumptionId(request.getConsumptionId())
                .isDeleted(request.getIsDeleted())
                .isDlq(request.getIsDlq())
                .deadLetterTopic(request.getDeadLetterTopic())
                .isRlq(request.getIsRlq())
                .build();
    }

    /**
     * Get the dto instance from the JSON string.
     */
    public static ConsumePulsarDTO getFromJson(@NotNull String extParams) {
        try {
            OBJECT_MAPPER.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
            return OBJECT_MAPPER.readValue(extParams, ConsumePulsarDTO.class);
        } catch (Exception e) {
            throw new BusinessException(ErrorCodeEnum.CONSUMER_INFO_INCORRECT.getMessage() + ": " + e.getMessage());
        }
    }

}
