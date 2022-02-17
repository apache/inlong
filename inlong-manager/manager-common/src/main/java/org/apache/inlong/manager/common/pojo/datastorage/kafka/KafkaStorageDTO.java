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

package org.apache.inlong.manager.common.pojo.datastorage.kafka;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.swagger.annotations.ApiModelProperty;
import javax.validation.constraints.NotNull;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.inlong.manager.common.enums.BizErrorCodeEnum;
import org.apache.inlong.manager.common.exceptions.BusinessException;

/**
 * Kafka storage info
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class KafkaStorageDTO {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    @ApiModelProperty("Kafka sasl mechanism")
    private String saslMechanism;

    @ApiModelProperty("Kafka security protocol")
    private String securityProtocol;

    @ApiModelProperty("Kafka bootstrap servers")
    private String bootstrapServers;

    @ApiModelProperty("Group Id for ConsumerGroup")
    private String groupId;

    @ApiModelProperty("Kafka topicName")
    private String topicName;

    @ApiModelProperty("topic offset reset")
    private String topicOffsetReset;

    @ApiModelProperty("poll timeout ms")
    private Long pollTimeoutMs;

    @ApiModelProperty("enable auto commit")
    private Boolean enableAutoCommit;

    @ApiModelProperty("auto commit interval ms")
    private Integer autoCommitIntervalMs;

    @ApiModelProperty("session timeout ms")
    private Integer sessionTimeoutMs;

    /**
     * Get the dto instance from the request
     */
    public static KafkaStorageDTO getFromRequest(KafkaStorageRequest request) {
        return KafkaStorageDTO.builder()
                .saslMechanism(request.getSaslMechanism())
                .securityProtocol(request.getSecurityProtocol())
                .bootstrapServers(request.getBootstrapServers())
                .groupId(request.getGroupId())
                .topicName(request.getTopicName())
                .topicOffsetReset(request.getTopicOffsetReset())
                .pollTimeoutMs(request.getPollTimeoutMs())
                .enableAutoCommit(request.getEnableAutoCommit())
                .autoCommitIntervalMs(request.getAutoCommitIntervalMs())
                .sessionTimeoutMs(request.getSessionTimeoutMs())
                .build();
    }

    public static KafkaStorageDTO getFromJson(@NotNull String extParams) {
        try {
            OBJECT_MAPPER.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
            return OBJECT_MAPPER.readValue(extParams, KafkaStorageDTO.class);
        } catch (Exception e) {
            throw new BusinessException(BizErrorCodeEnum.STORAGE_INFO_INCORRECT.getMessage());
        }
    }
}
