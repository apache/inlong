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

import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import org.apache.inlong.manager.common.enums.BizConstant;
import org.apache.inlong.manager.common.pojo.datastorage.StorageResponse;

/**
 * Response of the Kafka storage
 */
@Data
@ToString(callSuper = true)
@EqualsAndHashCode(callSuper = true)
@ApiModel(value = "Response of the Kafka storage")
public class KafkaStorageResponse extends StorageResponse {

    private String storageType = BizConstant.STORAGE_HIVE;

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

}
