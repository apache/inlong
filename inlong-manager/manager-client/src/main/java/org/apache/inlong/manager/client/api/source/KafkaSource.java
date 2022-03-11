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

package org.apache.inlong.manager.client.api.source;

import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import org.apache.inlong.manager.client.api.DataFormat;
import org.apache.inlong.manager.client.api.StreamSource;
import org.apache.inlong.manager.common.enums.KafkaOffset;
import org.apache.inlong.manager.common.enums.SourceType;

@Data
@EqualsAndHashCode(callSuper = true)
@AllArgsConstructor
@NoArgsConstructor
@ApiModel("Base configuration for Kafka collection")
public class KafkaSource extends StreamSource {

    @ApiModelProperty(value = "DataSource type", required = true)
    private SourceType sourceType = SourceType.KAFKA;

    @ApiModelProperty("SyncType for Kafka")
    private SyncType syncType;

    @ApiModelProperty("Data format type for kafka")
    private DataFormat dataFormat;

    @ApiModelProperty("Kafka topic")
    private String topic;

    @ApiModelProperty("Kafka consumer group")
    private String consumerGroup;

    @ApiModelProperty("Kafka servers address, such as: 127.0.0.1:9092")
    private String bootstrapServers;

    @ApiModelProperty(value = "Limit the amount of data read per second",
            notes = "Greater than or equal to 0, equal to zero means no limit")
    private String recordSpeedLimit;

    @ApiModelProperty(value = "Limit the number of bytes read per second",
            notes = "Greater than or equal to 0, equal to zero means no limit")
    private String byteSpeedLimit;

    @ApiModelProperty(value = "Topic partition offset",
            notes = "For example, '0#100_1#10' means the offset of partition 0 is 100, the offset of partition 1 is 10")
    private String topicPartitionOffset;

    @ApiModelProperty(value = "The strategy of auto offset reset")
    private KafkaOffset autoOffsetReset;

}
