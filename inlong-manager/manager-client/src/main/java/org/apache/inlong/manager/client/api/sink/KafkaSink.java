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

package org.apache.inlong.manager.client.api.sink;

import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import java.util.List;
import java.util.Map;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import org.apache.inlong.manager.client.api.DataFormat;
import org.apache.inlong.manager.client.api.SinkField;
import org.apache.inlong.manager.client.api.StreamSink;
import org.apache.inlong.manager.common.enums.SinkType;

@Data
@EqualsAndHashCode(callSuper = true)
@NoArgsConstructor
@AllArgsConstructor
@ApiModel("Kafka sink configuration")
public class KafkaSink extends StreamSink {

    @ApiModelProperty(value = "Sink type", required = true)
    private SinkType sinkType = SinkType.KAFKA;

    @ApiModelProperty("Kafka bootstrap servers")
    private String address;

    @ApiModelProperty("Kafka topicName")
    private String topicName;

    @ApiModelProperty("Data format type for stream sink")
    private DataFormat dataFormat;

    @ApiModelProperty("Create topic or not")
    private boolean needCreated;

    @ApiModelProperty("Field definitions for kafka")
    private List<SinkField> sinkFields;

    @ApiModelProperty("Other properties if need")
    private Map<String, String> properties;
}
