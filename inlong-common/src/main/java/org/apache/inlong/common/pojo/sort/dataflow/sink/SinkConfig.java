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

package org.apache.inlong.common.pojo.sort.dataflow.sink;

import org.apache.inlong.common.constant.SinkType;
import org.apache.inlong.common.pojo.sort.dataflow.field.FieldConfig;

import lombok.Data;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonSubTypes;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonTypeInfo;

import java.io.Serializable;
import java.util.List;

@Data
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "type")
@JsonSubTypes({
        @JsonSubTypes.Type(value = ClsSinkConfig.class, name = SinkType.CLS),
        @JsonSubTypes.Type(value = EsSinkConfig.class, name = SinkType.ELASTICSEARCH),
        @JsonSubTypes.Type(value = PulsarSinkConfig.class, name = SinkType.PULSAR),
        @JsonSubTypes.Type(value = KafkaSinkConfig.class, name = SinkType.KAFKA),
        @JsonSubTypes.Type(value = HttpSinkConfig.class, name = SinkType.HTTP),
})
public abstract class SinkConfig implements Serializable {

    private String encodingType;
    private List<FieldConfig> fieldConfigs;
}
