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

package org.apache.inlong.common.pojo.sort.node;

import org.apache.inlong.common.constant.DataNodeType;

import lombok.Data;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonSubTypes;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonTypeInfo;

import java.io.Serializable;
import java.util.Map;

@Data
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "type")
@JsonSubTypes({
        @JsonSubTypes.Type(value = ClsNodeConfig.class, name = DataNodeType.CLS),
        @JsonSubTypes.Type(value = EsNodeConfig.class, name = DataNodeType.ELASTICSEARCH),
        @JsonSubTypes.Type(value = PulsarNodeConfig.class, name = DataNodeType.PULSAR),
        @JsonSubTypes.Type(value = KafkaNodeConfig.class, name = DataNodeType.KAFKA),
        @JsonSubTypes.Type(value = HttpNodeConfig.class, name = DataNodeType.HTTP),
})
public abstract class NodeConfig implements Serializable {

    private Integer version;
    private String nodeName;
    private Map<String, String> properties;
}
