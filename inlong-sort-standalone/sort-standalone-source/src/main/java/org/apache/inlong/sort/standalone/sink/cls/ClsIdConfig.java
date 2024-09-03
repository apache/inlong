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

package org.apache.inlong.sort.standalone.sink.cls;

import org.apache.inlong.common.pojo.sort.dataflow.DataFlowConfig;
import org.apache.inlong.common.pojo.sort.dataflow.field.FieldConfig;
import org.apache.inlong.common.pojo.sort.dataflow.sink.ClsSinkConfig;
import org.apache.inlong.common.pojo.sort.node.ClsNodeConfig;
import org.apache.inlong.sort.standalone.config.pojo.IdConfig;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.experimental.SuperBuilder;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Cls config of each uid.
 */
@EqualsAndHashCode(callSuper = true)
@Data
@NoArgsConstructor
@AllArgsConstructor
@SuperBuilder
public class ClsIdConfig extends IdConfig {

    private String separator = "|";
    private String endpoint;
    private String secretId;
    private String secretKey;
    private String topicId;
    private List<String> fieldList;
    private int fieldOffset = 2;
    private int contentOffset = 0;

    public static ClsIdConfig create(DataFlowConfig dataFlowConfig, ClsNodeConfig nodeConfig) {
        ClsSinkConfig sinkConfig = (ClsSinkConfig) dataFlowConfig.getSinkConfig();
        List<String> fields = sinkConfig.getFieldConfigs()
                .stream()
                .map(FieldConfig::getName)
                .collect(Collectors.toList());
        return ClsIdConfig.builder()
                .inlongGroupId(dataFlowConfig.getInlongGroupId())
                .inlongStreamId(dataFlowConfig.getInlongStreamId())
                .contentOffset(sinkConfig.getContentOffset())
                .fieldOffset(sinkConfig.getFieldOffset())
                .separator(sinkConfig.getSeparator())
                .fieldList(fields)
                .topicId(sinkConfig.getTopicId())
                .endpoint(nodeConfig.getEndpoint())
                .secretId(nodeConfig.getSendSecretId())
                .secretKey(nodeConfig.getSendSecretKey())
                .build();
    }

}
