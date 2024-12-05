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

package org.apache.inlong.sort.standalone.sink.kafka;

import org.apache.inlong.common.enums.DataTypeEnum;
import org.apache.inlong.common.pojo.sort.dataflow.DataFlowConfig;
import org.apache.inlong.common.pojo.sort.dataflow.dataType.CsvConfig;
import org.apache.inlong.common.pojo.sort.dataflow.dataType.DataTypeConfig;
import org.apache.inlong.common.pojo.sort.dataflow.dataType.KvConfig;
import org.apache.inlong.common.pojo.sort.dataflow.sink.KafkaSinkConfig;
import org.apache.inlong.sort.standalone.config.pojo.IdConfig;
import org.apache.inlong.sort.standalone.config.pojo.InlongId;
import org.apache.inlong.sort.standalone.utils.Constants;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.experimental.SuperBuilder;

import java.util.Map;

@EqualsAndHashCode(callSuper = true)
@Data
@NoArgsConstructor
@AllArgsConstructor
@SuperBuilder
public class KafkaIdConfig extends IdConfig {

    public static final String KEY_DATA_TYPE = "dataType";
    public static final String KEY_SEPARATOR = "separator";
    public static final String DEFAULT_SEPARATOR = "|";

    private String uid;
    private String separator = "|";
    private String topic;
    private DataTypeEnum dataType = DataTypeEnum.TEXT;

    public KafkaIdConfig(Map<String, String> idParam) {
        this.inlongGroupId = idParam.get(Constants.INLONG_GROUP_ID);
        this.inlongStreamId = idParam.get(Constants.INLONG_STREAM_ID);
        this.uid = InlongId.generateUid(inlongGroupId, inlongStreamId);
        this.separator = idParam.getOrDefault(KafkaIdConfig.KEY_SEPARATOR, KafkaIdConfig.DEFAULT_SEPARATOR);
        this.topic = idParam.getOrDefault(Constants.TOPIC, uid);
        this.dataType = DataTypeEnum
                .convert(idParam.getOrDefault(KafkaIdConfig.KEY_DATA_TYPE, DataTypeEnum.TEXT.getType()));
    }

    public static KafkaIdConfig create(DataFlowConfig dataFlowConfig) {
        KafkaSinkConfig sinkConfig = (KafkaSinkConfig) dataFlowConfig.getSinkConfig();
        DataTypeConfig dataTypeConfig = dataFlowConfig.getSourceConfig().getDataTypeConfig();
        String separator = DEFAULT_SEPARATOR;
        if (dataTypeConfig instanceof CsvConfig) {
            separator = String.valueOf(((CsvConfig) dataTypeConfig).getDelimiter());
        } else if (dataTypeConfig instanceof KvConfig) {
            separator = String.valueOf(((KvConfig) dataTypeConfig).getEntrySplitter());
        }

        return KafkaIdConfig.builder()
                .inlongGroupId(dataFlowConfig.getInlongGroupId())
                .inlongStreamId(dataFlowConfig.getInlongStreamId())
                .uid(InlongId.generateUid(dataFlowConfig.getInlongGroupId(), dataFlowConfig.getInlongStreamId()))
                .topic(sinkConfig.getTopicName())
                .dataType(DataTypeEnum.TEXT)
                .separator(separator)
                .build();
    }

}
