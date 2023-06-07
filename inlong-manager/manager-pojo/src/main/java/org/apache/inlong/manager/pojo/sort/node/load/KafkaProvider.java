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

package org.apache.inlong.manager.pojo.sort.node.load;

import org.apache.inlong.common.enums.DataTypeEnum;
import org.apache.inlong.manager.common.consts.SinkType;
import org.apache.inlong.manager.pojo.sink.kafka.KafkaSink;
import org.apache.inlong.manager.pojo.sort.node.base.LoadNodeProvider;
import org.apache.inlong.manager.pojo.stream.StreamField;
import org.apache.inlong.manager.pojo.stream.StreamNode;
import org.apache.inlong.sort.protocol.FieldInfo;
import org.apache.inlong.sort.protocol.node.LoadNode;
import org.apache.inlong.sort.protocol.node.format.AvroFormat;
import org.apache.inlong.sort.protocol.node.format.CanalJsonFormat;
import org.apache.inlong.sort.protocol.node.format.CsvFormat;
import org.apache.inlong.sort.protocol.node.format.DebeziumJsonFormat;
import org.apache.inlong.sort.protocol.node.format.Format;
import org.apache.inlong.sort.protocol.node.format.JsonFormat;
import org.apache.inlong.sort.protocol.node.format.RawFormat;
import org.apache.inlong.sort.protocol.node.load.KafkaLoadNode;
import org.apache.inlong.sort.protocol.transformation.FieldRelation;

import com.google.common.collect.Lists;
import org.apache.commons.lang3.StringUtils;

import java.util.List;
import java.util.Map;

/**
 * The Provider for creating Kafka load nodes.
 */
public class KafkaProvider implements LoadNodeProvider {

    @Override
    public Boolean accept(String sinkType) {
        return SinkType.KAFKA.equals(sinkType);
    }

    @Override
    public LoadNode createNode(StreamNode nodeInfo, Map<String, StreamField> constantFieldMap) {
        KafkaSink kafkaSink = (KafkaSink) nodeInfo;
        Map<String, String> properties = parseProperties(kafkaSink.getProperties());
        List<FieldInfo> fieldInfos = parseFieldInfos(kafkaSink.getSinkFieldList(), kafkaSink.getSinkName());
        List<FieldRelation> fieldRelations = parseSinkFields(kafkaSink.getSinkFieldList(), constantFieldMap);
        Integer sinkParallelism = null;
        if (StringUtils.isNotEmpty(kafkaSink.getPartitionNum())) {
            sinkParallelism = Integer.parseInt(kafkaSink.getPartitionNum());
        }
        DataTypeEnum dataType = DataTypeEnum.forType(kafkaSink.getSerializationType());
        Format format;
        switch (dataType) {
            case CSV:
                format = new CsvFormat();
                break;
            case AVRO:
                format = new AvroFormat();
                break;
            case JSON:
                format = new JsonFormat();
                break;
            case CANAL:
                format = new CanalJsonFormat();
                break;
            case DEBEZIUM_JSON:
                format = new DebeziumJsonFormat();
                break;
            case RAW:
                format = new RawFormat();
                break;
            default:
                throw new IllegalArgumentException(String.format("Unsupported dataType=%s for Kafka", dataType));
        }

        return new KafkaLoadNode(
                kafkaSink.getSinkName(),
                kafkaSink.getSinkName(),
                fieldInfos,
                fieldRelations,
                Lists.newArrayList(),
                null,
                kafkaSink.getTopicName(),
                kafkaSink.getBootstrapServers(),
                format,
                sinkParallelism,
                properties,
                kafkaSink.getPrimaryKey());
    }
}
