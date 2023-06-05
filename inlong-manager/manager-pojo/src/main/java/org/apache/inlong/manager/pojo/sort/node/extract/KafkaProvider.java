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

package org.apache.inlong.manager.pojo.sort.node.extract;

import org.apache.inlong.manager.common.consts.SourceType;
import org.apache.inlong.manager.pojo.sort.node.ExtractNodeProvider;
import org.apache.inlong.manager.pojo.source.kafka.KafkaOffset;
import org.apache.inlong.manager.pojo.source.kafka.KafkaSource;
import org.apache.inlong.manager.pojo.stream.StreamNode;
import org.apache.inlong.sort.protocol.FieldInfo;
import org.apache.inlong.sort.protocol.enums.KafkaScanStartupMode;
import org.apache.inlong.sort.protocol.node.ExtractNode;
import org.apache.inlong.sort.protocol.node.extract.KafkaExtractNode;
import org.apache.inlong.sort.protocol.node.format.Format;

import java.util.List;
import java.util.Map;

/**
 * The Provider for creating Kafka extract nodes.
 */
public class KafkaProvider implements ExtractNodeProvider {

    @Override
    public Boolean accept(String sourceType) {
        return SourceType.KAFKA.equals(sourceType);
    }

    @Override
    public ExtractNode createNode(StreamNode streamNodeInfo) {
        KafkaSource kafkaSource = (KafkaSource) streamNodeInfo;
        List<FieldInfo> fieldInfos = parseFieldInfos(kafkaSource.getFieldList(), kafkaSource.getSourceName());
        Map<String, String> properties = parseProperties(kafkaSource.getProperties());

        String topic = kafkaSource.getTopic();
        String bootstrapServers = kafkaSource.getBootstrapServers();

        Format format = parsingFormat(
                kafkaSource.getSerializationType(),
                kafkaSource.isWrapWithInlongMsg(),
                kafkaSource.getDataSeparator(),
                kafkaSource.isIgnoreParseErrors());

        KafkaOffset kafkaOffset = KafkaOffset.forName(kafkaSource.getAutoOffsetReset());
        KafkaScanStartupMode startupMode;
        switch (kafkaOffset) {
            case EARLIEST:
                startupMode = KafkaScanStartupMode.EARLIEST_OFFSET;
                break;
            case SPECIFIC:
                startupMode = KafkaScanStartupMode.SPECIFIC_OFFSETS;
                break;
            case TIMESTAMP_MILLIS:
                startupMode = KafkaScanStartupMode.TIMESTAMP_MILLIS;
                break;
            case LATEST:
            default:
                startupMode = KafkaScanStartupMode.LATEST_OFFSET;
        }
        final String primaryKey = kafkaSource.getPrimaryKey();
        String groupId = kafkaSource.getGroupId();
        String partitionOffset = kafkaSource.getPartitionOffsets();
        String scanTimestampMillis = kafkaSource.getTimestampMillis();
        return new KafkaExtractNode(kafkaSource.getSourceName(),
                kafkaSource.getSourceName(),
                fieldInfos,
                null,
                properties,
                topic,
                bootstrapServers,
                format,
                startupMode,
                primaryKey,
                groupId,
                partitionOffset,
                scanTimestampMillis);
    }
}