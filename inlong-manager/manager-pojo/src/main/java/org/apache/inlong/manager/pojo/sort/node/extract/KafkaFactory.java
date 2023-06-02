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

import org.apache.inlong.common.enums.DataTypeEnum;
import org.apache.inlong.manager.pojo.sort.node.ExtractNodeFactory;
import org.apache.inlong.manager.pojo.source.kafka.KafkaOffset;
import org.apache.inlong.manager.pojo.source.kafka.KafkaSource;
import org.apache.inlong.manager.pojo.stream.StreamNode;
import org.apache.inlong.sort.protocol.FieldInfo;
import org.apache.inlong.sort.protocol.enums.KafkaScanStartupMode;
import org.apache.inlong.sort.protocol.node.ExtractNode;
import org.apache.inlong.sort.protocol.node.extract.KafkaExtractNode;
import org.apache.inlong.sort.protocol.node.format.AvroFormat;
import org.apache.inlong.sort.protocol.node.format.CanalJsonFormat;
import org.apache.inlong.sort.protocol.node.format.CsvFormat;
import org.apache.inlong.sort.protocol.node.format.DebeziumJsonFormat;
import org.apache.inlong.sort.protocol.node.format.Format;
import org.apache.inlong.sort.protocol.node.format.InLongMsgFormat;
import org.apache.inlong.sort.protocol.node.format.JsonFormat;
import org.apache.inlong.sort.protocol.node.format.RawFormat;

import org.apache.commons.lang3.StringUtils;

import java.util.List;
import java.util.Map;

/**
 * The Factory for creating Kafka extract nodes.
 */
public class KafkaFactory extends ExtractNodeFactory {

    /**
     * Create Kafka extract node
     *
     * @param streamNodeInfo Kafka source info
     * @return Kafka extract node info
     */
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

    /**
     * Parse format
     *
     * @param serializationType data serialization, support: csv, json, canal, avro, etc
     * @param wrapWithInlongMsg whether wrap content with {@link InLongMsgFormat}
     * @param separatorStr the separator of data content
     * @param ignoreParseErrors whether ignore deserialization error data
     * @return the format for serialized content
     */
    public static Format parsingFormat(
            String serializationType,
            boolean wrapWithInlongMsg,
            String separatorStr,
            boolean ignoreParseErrors) {
        Format format;
        DataTypeEnum dataType = DataTypeEnum.forType(serializationType);
        switch (dataType) {
            case CSV:
                if (StringUtils.isNumeric(separatorStr)) {
                    char dataSeparator = (char) Integer.parseInt(separatorStr);
                    separatorStr = Character.toString(dataSeparator);
                }
                CsvFormat csvFormat = new CsvFormat(separatorStr);
                csvFormat.setIgnoreParseErrors(ignoreParseErrors);
                format = csvFormat;
                break;
            case AVRO:
                format = new AvroFormat();
                break;
            case JSON:
                JsonFormat jsonFormat = new JsonFormat();
                jsonFormat.setIgnoreParseErrors(ignoreParseErrors);
                format = jsonFormat;
                break;
            case CANAL:
                format = new CanalJsonFormat();
                break;
            case DEBEZIUM_JSON:
                DebeziumJsonFormat debeziumJsonFormat = new DebeziumJsonFormat();
                debeziumJsonFormat.setIgnoreParseErrors(ignoreParseErrors);
                format = debeziumJsonFormat;
                break;
            case RAW:
                format = new RawFormat();
                break;
            default:
                throw new IllegalArgumentException(String.format("Unsupported dataType=%s", dataType));
        }
        if (wrapWithInlongMsg) {
            Format innerFormat = format;
            format = new InLongMsgFormat(innerFormat, false);
        }
        return format;
    }
}