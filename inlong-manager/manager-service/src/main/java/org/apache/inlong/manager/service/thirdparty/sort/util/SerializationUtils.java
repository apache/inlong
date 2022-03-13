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

package org.apache.inlong.manager.service.thirdparty.sort.util;

import org.apache.inlong.common.enums.DataTypeEnum;
import org.apache.inlong.manager.common.enums.SinkType;
import org.apache.inlong.manager.common.enums.SourceType;
import org.apache.inlong.manager.common.pojo.sink.SinkResponse;
import org.apache.inlong.manager.common.pojo.sink.kafka.KafkaSinkResponse;
import org.apache.inlong.manager.common.pojo.source.SourceResponse;
import org.apache.inlong.manager.common.pojo.source.binlog.BinlogSourceResponse;
import org.apache.inlong.manager.common.pojo.source.kafka.CanalConfig;
import org.apache.inlong.manager.common.pojo.source.kafka.KafkaSourceResponse;
import org.apache.inlong.manager.common.pojo.stream.InlongStreamInfo;
import org.apache.inlong.sort.protocol.deserialization.AvroDeserializationInfo;
import org.apache.inlong.sort.protocol.deserialization.CanalDeserializationInfo;
import org.apache.inlong.sort.protocol.deserialization.CsvDeserializationInfo;
import org.apache.inlong.sort.protocol.deserialization.DebeziumDeserializationInfo;
import org.apache.inlong.sort.protocol.deserialization.DeserializationInfo;
import org.apache.inlong.sort.protocol.deserialization.JsonDeserializationInfo;
import org.apache.inlong.sort.protocol.serialization.AvroSerializationInfo;
import org.apache.inlong.sort.protocol.serialization.CanalSerializationInfo;
import org.apache.inlong.sort.protocol.serialization.DebeziumSerializationInfo;
import org.apache.inlong.sort.protocol.serialization.JsonSerializationInfo;
import org.apache.inlong.sort.protocol.serialization.SerializationInfo;
import org.springframework.util.Assert;

import java.util.Map;

/**
 * Utils for Serialization and Deserialization info
 */
public class SerializationUtils {

    /**
     * Create deserialization info
     */
    public static DeserializationInfo createDeserialInfo(SourceResponse sourceResponse,
            InlongStreamInfo streamInfo) {
        SourceType sourceType = SourceType.forType(sourceResponse.getSourceType());
        switch (sourceType) {
            case BINLOG:
                return deserializeForBinlog((BinlogSourceResponse) sourceResponse);
            case KAFKA:
                return deserializeForKafka((KafkaSourceResponse) sourceResponse, streamInfo);
            case FILE:
                return deserializeForFile(sourceResponse, streamInfo);
            default:
                throw new IllegalArgumentException(String.format("Unsupported sourceType: %s", sourceType));
        }
    }

    /**
     * Create serialization info
     */
    public static SerializationInfo createSerialInfo(SourceResponse sourceResponse, SinkResponse sinkResponse) {
        SinkType sinkType = SinkType.forType(sinkResponse.getSinkType());
        switch (sinkType) {
            case HIVE:
                return null;
            case KAFKA:
                return serializeForKafka(sourceResponse, (KafkaSinkResponse) sinkResponse);
            default:
                throw new IllegalArgumentException(String.format("Unsupported sinkType: %s", sinkType));
        }
    }

    /**
     * Get serialization info for Binlog
     */
    private static DeserializationInfo deserializeForBinlog(BinlogSourceResponse sourceResponse) {
        return new DebeziumDeserializationInfo(true, sourceResponse.getTimestampFormatStandard());
    }

    /**
     * Get deserialization info for Kafka
     */
    private static DeserializationInfo deserializeForKafka(KafkaSourceResponse source, InlongStreamInfo stream) {
        String serializationType = source.getSerializationType();
        DataTypeEnum dataType = DataTypeEnum.forName(serializationType);
        switch (dataType) {
            case CSV:
                char separator = stream.getDataSeparator().toCharArray()[0];
                return new CsvDeserializationInfo(separator);
            case AVRO:
                return new AvroDeserializationInfo();
            case JSON:
                return new JsonDeserializationInfo();
            case CANAL:
                Map<String, Object> properties = source.getProperties();
                CanalConfig canalConfig = CanalConfig.forCanalConfig(properties);
                return new CanalDeserializationInfo(canalConfig.getDatabase(),
                        canalConfig.getTable(),
                        canalConfig.isIgnoreParseErrors(),
                        canalConfig.getTimestampFormatStandard(),
                        canalConfig.isIncludeMetadata());
            default:
                throw new IllegalArgumentException(
                        String.format("Unsupported serializationType for Kafka source: %s", serializationType));
        }
    }

    /**
     * Get serialization info for Kafka
     */
    private static SerializationInfo serializeForKafka(SourceResponse sourceResponse, KafkaSinkResponse sinkResponse) {
        String serializationType = sinkResponse.getSerializationType();
        DataTypeEnum dataType = DataTypeEnum.forName(serializationType);
        switch (dataType) {
            case AVRO:
                return new AvroSerializationInfo();
            case JSON:
                return new JsonSerializationInfo();
            case CANAL:
                return new CanalSerializationInfo();
            case DEBEZIUM_JSON:
                Assert.isInstanceOf(BinlogSourceResponse.class, sourceResponse,
                        "Unsupported serializationType for Kafka");
                BinlogSourceResponse binlogSource = (BinlogSourceResponse) sourceResponse;
                return new DebeziumSerializationInfo(binlogSource.getTimestampFormatStandard(),
                        "FAIL", "", false);
            default:
                throw new IllegalArgumentException(
                        String.format("Unsupported serializationType for Kafka sink: %s", serializationType));
        }
    }

    /**
     * Get deserialization info for File
     */
    private static DeserializationInfo deserializeForFile(SourceResponse sourceResponse, InlongStreamInfo streamInfo) {
        String serializationType = sourceResponse.getSerializationType();
        DataTypeEnum dataType = DataTypeEnum.forName(serializationType);
        switch (dataType) {
            case CSV:
                char separator = streamInfo.getDataSeparator().toCharArray()[0];
                return new CsvDeserializationInfo(separator);
            case AVRO:
                return new AvroDeserializationInfo();
            case JSON:
                return new JsonDeserializationInfo();
            default:
                throw new IllegalArgumentException(
                        String.format("Unsupported type for File source:%s", serializationType));
        }
    }
}
