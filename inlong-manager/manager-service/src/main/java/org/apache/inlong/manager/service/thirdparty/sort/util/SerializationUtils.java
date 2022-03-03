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
import org.apache.inlong.manager.common.pojo.source.kafka.KafkaSourceResponse;
import org.apache.inlong.manager.common.pojo.stream.InlongStreamInfo;
import org.apache.inlong.sort.protocol.deserialization.AvroDeserializationInfo;
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

public class SerializationUtils {

    public static DeserializationInfo createDeserializationInfo(SourceResponse sourceResponse,
            InlongStreamInfo streamInfo) {
        SourceType sourceType = SourceType.forType(sourceResponse.getSourceType());
        switch (sourceType) {
            case BINLOG:
                return forBinlog((BinlogSourceResponse) sourceResponse, streamInfo);
            case KAFKA:
                return forKafka((KafkaSourceResponse) sourceResponse, streamInfo);
            case FILE:
                return forFile(sourceResponse, streamInfo);
            default:
                throw new IllegalArgumentException(String.format("Unsupport sourceType for Inlong:%s", sourceType));
        }
    }

    public static SerializationInfo createSerializationInfo(SourceResponse sourceResponse, SinkResponse sinkResponse,
            InlongStreamInfo inlongStreamInfo) {
        SinkType sinkType = SinkType.forType(sinkResponse.getSinkType());
        switch (sinkType) {
            case HIVE:
                return null;
            case KAFKA:
                return forKafka(sourceResponse, (KafkaSinkResponse) sinkResponse, inlongStreamInfo);
            default:
                throw new IllegalArgumentException(String.format("Unsupport sinkType for Inlong:%s", sinkType));
        }
    }

    public static DeserializationInfo forBinlog(BinlogSourceResponse binlogSourceResponse,
            InlongStreamInfo streamInfo) {
        return new DebeziumDeserializationInfo(true, binlogSourceResponse.getTimestampFormatStandard());
    }

    public static DeserializationInfo forKafka(KafkaSourceResponse kafkaSourceResponse,
            InlongStreamInfo streamInfo) {
        String serializationType = kafkaSourceResponse.getSerializationType();
        DataTypeEnum dataType = DataTypeEnum.forName(serializationType);
        switch (dataType) {
            case CSV:
                char seperator = streamInfo.getDataSeparator().toCharArray()[0];
                return new CsvDeserializationInfo(seperator);
            case AVRO:
                return new AvroDeserializationInfo();
            case JSON:
                return new JsonDeserializationInfo();
            default:
                throw new IllegalArgumentException(
                        String.format("Unsupport serializationType for Kafka source:%s", serializationType));
        }
    }

    public static SerializationInfo forKafka(SourceResponse sourceResponse, KafkaSinkResponse kafkaSinkResponse,
            InlongStreamInfo streamInfo) {
        String serializationType = kafkaSinkResponse.getSerializationType();
        DataTypeEnum dataType = DataTypeEnum.forName(serializationType);
        switch (dataType) {
            case AVRO:
                return new AvroSerializationInfo();
            case JSON:
                return new JsonSerializationInfo();
            case CANAL:
                Assert.isInstanceOf(BinlogSourceResponse.class, sourceResponse,
                        "Unsupport serializationType for Kafka;");
                BinlogSourceResponse binlogSourceResponse = (BinlogSourceResponse) sourceResponse;
                return new CanalSerializationInfo(binlogSourceResponse.getTimestampFormatStandard(),
                        "FAIL", "", false);
            case DEBEZIUM_JSON:
                Assert.isInstanceOf(BinlogSourceResponse.class, sourceResponse,
                        "Unsupport serializationType for Kafka;");
                binlogSourceResponse = (BinlogSourceResponse) sourceResponse;
                return new DebeziumSerializationInfo(binlogSourceResponse.getTimestampFormatStandard(),
                        "FAIL", "", false);
            default:
                throw new IllegalArgumentException(
                        String.format("Unsupport serializationType for Kafka sink:%s", serializationType));
        }
    }

    public static DeserializationInfo forFile(SourceResponse sourceResponse,
            InlongStreamInfo streamInfo) {
        String serializationType = sourceResponse.getSerializationType();
        DataTypeEnum dataType = DataTypeEnum.forName(serializationType);
        switch (dataType) {
            case CSV:
                char seperator = streamInfo.getDataSeparator().toCharArray()[0];
                return new CsvDeserializationInfo(seperator);
            case AVRO:
                return new AvroDeserializationInfo();
            case JSON:
                return new JsonDeserializationInfo();
            default:
                throw new IllegalArgumentException(
                        String.format("Unsupport type for File source:%s", serializationType));
        }
    }
}
