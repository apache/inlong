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

package org.apache.inlong.manager.client.api.util;

import java.nio.charset.Charset;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.commons.collections.CollectionUtils;
import org.apache.inlong.manager.client.api.DataFormat;
import org.apache.inlong.manager.client.api.DataSeparator;
import org.apache.inlong.manager.client.api.StreamField;
import org.apache.inlong.manager.client.api.StreamField.FieldType;
import org.apache.inlong.manager.client.api.StreamSink;
import org.apache.inlong.manager.client.api.auth.DefaultAuthentication;
import org.apache.inlong.manager.client.api.sink.HiveSink;
import org.apache.inlong.manager.client.api.sink.HiveSink.FileFormat;
import org.apache.inlong.manager.client.api.sink.KafkaSink;
import org.apache.inlong.manager.common.enums.SinkType;
import org.apache.inlong.manager.common.pojo.sink.SinkFieldRequest;
import org.apache.inlong.manager.common.pojo.sink.SinkRequest;
import org.apache.inlong.manager.common.pojo.sink.SinkResponse;
import org.apache.inlong.manager.common.pojo.sink.hive.HiveSinkRequest;
import org.apache.inlong.manager.common.pojo.sink.hive.HiveSinkResponse;
import org.apache.inlong.manager.common.pojo.sink.kafka.KafkaSinkRequest;
import org.apache.inlong.manager.common.pojo.sink.kafka.KafkaSinkResponse;
import org.apache.inlong.manager.common.pojo.stream.InlongStreamInfo;

public class InlongStreamSinkTransfer {

    public static SinkRequest createSinkRequest(StreamSink streamSink, InlongStreamInfo streamInfo) {
        SinkType sinkType = streamSink.getSinkType();
        if (sinkType == SinkType.HIVE) {
            return createHiveRequest(streamSink, streamInfo);
        }
        if (sinkType == SinkType.KAFKA) {
            return createKafkaRequest(streamSink, streamInfo);
        }
        throw new IllegalArgumentException(String.format("Unsupport sink type : %s for Inlong", sinkType));
    }

    public static StreamSink parseStreamSink(SinkResponse sinkResponse) {
        return parseStreamSink(sinkResponse, null);
    }

    public static StreamSink parseStreamSink(SinkResponse sinkResponse, StreamSink streamSink) {
        String type = sinkResponse.getSinkType();
        SinkType sinkType = SinkType.forType(type);
        if (sinkType == SinkType.HIVE) {
            return parseHiveSink((HiveSinkResponse) sinkResponse, streamSink);
        }
        if (sinkType == SinkType.KAFKA) {
            return parseKafkaSink((KafkaSinkResponse) sinkResponse, streamSink);
        }
        throw new IllegalArgumentException(String.format("Unsupport sink type : %s for Inlong", sinkType));
    }

    private static SinkRequest createKafkaRequest(StreamSink streamSink, InlongStreamInfo streamInfo) {
        KafkaSinkRequest kafkaSinkRequest = new KafkaSinkRequest();
        KafkaSink kafkaSink = (KafkaSink) streamSink;
        kafkaSinkRequest.setSinkName(streamSink.getSinkName());
        kafkaSinkRequest.setAddress(kafkaSink.getAddress());
        kafkaSinkRequest.setTopicName(kafkaSink.getTopicName());
        kafkaSinkRequest.setSinkType(kafkaSink.getSinkType().name());
        kafkaSinkRequest.setInlongGroupId(streamInfo.getInlongGroupId());
        kafkaSinkRequest.setInlongStreamId(streamInfo.getInlongStreamId());
        kafkaSinkRequest.setSerializationType(kafkaSink.getDataFormat().name());
        kafkaSinkRequest.setEnableCreateResource(kafkaSink.isNeedCreated() ? 1 : 0);
        if (CollectionUtils.isNotEmpty(kafkaSink.getStreamFields())) {
            List<SinkFieldRequest> fieldRequests = kafkaSink.getStreamFields()
                    .stream()
                    .map(streamField -> {
                        SinkFieldRequest storageFieldRequest = new SinkFieldRequest();
                        storageFieldRequest.setFieldName(streamField.getFieldName());
                        storageFieldRequest.setFieldType(streamField.getFieldType().toString());
                        storageFieldRequest.setFieldComment(streamField.getFieldComment());
                        return storageFieldRequest;
                    })
                    .collect(Collectors.toList());
            kafkaSinkRequest.setFieldList(fieldRequests);
        }
        return kafkaSinkRequest;
    }

    private static StreamSink parseKafkaSink(KafkaSinkResponse sinkResponse, StreamSink sink) {
        KafkaSink kafkaSink = new KafkaSink();
        kafkaSink.setSinkType(SinkType.KAFKA);
        if (sink != null) {
            AssertUtil.isTrue(sinkResponse.getSinkName().equals(sink.getSinkName()),
                    String.format("SinkName is not equal: %s != %s", sinkResponse, sink));
            KafkaSink snapshot = (KafkaSink) sink;
            kafkaSink.setSinkName(snapshot.getSinkName());
            kafkaSink.setAddress(snapshot.getAddress());
            kafkaSink.setTopicName(snapshot.getTopicName());
            kafkaSink.setDataFormat(snapshot.getDataFormat());
        } else {
            kafkaSink.setSinkName(sinkResponse.getSinkName());
            kafkaSink.setAddress(sinkResponse.getAddress());
            kafkaSink.setTopicName(sinkResponse.getTopicName());
            kafkaSink.setDataFormat(DataFormat.forName(sinkResponse.getSerializationType()));
        }

        kafkaSink.setNeedCreated(sinkResponse.getEnableCreateResource() == 1);
        List<StreamField> fieldList = sinkResponse.getFieldList()
                .stream()
                .map(storageFieldRequest -> {
                    return new StreamField(storageFieldRequest.getId(),
                            FieldType.forName(storageFieldRequest.getFieldType()),
                            storageFieldRequest.getFieldName(),
                            storageFieldRequest.getFieldComment(),
                            null);
                }).collect(Collectors.toList());
        kafkaSink.setStreamFields(fieldList);
        return kafkaSink;
    }

    private static HiveSinkRequest createHiveRequest(StreamSink streamSink, InlongStreamInfo streamInfo) {
        HiveSinkRequest hiveSinkRequest = new HiveSinkRequest();
        HiveSink hiveSink = (HiveSink) streamSink;
        hiveSinkRequest.setSinkName(streamSink.getSinkName());
        hiveSinkRequest.setInlongGroupId(streamInfo.getInlongGroupId());
        hiveSinkRequest.setInlongStreamId(streamInfo.getInlongStreamId());
        hiveSinkRequest.setDataEncoding(hiveSink.getCharset().name());
        hiveSinkRequest.setEnableCreateTable(hiveSink.isNeedCreated() ? 1 : 0);
        hiveSinkRequest.setDataSeparator(String.valueOf(hiveSink.getDataSeparator().getAsciiCode()));
        hiveSinkRequest.setDbName(hiveSink.getDbName());
        hiveSinkRequest.setTableName(hiveSink.getTableName());
        hiveSinkRequest.setHdfsDefaultFs(hiveSink.getHdfsDefaultFs());
        hiveSinkRequest.setJdbcUrl(hiveSink.getJdbcUrl());
        hiveSinkRequest.setWarehouseDir(hiveSink.getWarehouseDir());
        hiveSinkRequest.setFileFormat(hiveSink.getFileFormat().name());
        hiveSinkRequest.setSinkType(hiveSink.getSinkType().name());
        DefaultAuthentication defaultAuthentication = hiveSink.getAuthentication();
        AssertUtil.notNull(defaultAuthentication,
                String.format("Hive storage:%s must be authenticated", hiveSink.getDbName()));
        hiveSinkRequest.setUsername(defaultAuthentication.getUserName());
        hiveSinkRequest.setPassword(defaultAuthentication.getPassword());
        hiveSinkRequest.setPrimaryPartition(hiveSink.getPrimaryPartition());
        hiveSinkRequest.setSecondaryPartition(hiveSink.getSecondaryPartition());
        if (CollectionUtils.isNotEmpty(hiveSink.getStreamFields())) {
            List<SinkFieldRequest> fieldRequests = hiveSink.getStreamFields()
                    .stream()
                    .map(streamField -> {
                        SinkFieldRequest storageFieldRequest = new SinkFieldRequest();
                        storageFieldRequest.setFieldName(streamField.getFieldName());
                        storageFieldRequest.setFieldType(streamField.getFieldType().toString());
                        storageFieldRequest.setFieldComment(streamField.getFieldComment());
                        return storageFieldRequest;
                    })
                    .collect(Collectors.toList());
            hiveSinkRequest.setFieldList(fieldRequests);
        }
        return hiveSinkRequest;
    }

    private static HiveSink parseHiveSink(HiveSinkResponse sinkResponse, StreamSink sink) {
        HiveSink hiveSink = new HiveSink();
        if (sink != null) {
            AssertUtil.isTrue(sinkResponse.getSinkName().equals(sink.getSinkName()),
                    String.format("SinkName is not equal: %s != %s", sinkResponse, sink));
            HiveSink snapshot = (HiveSink) sink;
            hiveSink.setSinkName(snapshot.getSinkName());
            hiveSink.setDataSeparator(snapshot.getDataSeparator());
            hiveSink.setCharset(snapshot.getCharset());
            hiveSink.setAuthentication(snapshot.getAuthentication());
            hiveSink.setWarehouseDir(snapshot.getWarehouseDir());
            hiveSink.setFileFormat(snapshot.getFileFormat());
            hiveSink.setJdbcUrl(snapshot.getJdbcUrl());
            hiveSink.setTableName(snapshot.getTableName());
            hiveSink.setDbName(snapshot.getDbName());
            hiveSink.setHdfsDefaultFs(snapshot.getHdfsDefaultFs());
            hiveSink.setSecondaryPartition(snapshot.getSecondaryPartition());
            hiveSink.setPrimaryPartition(snapshot.getPrimaryPartition());
        } else {
            hiveSink.setSinkName(sinkResponse.getSinkName());
            hiveSink.setDataSeparator(DataSeparator.forAscii(Integer.parseInt(sinkResponse.getDataSeparator())));
            hiveSink.setCharset(Charset.forName(sinkResponse.getDataEncoding()));
            String password = sinkResponse.getPassword();
            String uname = sinkResponse.getUsername();
            hiveSink.setAuthentication(new DefaultAuthentication(uname, password));
            hiveSink.setWarehouseDir(sinkResponse.getWarehouseDir());
            hiveSink.setFileFormat(FileFormat.forName(sinkResponse.getFileFormat()));
            hiveSink.setJdbcUrl(sinkResponse.getJdbcUrl());
            hiveSink.setTableName(sinkResponse.getTableName());
            hiveSink.setDbName(sinkResponse.getDbName());
            hiveSink.setHdfsDefaultFs(sinkResponse.getHdfsDefaultFs());
            hiveSink.setSecondaryPartition(sinkResponse.getSecondaryPartition());
            hiveSink.setPrimaryPartition(sinkResponse.getPrimaryPartition());
        }

        hiveSink.setSinkType(SinkType.HIVE);
        hiveSink.setNeedCreated(sinkResponse.getEnableCreateResource() == 1);
        List<StreamField> fieldList = sinkResponse.getFieldList()
                .stream()
                .map(storageFieldRequest -> {
                    return new StreamField(storageFieldRequest.getId(),
                            FieldType.forName(storageFieldRequest.getFieldType()),
                            storageFieldRequest.getFieldName(),
                            storageFieldRequest.getFieldComment(),
                            null);
                }).collect(Collectors.toList());
        hiveSink.setStreamFields(fieldList);
        return hiveSink;
    }

}
