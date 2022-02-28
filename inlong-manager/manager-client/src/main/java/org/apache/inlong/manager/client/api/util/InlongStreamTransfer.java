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

import java.util.List;
import java.util.stream.Collectors;
import org.apache.commons.collections.CollectionUtils;
import org.apache.inlong.manager.client.api.sink.HiveSink;
import org.apache.inlong.manager.client.api.InlongStreamConf;
import org.apache.inlong.manager.client.api.StreamField;
import org.apache.inlong.manager.client.api.StreamField.FieldType;
import org.apache.inlong.manager.client.api.StreamSink;
import org.apache.inlong.manager.client.api.StreamSink.SinkType;
import org.apache.inlong.manager.client.api.auth.DefaultAuthentication;
import org.apache.inlong.manager.common.pojo.group.InlongGroupRequest;
import org.apache.inlong.manager.common.pojo.sink.SinkFieldRequest;
import org.apache.inlong.manager.common.pojo.sink.SinkRequest;
import org.apache.inlong.manager.common.pojo.sink.SinkResponse;
import org.apache.inlong.manager.common.pojo.sink.hive.HiveSinkRequest;
import org.apache.inlong.manager.common.pojo.stream.InlongStreamFieldInfo;
import org.apache.inlong.manager.common.pojo.stream.InlongStreamInfo;

public class InlongStreamTransfer {

    public static InlongStreamInfo createStreamInfo(InlongStreamConf streamConf, InlongGroupRequest groupRequest) {
        InlongStreamInfo dataStreamInfo = new InlongStreamInfo();
        dataStreamInfo.setInlongGroupId(groupRequest.getInlongGroupId());
        dataStreamInfo.setInlongStreamId("b_" + streamConf.getName());
        dataStreamInfo.setName(streamConf.getName());
        dataStreamInfo.setDataEncoding(streamConf.getCharset().name());
        dataStreamInfo.setMqResourceObj(groupRequest.getMqResourceObj() + "_" + streamConf.getName());
        dataStreamInfo.setDataSeparator(String.valueOf(streamConf.getDataSeparator().getAsciiCode()));
        dataStreamInfo.setDescription(streamConf.getDescription());
        dataStreamInfo.setCreator(groupRequest.getCreator());
        dataStreamInfo.setDailyRecords(streamConf.getDailyRecords());
        dataStreamInfo.setDailyStorage(streamConf.getDailyStorage());
        dataStreamInfo.setPeakRecords(streamConf.getPeakRecords());
        dataStreamInfo.setHavePredefinedFields(0);
        return dataStreamInfo;
    }

    public static List<InlongStreamFieldInfo> createStreamFields(
            List<StreamField> fieldList,
            InlongStreamInfo streamInfo) {
        List<InlongStreamFieldInfo> fieldInfos = fieldList.stream().map(streamField -> {
            InlongStreamFieldInfo fieldInfo = new InlongStreamFieldInfo();
            fieldInfo.setInlongStreamId(streamInfo.getInlongStreamId());
            fieldInfo.setInlongGroupId(streamInfo.getInlongGroupId());
            fieldInfo.setFieldName(streamField.getFieldName());
            fieldInfo.setFieldType(streamField.getFieldType().toString());
            fieldInfo.setFieldComment(streamField.getFieldComment());
            fieldInfo.setFieldValue(streamField.getFieldValue());
            return fieldInfo;
        }).collect(Collectors.toList());
        return fieldInfos;
    }

    public static SinkRequest createSinkRequest(StreamSink streamSink, InlongStreamInfo streamInfo) {
        SinkType sinkType = streamSink.getSinkType();
        if (sinkType == SinkType.HIVE) {
            return createHiveRequest(streamSink, streamInfo);
        } else {
            throw new IllegalArgumentException(String.format("Unsupport sink type : %s for Inlong", sinkType));
        }
    }

    public static StreamSink parseStreamSink(SinkResponse sinkResponse, StreamSink streamSink) {
        String type = sinkResponse.getSinkType();
        SinkType sinkType = SinkType.forType(type);
        if (sinkType == SinkType.HIVE) {
            return parseHiveSink(sinkResponse, (HiveSink) streamSink);
        } else {
            throw new IllegalArgumentException(String.format("Unsupport sink type : %s for Inlong", sinkType));
        }
    }

    private static HiveSinkRequest createHiveRequest(StreamSink streamSink, InlongStreamInfo streamInfo) {
        HiveSinkRequest hiveSinkRequest = new HiveSinkRequest();
        HiveSink hiveSink = (HiveSink) streamSink;
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

    private static HiveSink parseHiveSink(SinkResponse sinkResponse, HiveSink snapshot) {
        HiveSink hiveSink = new HiveSink();
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
