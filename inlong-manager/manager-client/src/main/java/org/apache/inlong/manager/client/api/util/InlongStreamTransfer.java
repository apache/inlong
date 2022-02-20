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

import org.apache.commons.collections.CollectionUtils;
import org.apache.inlong.manager.client.api.DataSeparator;
import org.apache.inlong.manager.client.api.HiveSink;
import org.apache.inlong.manager.client.api.HiveSink.FileFormat;
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
import org.apache.inlong.manager.common.pojo.sink.hive.HiveSinkResponse;
import org.apache.inlong.manager.common.pojo.stream.InlongStreamFieldInfo;
import org.apache.inlong.manager.common.pojo.stream.InlongStreamInfo;

import java.nio.charset.Charset;
import java.util.List;
import java.util.stream.Collectors;

public class InlongStreamTransfer {

    public static InlongStreamInfo createStreamInfo(InlongStreamConf streamConf, InlongGroupRequest groupInfo) {
        InlongStreamInfo streamInfo = new InlongStreamInfo();
        streamInfo.setInlongGroupId(groupInfo.getInlongGroupId());
        streamInfo.setInlongStreamId("b_" + streamConf.getName());
        streamInfo.setName(streamConf.getName());
        streamInfo.setDataEncoding(streamConf.getCharset().name());
        streamInfo.setMqResourceObj(groupInfo.getName() + "_" + streamConf.getName());
        streamInfo.setDataSeparator(String.valueOf(streamConf.getDataSeparator().getAsciiCode()));
        streamInfo.setDescription(streamConf.getDescription());
        streamInfo.setCreator(groupInfo.getCreator());
        streamInfo.setDailyRecords(streamConf.getDailyRecords());
        streamInfo.setDailyStorage(streamConf.getDailyStorage());
        streamInfo.setPeakRecords(streamConf.getPeakRecords());
        streamInfo.setHavePredefinedFields(0);
        return streamInfo;
    }

    public static List<InlongStreamFieldInfo> createStreamFields(List<StreamField> fieldList,
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
            return createHiveSinkRequest(streamSink, streamInfo);
        } else {
            throw new IllegalArgumentException(String.format("Unsupported sink type: %s for Inlong", sinkType));
        }
    }

    public static StreamSink parseStreamSink(SinkResponse sinkResponse) {
        String sinkType = sinkResponse.getSinkType();
        if ("HIVE".equals(sinkType)) {
            return parseHiveSink((HiveSinkResponse) sinkResponse);
        } else {
            throw new IllegalArgumentException(String.format("Unsupported sink type: %s for Inlong", sinkType));
        }
    }

    private static HiveSinkRequest createHiveSinkRequest(StreamSink streamSink, InlongStreamInfo streamInfo) {
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
        DefaultAuthentication defaultAuthentication = hiveSink.getAuthentication();
        AssertUtil.notNull(defaultAuthentication,
                String.format("Hive sink %s must be authenticated", hiveSink.getDbName()));
        hiveSinkRequest.setUsername(defaultAuthentication.getUserName());
        hiveSinkRequest.setPassword(defaultAuthentication.getPassword());
        hiveSinkRequest.setPrimaryPartition(hiveSink.getPrimaryPartition());
        hiveSinkRequest.setSecondaryPartition(hiveSink.getSecondaryPartition());
        if (CollectionUtils.isNotEmpty(hiveSink.getStreamFields())) {
            List<SinkFieldRequest> fieldRequests = hiveSink.getStreamFields()
                    .stream()
                    .map(streamField -> {
                        SinkFieldRequest fieldRequest = new SinkFieldRequest();
                        fieldRequest.setFieldName(streamField.getFieldName());
                        fieldRequest.setFieldType(streamField.getFieldType().toString());
                        fieldRequest.setFieldComment(streamField.getFieldComment());
                        return fieldRequest;
                    })
                    .collect(Collectors.toList());
        }
        return hiveSinkRequest;
    }

    private static HiveSink parseHiveSink(HiveSinkResponse sinkResponse) {
        HiveSink hiveSink = new HiveSink();
        hiveSink.setSinkType(SinkType.HIVE);
        int asciiCode = Integer.parseInt(sinkResponse.getDataSeparator());
        hiveSink.setDataSeparator(DataSeparator.getByAscii(asciiCode));
        hiveSink.setCharset(Charset.forName(sinkResponse.getDataEncoding()));
        hiveSink.setDbName(sinkResponse.getDbName());
        hiveSink.setTableName(sinkResponse.getTableName());
        hiveSink.setFileFormat(FileFormat.forName(sinkResponse.getFileFormat()));
        hiveSink.setJdbcUrl(sinkResponse.getJdbcUrl());
        hiveSink.setHdfsDefaultFs(sinkResponse.getHdfsDefaultFs());
        hiveSink.setWarehouseDir(sinkResponse.getWarehouseDir());
        hiveSink.setAuthentication(new DefaultAuthentication(sinkResponse.getUsername(), sinkResponse.getPassword()));
        hiveSink.setNeedCreated(sinkResponse.getEnableCreateResource() == 1);
        hiveSink.setPrimaryPartition(sinkResponse.getPrimaryPartition());
        hiveSink.setSecondaryPartition(sinkResponse.getSecondaryPartition());
        if (CollectionUtils.isNotEmpty(sinkResponse.getFieldList())) {
            List<StreamField> fieldList = sinkResponse.getFieldList()
                    .stream()
                    .map(sinkFieldRequest -> new StreamField(sinkFieldRequest.getId(),
                            FieldType.forName(sinkFieldRequest.getFieldType()),
                            sinkFieldRequest.getFieldName(),
                            sinkFieldRequest.getFieldComment(),
                            null)).collect(Collectors.toList());
            hiveSink.setStreamFields(fieldList);
        }
        return hiveSink;
    }

}
