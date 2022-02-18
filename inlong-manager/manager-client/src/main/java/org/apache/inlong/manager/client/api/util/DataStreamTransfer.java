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
import org.apache.inlong.manager.client.api.DataSeparator;
import org.apache.inlong.manager.client.api.DataStreamConf;
import org.apache.inlong.manager.client.api.HiveSink;
import org.apache.inlong.manager.client.api.HiveSink.FileFormat;
import org.apache.inlong.manager.client.api.StreamField;
import org.apache.inlong.manager.client.api.StreamField.FieldType;
import org.apache.inlong.manager.client.api.StreamSink;
import org.apache.inlong.manager.client.api.StreamSink.SinkType;
import org.apache.inlong.manager.client.api.auth.DefaultAuthentication;
import org.apache.inlong.manager.common.pojo.business.BusinessInfo;
import org.apache.inlong.manager.common.pojo.datastorage.StorageFieldRequest;
import org.apache.inlong.manager.common.pojo.datastorage.StorageRequest;
import org.apache.inlong.manager.common.pojo.datastorage.StorageResponse;
import org.apache.inlong.manager.common.pojo.datastorage.hive.HiveStorageRequest;
import org.apache.inlong.manager.common.pojo.datastorage.hive.HiveStorageResponse;
import org.apache.inlong.manager.common.pojo.datastream.DataStreamFieldInfo;
import org.apache.inlong.manager.common.pojo.datastream.DataStreamInfo;

public class DataStreamTransfer {

    public static DataStreamInfo createDataStreamInfo(DataStreamConf streamConf, BusinessInfo businessInfo) {
        DataStreamInfo dataStreamInfo = new DataStreamInfo();
        dataStreamInfo.setInlongGroupId(businessInfo.getInlongGroupId());
        dataStreamInfo.setInlongStreamId("b_" + streamConf.getName());
        dataStreamInfo.setName(streamConf.getName());
        dataStreamInfo.setDataEncoding(streamConf.getCharset().name());
        dataStreamInfo.setMqResourceObj(businessInfo.getName() + "_" + streamConf.getName());
        dataStreamInfo.setDataSeparator(String.valueOf(streamConf.getDataSeparator().getAsciiCode()));
        dataStreamInfo.setDescription(streamConf.getDescription());
        dataStreamInfo.setCreator(businessInfo.getCreator());
        dataStreamInfo.setDailyRecords(streamConf.getDailyRecords());
        dataStreamInfo.setDailyStorage(streamConf.getDailyStorage());
        dataStreamInfo.setPeakRecords(streamConf.getPeakRecords());
        dataStreamInfo.setHavePredefinedFields(0);
        return dataStreamInfo;
    }

    public static List<DataStreamFieldInfo> createStreamFields(List<StreamField> fieldList, DataStreamInfo streamInfo) {
        List<DataStreamFieldInfo> fieldInfos = fieldList.stream().map(streamField -> {
            DataStreamFieldInfo fieldInfo = new DataStreamFieldInfo();
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

    public static StorageRequest createStorageRequest(StreamSink streamSink, DataStreamInfo streamInfo) {
        SinkType sinkType = streamSink.getSinkType();
        if (sinkType == SinkType.HIVE) {
            return createHiveRequest(streamSink, streamInfo);
        } else {
            throw new IllegalArgumentException(String.format("Unsupport sink type : %s for Inlong", sinkType));
        }
    }

    public static StreamSink parseStreamSink(StorageResponse storageResponse) {
        String storageType = storageResponse.getStorageType();
        if ("HIVE".equals(storageType)) {
            return parseHiveSink((HiveStorageResponse) storageResponse);
        } else {
            throw new IllegalArgumentException(String.format("Unsupport storage type : %s for Inlong", storageType));
        }
    }

    private static HiveStorageRequest createHiveRequest(StreamSink streamSink, DataStreamInfo streamInfo) {
        HiveStorageRequest hiveStorageRequest = new HiveStorageRequest();
        HiveSink hiveSink = (HiveSink) streamSink;
        hiveStorageRequest.setInlongGroupId(streamInfo.getInlongGroupId());
        hiveStorageRequest.setInlongStreamId(streamInfo.getInlongStreamId());
        hiveStorageRequest.setDataEncoding(hiveSink.getCharset().name());
        hiveStorageRequest.setEnableCreateTable(hiveSink.isNeedCreated() ? 1 : 0);
        hiveStorageRequest.setDataSeparator(String.valueOf(hiveSink.getDataSeparator().getAsciiCode()));
        hiveStorageRequest.setDbName(hiveSink.getDbName());
        hiveStorageRequest.setTableName(hiveSink.getTableName());
        hiveStorageRequest.setHdfsDefaultFs(hiveSink.getHdfsDefaultFs());
        hiveStorageRequest.setJdbcUrl(hiveSink.getJdbcUrl());
        hiveStorageRequest.setWarehouseDir(hiveSink.getWarehouseDir());
        hiveStorageRequest.setFileFormat(hiveSink.getFileFormat().name());
        DefaultAuthentication defaultAuthentication = hiveSink.getAuthentication();
        AssertUtil.notNull(defaultAuthentication,
                String.format("Hive storage:%s must be authenticated", hiveSink.getDbName()));
        hiveStorageRequest.setUsername(defaultAuthentication.getUserName());
        hiveStorageRequest.setPassword(defaultAuthentication.getPassword());
        hiveStorageRequest.setPrimaryPartition(hiveSink.getPrimaryPartition());
        hiveStorageRequest.setSecondaryPartition(hiveSink.getSecondaryPartition());
        if (CollectionUtils.isNotEmpty(hiveSink.getStreamFields())) {
            List<StorageFieldRequest> fieldRequests = hiveSink.getStreamFields()
                    .stream()
                    .map(streamField -> {
                        StorageFieldRequest storageFieldRequest = new StorageFieldRequest();
                        storageFieldRequest.setFieldName(streamField.getFieldName());
                        storageFieldRequest.setFieldType(streamField.getFieldType().toString());
                        storageFieldRequest.setFieldComment(streamField.getFieldComment());
                        return storageFieldRequest;
                    })
                    .collect(Collectors.toList());
        }
        return hiveStorageRequest;
    }

    private static HiveSink parseHiveSink(HiveStorageResponse hiveStorage) {
        HiveSink hiveSink = new HiveSink();
        hiveSink.setSinkType(SinkType.HIVE);
        int asciiCode = Integer.parseInt(hiveStorage.getDataSeparator());
        hiveSink.setDataSeparator(DataSeparator.getByAscii(asciiCode));
        hiveSink.setCharset(Charset.forName(hiveStorage.getDataEncoding()));
        hiveSink.setDbName(hiveStorage.getDbName());
        hiveSink.setTableName(hiveStorage.getTableName());
        hiveSink.setFileFormat(FileFormat.forName(hiveStorage.getFileFormat()));
        hiveSink.setJdbcUrl(hiveStorage.getJdbcUrl());
        hiveSink.setHdfsDefaultFs(hiveStorage.getHdfsDefaultFs());
        hiveSink.setWarehouseDir(hiveStorage.getWarehouseDir());
        hiveSink.setAuthentication(new DefaultAuthentication(hiveStorage.getUsername(), hiveStorage.getPassword()));
        hiveSink.setNeedCreated(hiveStorage.getEnableCreateResource() == 1);
        hiveSink.setPrimaryPartition(hiveStorage.getPrimaryPartition());
        hiveSink.setSecondaryPartition(hiveStorage.getSecondaryPartition());
        if (CollectionUtils.isNotEmpty(hiveStorage.getFieldList())) {
            List<StreamField> fieldList = hiveStorage.getFieldList()
                    .stream()
                    .map(storageFieldRequest -> {
                        return new StreamField(storageFieldRequest.getId(),
                                FieldType.forName(storageFieldRequest.getFieldType()),
                                storageFieldRequest.getFieldName(),
                                storageFieldRequest.getFieldComment(),
                                null);
                    }).collect(Collectors.toList());
            hiveSink.setStreamFields(fieldList);
        }
        return hiveSink;
    }

}
