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
import org.apache.inlong.manager.client.api.StreamSink;
import org.apache.inlong.manager.client.api.StreamSink.SinkType;
import org.apache.inlong.manager.client.api.DataStreamConf;
import org.apache.inlong.manager.client.api.HiveSink;
import org.apache.inlong.manager.client.api.StreamField;
import org.apache.inlong.manager.client.api.auth.DefaultAuthentication;
import org.apache.inlong.manager.common.pojo.business.BusinessInfo;
import org.apache.inlong.manager.common.pojo.datastorage.StorageRequest;
import org.apache.inlong.manager.common.pojo.datastorage.hive.HiveStorageRequest;
import org.apache.inlong.manager.common.pojo.datastream.DataStreamFieldInfo;
import org.apache.inlong.manager.common.pojo.datastream.DataStreamInfo;
import org.apache.shiro.util.Assert;

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

    public static StorageRequest createStorageRequest(StreamSink storage, DataStreamInfo streamInfo) {
        SinkType sinkType = storage.getSinkType();
        if (sinkType == SinkType.HIVE) {
            return createHiveRequest(storage, streamInfo);
        } else {
            throw new IllegalArgumentException(String.format("Unsupport storage type : %s for Inlong", sinkType));
        }
    }

    private static HiveStorageRequest createHiveRequest(StreamSink storage, DataStreamInfo streamInfo) {
        HiveStorageRequest hiveStorageRequest = new HiveStorageRequest();
        HiveSink hiveSink = (HiveSink) storage;
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
        Assert.notNull(defaultAuthentication,
                String.format("Hive storage:%s must be authenticated", hiveSink.getDbName()));
        hiveStorageRequest.setUsername(defaultAuthentication.getUserName());
        hiveStorageRequest.setPassword(defaultAuthentication.getPassword());
        hiveStorageRequest.setPrimaryPartition(hiveSink.getPrimaryPartition());
        hiveStorageRequest.setSecondaryPartition(hiveSink.getSecondaryPartition());
        return hiveStorageRequest;
    }

}
