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

import com.google.common.collect.Lists;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.inlong.manager.common.enums.DataSeparator;
import org.apache.inlong.manager.client.api.sink.ClickHouseSink;
import org.apache.inlong.manager.client.api.sink.HbaseSink;
import org.apache.inlong.manager.client.api.sink.HiveSink;
import org.apache.inlong.manager.client.api.sink.KafkaSink;
import org.apache.inlong.manager.common.auth.DefaultAuthentication;
import org.apache.inlong.manager.client.api.sink.PostgresSink;
import org.apache.inlong.manager.common.enums.DataFormat;
import org.apache.inlong.manager.common.enums.FieldType;
import org.apache.inlong.manager.common.enums.FileFormat;
import org.apache.inlong.manager.common.enums.GlobalConstants;
import org.apache.inlong.manager.common.enums.SinkType;
import org.apache.inlong.manager.common.pojo.sink.SinkFieldRequest;
import org.apache.inlong.manager.common.pojo.sink.SinkFieldResponse;
import org.apache.inlong.manager.common.pojo.sink.SinkRequest;
import org.apache.inlong.manager.common.pojo.sink.SinkResponse;
import org.apache.inlong.manager.common.pojo.sink.ck.ClickHouseSinkRequest;
import org.apache.inlong.manager.common.pojo.sink.ck.ClickHouseSinkResponse;
import org.apache.inlong.manager.common.pojo.sink.hbase.HbaseSinkRequest;
import org.apache.inlong.manager.common.pojo.sink.hbase.HbaseSinkResponse;
import org.apache.inlong.manager.common.pojo.sink.hive.HiveSinkRequest;
import org.apache.inlong.manager.common.pojo.sink.hive.HiveSinkResponse;
import org.apache.inlong.manager.common.pojo.sink.kafka.KafkaSinkRequest;
import org.apache.inlong.manager.common.pojo.sink.kafka.KafkaSinkResponse;
import org.apache.inlong.manager.common.pojo.sink.postgres.PostgresSinkRequest;
import org.apache.inlong.manager.common.pojo.sink.postgres.PostgresSinkResponse;
import org.apache.inlong.manager.common.pojo.stream.InlongStreamInfo;
import org.apache.inlong.manager.common.pojo.stream.SinkField;
import org.apache.inlong.manager.common.pojo.stream.StreamSink;
import org.apache.inlong.manager.common.util.AssertUtils;
import org.apache.inlong.manager.common.util.CommonBeanUtils;

import java.nio.charset.Charset;
import java.util.List;
import java.util.stream.Collectors;

public class InlongStreamSinkTransfer {

    /**
     * Create sink request
     */
    public static SinkRequest createSinkRequest(StreamSink streamSink, InlongStreamInfo streamInfo) {
        SinkType sinkType = streamSink.getSinkType();
        switch (sinkType) {
            case KAFKA:
                return createKafkaRequest(streamSink, streamInfo);
            case HIVE:
                return createHiveRequest(streamSink, streamInfo);
            case CLICKHOUSE:
                return createClickHouseRequest(streamSink, streamInfo);
            case HBASE:
                return createHbaseRequest(streamSink, streamInfo);
            case POSTGRES:
                return createPostgresRequest(streamSink, streamInfo);
            default:
                throw new IllegalArgumentException(String.format("Unsupported sink type : %s for Inlong", sinkType));
        }
    }

    public static StreamSink parseStreamSink(SinkResponse sinkResponse) {
        return parseStreamSink(sinkResponse, null);
    }

    /**
     * Parse stream sink
     */
    public static StreamSink parseStreamSink(SinkResponse sinkResponse, StreamSink streamSink) {
        String type = sinkResponse.getSinkType();
        SinkType sinkType = SinkType.forType(type);
        switch (sinkType) {
            case KAFKA:
                return parseKafkaSink((KafkaSinkResponse) sinkResponse, streamSink);
            case HIVE:
                return parseHiveSink((HiveSinkResponse) sinkResponse, streamSink);
            case CLICKHOUSE:
                return parseClickHouseSink((ClickHouseSinkResponse) sinkResponse, streamSink);
            case HBASE:
                return parseHbaseSink((HbaseSinkResponse) sinkResponse, streamSink);
            case POSTGRES:
                return parsePostgresSink((PostgresSinkResponse) sinkResponse, streamSink);
            default:
                throw new IllegalArgumentException(String.format("Unsupported sink type : %s for Inlong", sinkType));
        }
    }

    /**
     * Create clickHouse request.
     *
     * @param streamSink streamSink object
     * @param streamInfo inlongStreamInfo object
     * @return clickHouseSinkRequest object
     */
    private static SinkRequest createClickHouseRequest(StreamSink streamSink, InlongStreamInfo streamInfo) {
        ClickHouseSinkRequest clickHouseSinkRequest = new ClickHouseSinkRequest();
        ClickHouseSink clickHouseSink = (ClickHouseSink) streamSink;
        clickHouseSinkRequest.setSinkName(clickHouseSink.getSinkName());
        clickHouseSinkRequest.setDbName(clickHouseSink.getDbName());
        clickHouseSinkRequest.setSinkType(clickHouseSink.getSinkType().name());
        clickHouseSinkRequest.setJdbcUrl(clickHouseSink.getJdbcUrl());
        DefaultAuthentication defaultAuthentication = clickHouseSink.getAuthentication();
        AssertUtils.notNull(defaultAuthentication,
                String.format("Clickhouse storage:%s must be authenticated", clickHouseSink.getDbName()));
        clickHouseSinkRequest.setUsername(defaultAuthentication.getUserName());
        clickHouseSinkRequest.setPassword(defaultAuthentication.getPassword());
        clickHouseSinkRequest.setTableName(clickHouseSink.getTableName());
        clickHouseSinkRequest.setIsDistributed(clickHouseSink.getIsDistributed());
        clickHouseSinkRequest.setFlushInterval(clickHouseSink.getFlushInterval());
        clickHouseSinkRequest.setFlushRecord(clickHouseSink.getFlushRecord());
        clickHouseSinkRequest.setKeyFieldNames(clickHouseSink.getKeyFieldNames());
        clickHouseSinkRequest.setPartitionFields(clickHouseSink.getPartitionFields());
        clickHouseSinkRequest.setPartitionStrategy(clickHouseSink.getPartitionStrategy());
        clickHouseSinkRequest.setRetryTimes(clickHouseSink.getRetryTimes());
        clickHouseSinkRequest.setInlongGroupId(streamInfo.getInlongGroupId());
        clickHouseSinkRequest.setInlongStreamId(streamInfo.getInlongStreamId());
        clickHouseSinkRequest.setProperties(clickHouseSink.getProperties());
        clickHouseSinkRequest.setEnableCreateResource(clickHouseSink.isNeedCreated() ? 1 : 0);
        if (CollectionUtils.isNotEmpty(clickHouseSink.getSinkFields())) {
            List<SinkFieldRequest> fieldRequests = createSinkFieldRequests(streamSink.getSinkFields());
            clickHouseSinkRequest.setFieldList(fieldRequests);
        }
        return clickHouseSinkRequest;
    }

    /**
     * Parse clickHouse sink.
     *
     * @param sinkResponse kafkaSinkResponse object
     * @param streamSink streamSink object
     * @return clickHouseSink object
     */
    private static StreamSink parseClickHouseSink(ClickHouseSinkResponse sinkResponse,
            StreamSink streamSink) {
        ClickHouseSink clickHouseSink = new ClickHouseSink();
        if (streamSink != null) {
            AssertUtils.isTrue(sinkResponse.getSinkName().equals(streamSink.getSinkName()),
                    String.format("SinkName is not equal: %s != %s", sinkResponse, streamSink));
            ClickHouseSink snapshot = (ClickHouseSink) streamSink;
            clickHouseSink = CommonBeanUtils.copyProperties(snapshot, ClickHouseSink::new);
        } else {
            clickHouseSink.setIsDistributed(sinkResponse.getIsDistributed());
            clickHouseSink.setSinkName(sinkResponse.getSinkName());
            clickHouseSink.setFlushInterval(sinkResponse.getFlushInterval());
            clickHouseSink.setAuthentication(new DefaultAuthentication(sinkResponse.getSinkName(),
                    sinkResponse.getPassword()));
            clickHouseSink.setDbName(sinkResponse.getDbName());
            clickHouseSink.setFlushRecord(sinkResponse.getFlushRecord());
            clickHouseSink.setJdbcUrl(sinkResponse.getJdbcUrl());
            clickHouseSink.setPartitionFields(sinkResponse.getPartitionFields());
            clickHouseSink.setKeyFieldNames(sinkResponse.getKeyFieldNames());
            clickHouseSink.setPartitionStrategy(sinkResponse.getPartitionStrategy());
            clickHouseSink.setRetryTimes(sinkResponse.getRetryTimes());
            clickHouseSink.setIsDistributed(sinkResponse.getIsDistributed());
        }
        clickHouseSink.setProperties(sinkResponse.getProperties());
        clickHouseSink.setNeedCreated(
                GlobalConstants.ENABLE_CREATE_RESOURCE.equals(sinkResponse.getEnableCreateResource()));
        if (CollectionUtils.isNotEmpty(sinkResponse.getFieldList())) {
            clickHouseSink.setSinkFields(convertToSinkFields(sinkResponse.getFieldList()));
        }
        return clickHouseSink;
    }

    /**
     * Convert sinkFieldResponse to sinkField
     */
    private static List<SinkField> convertToSinkFields(List<SinkFieldResponse> sinkFieldResponses) {
        return sinkFieldResponses.stream().map(sinkFieldResponse -> new SinkField(sinkFieldResponse.getId(),
                FieldType.forName(sinkFieldResponse.getFieldType()),
                sinkFieldResponse.getFieldName(),
                sinkFieldResponse.getFieldComment(),
                null, sinkFieldResponse.getSourceFieldName(),
                StringUtils.isBlank(sinkFieldResponse.getSourceFieldType()) ? null :
                        FieldType.forName(sinkFieldResponse.getSourceFieldType()),
                sinkFieldResponse.getIsMetaField(),
                sinkFieldResponse.getFieldFormat())).collect(Collectors.toList());
    }

    /**
     * Create kafka request.
     *
     * @param streamSink streamSink object
     * @param streamInfo inlongStreamInfo object
     * @return kafkaSinkRequest object
     */
    private static SinkRequest createKafkaRequest(StreamSink streamSink, InlongStreamInfo streamInfo) {
        KafkaSinkRequest kafkaSinkRequest = new KafkaSinkRequest();
        KafkaSink kafkaSink = (KafkaSink) streamSink;
        kafkaSinkRequest.setSinkName(streamSink.getSinkName());
        kafkaSinkRequest.setBootstrapServers(kafkaSink.getBootstrapServers());
        kafkaSinkRequest.setTopicName(kafkaSink.getTopicName());
        kafkaSinkRequest.setSinkType(kafkaSink.getSinkType().name());
        kafkaSinkRequest.setInlongGroupId(streamInfo.getInlongGroupId());
        kafkaSinkRequest.setInlongStreamId(streamInfo.getInlongStreamId());
        kafkaSinkRequest.setSerializationType(kafkaSink.getDataFormat().name());
        kafkaSinkRequest.setEnableCreateResource(kafkaSink.isNeedCreated() ? 1 : 0);
        kafkaSinkRequest.setProperties(kafkaSink.getProperties());
        kafkaSinkRequest.setPrimaryKey(kafkaSink.getPrimaryKey());
        if (CollectionUtils.isNotEmpty(kafkaSink.getSinkFields())) {
            List<SinkFieldRequest> fieldRequests = createSinkFieldRequests(kafkaSink.getSinkFields());
            kafkaSinkRequest.setFieldList(fieldRequests);
        }
        return kafkaSinkRequest;
    }

    /**
     * Parse kafka sink.
     *
     * @param sinkResponse kafkaSinkResponse object
     * @param sink streamSink object
     * @return kafkaSink object
     */
    private static StreamSink parseKafkaSink(KafkaSinkResponse sinkResponse, StreamSink sink) {
        KafkaSink kafkaSink = new KafkaSink();
        if (sink != null) {
            AssertUtils.isTrue(sinkResponse.getSinkName().equals(sink.getSinkName()),
                    String.format("SinkName is not equal: %s != %s", sinkResponse, sink));
            KafkaSink snapshot = (KafkaSink) sink;
            kafkaSink.setSinkName(snapshot.getSinkName());
            kafkaSink.setBootstrapServers(snapshot.getBootstrapServers());
            kafkaSink.setTopicName(snapshot.getTopicName());
            kafkaSink.setDataFormat(snapshot.getDataFormat());
        } else {
            kafkaSink.setSinkName(sinkResponse.getSinkName());
            kafkaSink.setBootstrapServers(sinkResponse.getBootstrapServers());
            kafkaSink.setTopicName(sinkResponse.getTopicName());
            kafkaSink.setDataFormat(DataFormat.forName(sinkResponse.getSerializationType()));
        }
        kafkaSink.setPrimaryKey(sinkResponse.getPrimaryKey());
        kafkaSink.setProperties(sinkResponse.getProperties());
        kafkaSink.setNeedCreated(GlobalConstants.ENABLE_CREATE_RESOURCE.equals(sinkResponse.getEnableCreateResource()));
        if (CollectionUtils.isNotEmpty(sinkResponse.getFieldList())) {
            kafkaSink.setSinkFields(convertToSinkFields(sinkResponse.getFieldList()));
        }
        return kafkaSink;
    }

    /**
     * Create habse request.
     *
     * @param streamSink streamSink object
     * @param streamInfo inlongStreamInfo object
     * @return hbaseSinkRequest object
     */
    private static SinkRequest createHbaseRequest(StreamSink streamSink, InlongStreamInfo streamInfo) {
        HbaseSinkRequest hbaseSinkRequest = new HbaseSinkRequest();
        HbaseSink hbaseSink = (HbaseSink) streamSink;
        hbaseSinkRequest.setSinkName(streamSink.getSinkName());
        hbaseSinkRequest.setInlongGroupId(streamInfo.getInlongGroupId());
        hbaseSinkRequest.setInlongStreamId(streamInfo.getInlongStreamId());
        hbaseSinkRequest.setNamespace(hbaseSink.getNamespace());
        hbaseSinkRequest.setTableName(hbaseSink.getTableName());
        hbaseSinkRequest.setRowKey(hbaseSink.getRowKey());
        hbaseSinkRequest.setZookeeperQuorum(hbaseSink.getZookeeperQuorum());
        hbaseSinkRequest.setZookeeperZnodeParent(hbaseSink.getZookeeperZnodeParent());
        hbaseSinkRequest.setSinkBufferFlushInterval(hbaseSink.getSinkBufferFlushInterval());
        hbaseSinkRequest.setSinkBufferFlushMaxRows(hbaseSink.getSinkBufferFlushMaxRows());
        hbaseSinkRequest.setSinkBufferFlushMaxSize(hbaseSink.getSinkBufferFlushMaxSize());
        hbaseSinkRequest.setSinkType(hbaseSink.getSinkType().name());
        hbaseSinkRequest.setEnableCreateResource(hbaseSink.isNeedCreated() ? 1 : 0);
        hbaseSinkRequest.setProperties(hbaseSink.getProperties());
        if (CollectionUtils.isNotEmpty(hbaseSink.getSinkFields())) {
            List<SinkFieldRequest> fieldRequests = createSinkFieldRequests(hbaseSink.getSinkFields());
            hbaseSinkRequest.setFieldList(fieldRequests);
        }
        return hbaseSinkRequest;

    }

    /**
     * Parse hbase sink.
     *
     * @param sinkResponse hbaseSinkResponse object
     * @param sink streamSink object
     * @return hbaseSink object
     */
    private static StreamSink parseHbaseSink(HbaseSinkResponse sinkResponse, StreamSink sink) {
        HbaseSink hbaseSink = new HbaseSink();
        if (sink != null) {
            AssertUtils.isTrue(sinkResponse.getSinkName().equals(sink.getSinkName()),
                    String.format("SinkName is not equal: %s != %s", sinkResponse, sink));
            HbaseSink snapshot = (HbaseSink) sink;
            hbaseSink.setSinkName(snapshot.getSinkName());
            hbaseSink.setNamespace(snapshot.getNamespace());
            hbaseSink.setTableName(snapshot.getTableName());
            hbaseSink.setRowKey(snapshot.getRowKey());
            hbaseSink.setZookeeperQuorum(snapshot.getZookeeperQuorum());
            hbaseSink.setZookeeperZnodeParent(snapshot.getZookeeperZnodeParent());
            hbaseSink.setSinkBufferFlushInterval(snapshot.getSinkBufferFlushInterval());
            hbaseSink.setSinkBufferFlushMaxRows(snapshot.getSinkBufferFlushMaxRows());
            hbaseSink.setSinkBufferFlushMaxSize(snapshot.getSinkBufferFlushMaxSize());
        } else {
            hbaseSink.setSinkName(sinkResponse.getSinkName());
            hbaseSink.setNamespace(sinkResponse.getNamespace());
            hbaseSink.setTableName(sinkResponse.getTableName());
            hbaseSink.setRowKey(sinkResponse.getRowKey());
            hbaseSink.setZookeeperQuorum(sinkResponse.getZookeeperQuorum());
            hbaseSink.setZookeeperZnodeParent(sinkResponse.getZookeeperZnodeParent());
            hbaseSink.setSinkBufferFlushInterval(sinkResponse.getSinkBufferFlushInterval());
            hbaseSink.setSinkBufferFlushMaxRows(sinkResponse.getSinkBufferFlushMaxRows());
            hbaseSink.setSinkBufferFlushMaxSize(sinkResponse.getSinkBufferFlushMaxSize());
        }
        hbaseSink.setProperties(sinkResponse.getProperties());
        hbaseSink.setSinkType(SinkType.HBASE);
        hbaseSink.setNeedCreated(GlobalConstants.ENABLE_CREATE_RESOURCE.equals(sinkResponse.getEnableCreateResource()));
        if (CollectionUtils.isNotEmpty(sinkResponse.getFieldList())) {
            hbaseSink.setSinkFields(convertToSinkFields(sinkResponse.getFieldList()));
        }
        return hbaseSink;
    }

    /**
     * Create hive request.
     *
     * @param streamSink streamSink object
     * @param streamInfo inlongStreamInfo object
     * @return hiveSinkRequest object
     */
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
        hiveSinkRequest.setDataPath(hiveSink.getDataPath());
        hiveSinkRequest.setJdbcUrl(hiveSink.getJdbcUrl());
        hiveSinkRequest.setFileFormat(hiveSink.getFileFormat().name());
        hiveSinkRequest.setSinkType(hiveSink.getSinkType().name());
        hiveSinkRequest.setPartitionFieldList(hiveSink.getPartitionFieldList());
        hiveSinkRequest.setHiveConfDir(hiveSink.getHiveConfDir());
        hiveSinkRequest.setHiveVersion(hiveSink.getHiveVersion());
        DefaultAuthentication defaultAuthentication = hiveSink.getAuthentication();
        AssertUtils.notNull(defaultAuthentication,
                String.format("Hive storage:%s must be authenticated", hiveSink.getDbName()));
        hiveSinkRequest.setUsername(defaultAuthentication.getUserName());
        hiveSinkRequest.setPassword(defaultAuthentication.getPassword());
        hiveSinkRequest.setProperties(hiveSink.getProperties());
        if (CollectionUtils.isNotEmpty(hiveSink.getSinkFields())) {
            List<SinkFieldRequest> fieldRequests = createSinkFieldRequests(streamSink.getSinkFields());
            hiveSinkRequest.setFieldList(fieldRequests);
        }
        return hiveSinkRequest;
    }

    /**
     * Create sink field requests
     */
    private static List<SinkFieldRequest> createSinkFieldRequests(List<SinkField> sinkFields) {
        List<SinkFieldRequest> fieldRequestList = Lists.newArrayList();
        for (SinkField sinkField : sinkFields) {
            SinkFieldRequest request = new SinkFieldRequest();
            request.setFieldName(sinkField.getFieldName());
            request.setFieldType(sinkField.getFieldType().toString());
            request.setFieldComment(sinkField.getFieldComment());
            request.setSourceFieldName(sinkField.getSourceFieldName());
            request.setSourceFieldType(
                    sinkField.getSourceFieldType() == null ? null : sinkField.getSourceFieldType().toString());
            request.setIsMetaField(sinkField.getIsMetaField());
            request.setFieldFormat(sinkField.getFieldFormat());
            fieldRequestList.add(request);
        }
        return fieldRequestList;
    }

    /**
     * Parse hive sink.
     *
     * @param sinkResponse hiveSinkResponse object
     * @param sink streamSink object
     * @return hiveSink object
     */
    private static HiveSink parseHiveSink(HiveSinkResponse sinkResponse, StreamSink sink) {
        HiveSink hiveSink = new HiveSink();
        if (sink != null) {
            AssertUtils.isTrue(sinkResponse.getSinkName().equals(sink.getSinkName()),
                    String.format("SinkName is not equal: %s != %s", sinkResponse, sink));
            HiveSink snapshot = (HiveSink) sink;
            hiveSink.setSinkName(snapshot.getSinkName());
            hiveSink.setDataSeparator(snapshot.getDataSeparator());
            hiveSink.setCharset(snapshot.getCharset());
            hiveSink.setAuthentication(snapshot.getAuthentication());
            hiveSink.setFileFormat(snapshot.getFileFormat());
            hiveSink.setJdbcUrl(snapshot.getJdbcUrl());
            hiveSink.setTableName(snapshot.getTableName());
            hiveSink.setDbName(snapshot.getDbName());
            hiveSink.setDataPath(snapshot.getDataPath());
            hiveSink.setHiveVersion(snapshot.getHiveVersion());
            hiveSink.setHiveConfDir(snapshot.getHiveConfDir());
            hiveSink.setPartitionFieldList(snapshot.getPartitionFieldList());
        } else {
            hiveSink.setSinkName(sinkResponse.getSinkName());
            hiveSink.setDataSeparator(DataSeparator.forAscii(Integer.parseInt(sinkResponse.getDataSeparator())));
            hiveSink.setCharset(Charset.forName(sinkResponse.getDataEncoding()));
            String password = sinkResponse.getPassword();
            String uname = sinkResponse.getUsername();
            hiveSink.setAuthentication(new DefaultAuthentication(uname, password));
            hiveSink.setFileFormat(FileFormat.forName(sinkResponse.getFileFormat()));
            hiveSink.setJdbcUrl(sinkResponse.getJdbcUrl());
            hiveSink.setTableName(sinkResponse.getTableName());
            hiveSink.setDbName(sinkResponse.getDbName());
            hiveSink.setDataPath(sinkResponse.getDataPath());
            hiveSink.setHiveConfDir(sinkResponse.getHiveConfDir());
            hiveSink.setHiveVersion(sinkResponse.getHiveVersion());
            hiveSink.setPartitionFieldList(sinkResponse.getPartitionFieldList());
        }

        hiveSink.setProperties(sinkResponse.getProperties());
        hiveSink.setSinkType(SinkType.HIVE);
        hiveSink.setNeedCreated(GlobalConstants.ENABLE_CREATE_RESOURCE.equals(sinkResponse.getEnableCreateResource()));
        if (CollectionUtils.isNotEmpty(sinkResponse.getFieldList())) {
            hiveSink.setSinkFields(convertToSinkFields(sinkResponse.getFieldList()));
        }
        return hiveSink;
    }

    /**
     * Create postgres request
     *
     * @param streamSink streamSink
     * @param streamInfo streamInfo
     * @return postgres sinkRequest
     */
    private static SinkRequest createPostgresRequest(StreamSink streamSink, InlongStreamInfo streamInfo) {
        PostgresSinkRequest postgresSinkRequest = new PostgresSinkRequest();
        PostgresSink postgresSink = (PostgresSink) streamSink;
        postgresSinkRequest.setJdbcUrl(postgresSink.getJdbcUrl());
        postgresSinkRequest.setUsername(postgresSink.getAuthentication().getUserName());
        postgresSinkRequest.setPassword(postgresSink.getAuthentication().getPassword());
        postgresSinkRequest.setDbName(postgresSink.getDbName());
        postgresSinkRequest.setTableName(postgresSink.getTableName());

        postgresSinkRequest.setSinkName(postgresSink.getSinkName());
        postgresSinkRequest.setSinkType(postgresSink.getSinkType().name());
        postgresSinkRequest.setInlongGroupId(streamInfo.getInlongGroupId());
        postgresSinkRequest.setInlongStreamId(streamInfo.getInlongStreamId());

        postgresSinkRequest.setProperties(postgresSink.getProperties());
        postgresSinkRequest.setPrimaryKey(postgresSink.getPrimaryKey());

        if (CollectionUtils.isNotEmpty(postgresSink.getSinkFields())) {
            List<SinkFieldRequest> fieldRequests = createSinkFieldRequests(postgresSink.getSinkFields());
            postgresSinkRequest.setFieldList(fieldRequests);
        }
        return postgresSinkRequest;
    }

    /**
     * parse postgres sink
     * 
     * @param sinkResponse sinkResponse
     * @param sink sink
     * @return postgres streamSink
     */
    private static StreamSink parsePostgresSink(PostgresSinkResponse sinkResponse, StreamSink sink) {
        PostgresSink postgresSink = new PostgresSink();
        if (sink != null) {
            AssertUtils.isTrue(sinkResponse.getSinkName().equals(sink.getSinkName()),
                    String.format("SinkName is not equal: %s != %s", sinkResponse, sink));
            PostgresSink snapshot = (PostgresSink) sink;

            postgresSink.setSinkName(snapshot.getSinkName());
            postgresSink.setAuthentication(snapshot.getAuthentication());

            postgresSink.setJdbcUrl(snapshot.getJdbcUrl());
            postgresSink.setTableName(snapshot.getTableName());
            postgresSink.setDbName(snapshot.getDbName());
        } else {
            postgresSink.setSinkName(sinkResponse.getSinkName());
            String password = sinkResponse.getPassword();
            String uname = sinkResponse.getUsername();
            postgresSink.setAuthentication(new DefaultAuthentication(uname, password));

            postgresSink.setJdbcUrl(sinkResponse.getJdbcUrl());
            postgresSink.setTableName(sinkResponse.getTableName());
            postgresSink.setDbName(sinkResponse.getDbName());
        }
        postgresSink.setPrimaryKey(sinkResponse.getPrimaryKey());
        postgresSink.setProperties(sinkResponse.getProperties());
        postgresSink.setSinkType(SinkType.POSTGRES);
        if (CollectionUtils.isNotEmpty(sinkResponse.getFieldList())) {
            postgresSink.setSinkFields(convertToSinkFields(sinkResponse.getFieldList()));
        }
        return postgresSink;
    }
}
