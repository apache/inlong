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

package org.apache.inlong.manager.client.api.impl;

import com.github.pagehelper.PageInfo;
import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.client.WireMock;
import com.google.common.collect.Lists;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.inlong.manager.client.api.ClientConfiguration;
import org.apache.inlong.manager.client.api.inner.InnerInlongManagerClient;
import org.apache.inlong.manager.common.auth.DefaultAuthentication;
import org.apache.inlong.manager.common.beans.Response;
import org.apache.inlong.manager.common.pojo.group.InlongGroupExtInfo;
import org.apache.inlong.manager.common.pojo.group.InlongGroupInfo;
import org.apache.inlong.manager.common.pojo.group.InlongGroupListResponse;
import org.apache.inlong.manager.common.pojo.group.InlongGroupRequest;
import org.apache.inlong.manager.common.pojo.group.pulsar.InlongPulsarInfo;
import org.apache.inlong.manager.common.pojo.sink.SinkListResponse;
import org.apache.inlong.manager.common.pojo.sink.ck.ClickHouseSink;
import org.apache.inlong.manager.common.pojo.sink.ck.ClickHouseSinkListResponse;
import org.apache.inlong.manager.common.pojo.sink.es.ElasticsearchSinkListResponse;
import org.apache.inlong.manager.common.pojo.sink.hbase.HBaseSinkListResponse;
import org.apache.inlong.manager.common.pojo.sink.hive.HiveSink;
import org.apache.inlong.manager.common.pojo.sink.hive.HiveSinkListResponse;
import org.apache.inlong.manager.common.pojo.sink.iceberg.IcebergSink;
import org.apache.inlong.manager.common.pojo.sink.iceberg.IcebergSinkListResponse;
import org.apache.inlong.manager.common.pojo.sink.kafka.KafkaSink;
import org.apache.inlong.manager.common.pojo.sink.kafka.KafkaSinkListResponse;
import org.apache.inlong.manager.common.pojo.sink.postgres.PostgresSinkListResponse;
import org.apache.inlong.manager.common.pojo.source.SourceListResponse;
import org.apache.inlong.manager.common.pojo.source.autopush.AutoPushSource;
import org.apache.inlong.manager.common.pojo.source.autopush.AutoPushSourceListResponse;
import org.apache.inlong.manager.common.pojo.source.file.FileSource;
import org.apache.inlong.manager.common.pojo.source.file.FileSourceListResponse;
import org.apache.inlong.manager.common.pojo.source.kafka.KafkaSource;
import org.apache.inlong.manager.common.pojo.source.kafka.KafkaSourceListResponse;
import org.apache.inlong.manager.common.pojo.source.mysql.MySQLBinlogSource;
import org.apache.inlong.manager.common.pojo.source.mysql.MySQLBinlogSourceListResponse;
import org.apache.inlong.manager.common.pojo.stream.FullStreamResponse;
import org.apache.inlong.manager.common.pojo.stream.InlongStreamInfo;
import org.apache.inlong.manager.common.pojo.stream.InlongStreamResponse;
import org.apache.inlong.manager.common.pojo.stream.StreamField;
import org.apache.inlong.manager.common.util.JsonUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.Date;
import java.util.List;

import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.okJson;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.client.WireMock.stubFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlMatching;
import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.options;

/**
 * Test class for InnerInlongManagerClientTest.
 */
@Slf4j
class InnerInlongManagerClientTest {

    public static WireMockServer wireMockServer;
    public static InnerInlongManagerClient innerInlongManagerClient;

    @BeforeAll
    static void setup() {
        wireMockServer = new WireMockServer(
                options()
                        .port(8084)
        );
        wireMockServer.start();
        WireMock.configureFor(wireMockServer.port());

        String serviceUrl = "127.0.0.1:8084";
        ClientConfiguration configuration = new ClientConfiguration();
        configuration.setAuthentication(new DefaultAuthentication("admin", "inlong"));
        InlongClientImpl inlongClient = new InlongClientImpl(serviceUrl, configuration);
        innerInlongManagerClient = new InnerInlongManagerClient(
                inlongClient.getConfiguration());
    }

    @AfterAll
    static void teardown() {
        wireMockServer.stop();
    }

    @Test
    void testGroupExist() {
        stubFor(
                get(urlMatching("/api/inlong/manager/group/exist/123.*"))
                        .willReturn(
                                okJson(
                                        JsonUtils.toJsonString(Response.success(true))
                                )
                        )
        );
        Boolean groupExists = innerInlongManagerClient.isGroupExists("123");

        Assertions.assertTrue(groupExists);
    }

    @Test
    void testGetGroupInfo_pulsar() {
        InlongPulsarInfo inlongGroupResponse = InlongPulsarInfo.builder()
                .id(1)
                .inlongGroupId("1")
                .mqType("PULSAR")
                .mqResource("mq resource")

                .enableCreateResource(1)
                .lightweight(1)
                .extList(
                        Lists.newArrayList(
                                InlongGroupExtInfo.builder()
                                        .id(1)
                                        .inlongGroupId("1")
                                        .keyName("keyName")
                                        .keyValue("keyValue")
                                        .build()
                        )
                )
                .build();

        stubFor(
                get(urlMatching("/api/inlong/manager/group/get/1.*"))
                        .willReturn(
                                okJson(
                                        JsonUtils.toJsonString(
                                                Response.success(inlongGroupResponse)
                                        )
                                )
                        )
        );

        InlongGroupInfo groupInfo = innerInlongManagerClient.getGroupInfo("1");

        Assertions.assertTrue(groupInfo instanceof InlongPulsarInfo);
        Assertions.assertEquals(JsonUtils.toJsonString(inlongGroupResponse), JsonUtils.toJsonString(groupInfo));
    }

    @Test
    void testGetGroupInfo_tdmqPulsar_failAndREturnNull() {
        stubFor(
                get(urlMatching("/api/inlong/manager/group/get/33333.*"))
                        .willReturn(
                                okJson(
                                        JsonUtils.toJsonString(
                                                Response.fail("Inlong group does not exist/no operation authority")
                                        )
                                )
                        )
        );

        Assertions.assertNull(innerInlongManagerClient.getGroupInfo("333333"));
    }

    @Test
    void testListGroup_autoPush() {
        List<InlongGroupListResponse> groupListResponses = Lists.newArrayList(
                InlongGroupListResponse.builder()
                        .id(1)
                        .inlongGroupId("1")
                        .name("name")
                        .modifyTime(new Date())
                        .sourceResponses(
                                Lists.newArrayList(
                                        AutoPushSourceListResponse.builder()
                                                .id(22)
                                                .inlongGroupId("1")
                                                .inlongStreamId("2")
                                                .sourceType("AUTO_PUSH")

                                                .dataProxyGroup("111")
                                                .build()
                                )
                        )
                        .build()
        );

        stubFor(
                post(urlMatching("/api/inlong/manager/group/list.*"))
                        .willReturn(
                                okJson(
                                        JsonUtils.toJsonString(
                                                Response.success(
                                                        new PageInfo<>(groupListResponses)
                                                )
                                        )
                                )
                        )
        );

        PageInfo<InlongGroupListResponse> listResponse = innerInlongManagerClient.listGroups("keyword", 1, 1, 10);

        Assertions.assertEquals(JsonUtils.toJsonString(groupListResponses),
                JsonUtils.toJsonString(listResponse.getList()));
    }

    @Test
    void testListGroup_binlog() {
        List<InlongGroupListResponse> groupListResponses = Lists.newArrayList(
                InlongGroupListResponse.builder()
                        .id(1)
                        .inlongGroupId("1")
                        .name("name")
                        .sourceResponses(
                                Lists.newArrayList(
                                        MySQLBinlogSourceListResponse.builder()
                                                .id(22)
                                                .inlongGroupId("1")
                                                .inlongStreamId("2")
                                                .sourceType("BINLOG")
                                                .clusterId(1)
                                                .status(1)

                                                .user("root")
                                                .password("pwd")
                                                .databaseWhiteList("")
                                                .build()
                                )
                        )
                        .build()
        );

        stubFor(
                post(urlMatching("/api/inlong/manager/group/list.*"))
                        .willReturn(
                                okJson(
                                        JsonUtils.toJsonString(
                                                Response.success(
                                                        new PageInfo<>(groupListResponses)
                                                )
                                        )
                                )
                        )

        );

        PageInfo<InlongGroupListResponse> listResponse = innerInlongManagerClient.listGroups("keyword", 1, 1, 10);

        Assertions.assertEquals(JsonUtils.toJsonString(groupListResponses),
                JsonUtils.toJsonString(listResponse.getList()));
    }

    @Test
    void testListGroup_binlog2() {
        stubFor(
                post(urlMatching("/api/inlong/manager/group/list.*"))
                        .willReturn(
                                okJson(
                                        "{\n"
                                                + "  \"success\" : true,\n"
                                                + "  \"errMsg\" : null,\n"
                                                + "  \"data\" : {\n"
                                                + "    \"total\" : 1,\n"
                                                + "    \"list\" : [ {\n"
                                                + "      \"id\" : 1,\n"
                                                + "      \"inlongGroupId\" : \"1\",\n"
                                                + "      \"name\" : \"name\",\n"
                                                + "      \"inCharges\" : null,\n"
                                                + "      \"status\" : null,\n"
                                                + "      \"createTime\" : null,\n"
                                                + "      \"modifyTime\" : null,\n"
                                                + "      \"sourceResponses\" : [ {\n"
                                                + "        \"sourceType\" : \"BINLOG\",\n"
                                                + "        \"id\" : 22,\n"
                                                + "        \"inlongGroupId\" : \"1\",\n"
                                                + "        \"inlongStreamId\" : \"2\",\n"
                                                + "        \"sourceType\" : \"BINLOG\",\n"
                                                + "        \"sourceName\" : null,\n"
                                                + "        \"serializationType\" : null,\n"
                                                + "        \"dataNodeName\" : null,\n"
                                                + "        \"clusterId\" : 1,\n"
                                                + "        \"status\" : 1,\n"
                                                + "        \"version\" : null,\n"
                                                + "        \"createTime\" : null,\n"
                                                + "        \"modifyTime\" : null,\n"
                                                + "        \"user\" : \"root\",\n"
                                                + "        \"password\" : \"pwd\",\n"
                                                + "        \"hostname\" : null,\n"
                                                + "        \"port\" : 0,\n"
                                                + "        \"serverId\" : null,\n"
                                                + "        \"includeSchema\" : null,\n"
                                                + "        \"databaseWhiteList\" : \"\",\n"
                                                + "        \"tableWhiteList\" : null,\n"
                                                + "        \"serverTimezone\" : null,\n"
                                                + "        \"intervalMs\" : null,\n"
                                                + "        \"snapshotMode\" : null,\n"
                                                + "        \"offsetFilename\" : null,\n"
                                                + "        \"historyFilename\" : null,\n"
                                                + "        \"monitoredDdl\" : null,\n"
                                                + "        \"timestampFormatStandard\" : null,\n"
                                                + "        \"allMigration\" : false,\n"
                                                + "        \"primaryKey\" : null\n"
                                                + "      } ]\n"
                                                + "    } ],\n"
                                                + "    \"pageNum\" : 1,\n"
                                                + "    \"pageSize\" : 1,\n"
                                                + "    \"size\" : 1,\n"
                                                + "    \"startRow\" : 0,\n"
                                                + "    \"endRow\" : 0,\n"
                                                + "    \"pages\" : 1,\n"
                                                + "    \"prePage\" : 0,\n"
                                                + "    \"nextPage\" : 0,\n"
                                                + "    \"isFirstPage\" : true,\n"
                                                + "    \"isLastPage\" : true,\n"
                                                + "    \"hasPreviousPage\" : false,\n"
                                                + "    \"hasNextPage\" : false,\n"
                                                + "    \"navigatePages\" : 8,\n"
                                                + "    \"navigatepageNums\" : [ 1 ],\n"
                                                + "    \"navigateFirstPage\" : 1,\n"
                                                + "    \"navigateLastPage\" : 1\n"
                                                + "  }\n"
                                                + "}"
                                )
                        )
        );

        PageInfo<InlongGroupListResponse> listResponse = innerInlongManagerClient.listGroups("keyword", 1, 1, 10);

        SourceListResponse sourceListResponse = listResponse.getList()
                .get(0)
                .getSourceResponses()
                .get(0);
        Assertions.assertTrue(sourceListResponse instanceof MySQLBinlogSourceListResponse);
    }

    @Test
    void testListGroup_file() {
        List<InlongGroupListResponse> groupListResponses = Lists.newArrayList(
                InlongGroupListResponse.builder()
                        .id(1)
                        .inlongGroupId("1")
                        .name("name")
                        .inCharges("inCharges")
                        .status(1)
                        .createTime(new Date())
                        .modifyTime(new Date())
                        .sourceResponses(
                                Lists.newArrayList(
                                        FileSourceListResponse.builder()
                                                .id(22)
                                                .inlongGroupId("1")
                                                .inlongStreamId("2")
                                                .sourceType("FILE")
                                                .status(1)

                                                .ip("127.0.0.1")
                                                .pattern("pattern")
                                                .build()
                                )
                        )
                        .build()
        );

        stubFor(
                post(urlMatching("/api/inlong/manager/group/list.*"))
                        .willReturn(
                                okJson(
                                        JsonUtils.toJsonString(
                                                Response.success(
                                                        new PageInfo<>(groupListResponses)
                                                )
                                        )
                                )
                        )
        );

        PageInfo<InlongGroupListResponse> listResponse = innerInlongManagerClient.listGroups("keyword", 1, 1, 10);

        Assertions.assertEquals(JsonUtils.toJsonString(groupListResponses),
                JsonUtils.toJsonString(listResponse.getList()));
    }

    @Test
    void testListGroup_kafka() {
        List<InlongGroupListResponse> groupListResponses = Lists.newArrayList(
                InlongGroupListResponse.builder()
                        .id(1)
                        .inlongGroupId("1")
                        .status(1)
                        .sourceResponses(
                                Lists.newArrayList(
                                        KafkaSourceListResponse.builder()
                                                .id(22)
                                                .inlongGroupId("1")
                                                .inlongStreamId("2")
                                                .sourceType("KAFKA")
                                                .dataNodeName("dataNodeName")
                                                .clusterId(1)
                                                .status(1)
                                                .version(1)
                                                .createTime(new Date())
                                                .modifyTime(new Date())

                                                .topic("topic")
                                                .groupId("111")
                                                .bootstrapServers("bootstrapServers")
                                                .recordSpeedLimit("recordSpeedLimit")
                                                .primaryKey("primaryKey")
                                                .build()
                                )
                        )
                        .build()
        );

        stubFor(
                post(urlMatching("/api/inlong/manager/group/list.*"))
                        .willReturn(
                                okJson(
                                        JsonUtils.toJsonString(
                                                Response.success(
                                                        new PageInfo<>(groupListResponses)
                                                )
                                        )
                                )
                        )
        );

        PageInfo<InlongGroupListResponse> listResponse = innerInlongManagerClient.listGroups("keyword", 1, 1, 10);

        Assertions.assertEquals(JsonUtils.toJsonString(groupListResponses),
                JsonUtils.toJsonString(listResponse.getList()));
    }

    @Test
    void testListGroup_allType() {
        List<InlongGroupListResponse> groupListResponses = Lists.newArrayList(
                InlongGroupListResponse.builder()
                        .id(1)
                        .inlongGroupId("1")
                        .name("name")
                        .inCharges("inCharges")
                        .sourceResponses(
                                Lists.newArrayList(
                                        AutoPushSourceListResponse.builder()
                                                .id(22)
                                                .inlongGroupId("1")
                                                .inlongStreamId("2")
                                                .sourceType("AUTO_PUSH")
                                                .version(1)

                                                .dataProxyGroup("111")
                                                .build(),

                                        MySQLBinlogSourceListResponse.builder()
                                                .id(22)
                                                .inlongGroupId("1")
                                                .inlongStreamId("2")
                                                .sourceType("BINLOG")

                                                .user("root")
                                                .password("pwd")
                                                .hostname("localhost")
                                                .includeSchema("false")
                                                .databaseWhiteList("")
                                                .tableWhiteList("")
                                                .build(),

                                        FileSourceListResponse.builder()
                                                .id(22)
                                                .inlongGroupId("1")
                                                .inlongStreamId("2")
                                                .version(1)

                                                .ip("127.0.0.1")
                                                .pattern("pattern")
                                                .timeOffset("timeOffset")
                                                .build(),

                                        KafkaSourceListResponse.builder()
                                                .id(22)
                                                .inlongGroupId("1")
                                                .inlongStreamId("2")
                                                .sourceType("KAFKA")
                                                .sourceName("source name")
                                                .serializationType("csv")
                                                .dataNodeName("dataNodeName")

                                                .topic("topic")
                                                .groupId("111")
                                                .bootstrapServers("bootstrapServers")
                                                .recordSpeedLimit("recordSpeedLimit")
                                                .primaryKey("primaryKey")
                                                .build()
                                )
                        )
                        .build()
        );

        stubFor(
                post(urlMatching("/api/inlong/manager/group/list.*"))
                        .willReturn(
                                okJson(
                                        JsonUtils.toJsonString(
                                                Response.success(
                                                        new PageInfo<>(groupListResponses)
                                                )
                                        )
                                )
                        )
        );

        PageInfo<InlongGroupListResponse> listResponse = innerInlongManagerClient.listGroups("keyword", 1, 1, 10);

        Assertions.assertEquals(JsonUtils.toJsonString(groupListResponses),
                JsonUtils.toJsonString(listResponse.getList()));
    }

    @Test
    void testListGroup_null_groupNotExist() {
        stubFor(
                post(urlMatching("/api/inlong/manager/group/list.*"))
                        .willReturn(
                                okJson(
                                        JsonUtils.toJsonString(
                                                Response.fail("Inlong group does not exist/no operation authority")
                                        )
                                )
                        )
        );

        PageInfo<InlongGroupListResponse> listResponse = innerInlongManagerClient.listGroups("keyword", 1, 1, 10);

        Assertions.assertNull(listResponse);
    }

    @Test
    void testCreateGroup() {
        stubFor(
                post(urlMatching("/api/inlong/manager/group/save.*"))
                        .willReturn(
                                okJson(
                                        JsonUtils.toJsonString(
                                                Response.success("1111")
                                        )
                                )
                        )
        );

        String groupId = innerInlongManagerClient.createGroup(new InlongGroupRequest());

        Assertions.assertEquals("1111", groupId);
    }

    @Test
    void testUpdateGroup() {
        stubFor(
                post(urlMatching("/api/inlong/manager/group/update.*"))
                        .willReturn(
                                okJson(
                                        JsonUtils.toJsonString(
                                                Response.success("1111")
                                        )
                                )
                        )
        );

        Pair<String, String> updateGroup = innerInlongManagerClient.updateGroup(new InlongGroupRequest());

        Assertions.assertEquals("1111", updateGroup.getKey());
        Assertions.assertTrue(StringUtils.isBlank(updateGroup.getValue()));
    }

    @Test
    void testCreateStreamInfo() {
        stubFor(
                post(urlMatching("/api/inlong/manager/stream/save.*"))
                        .willReturn(
                                okJson(
                                        JsonUtils.toJsonString(
                                                Response.success(11)
                                        )
                                )
                        )
        );

        Integer groupId = innerInlongManagerClient.createStreamInfo(new InlongStreamInfo());

        Assertions.assertEquals(11, groupId);
    }

    @Test
    void testStreamExist() {
        stubFor(
                get(urlMatching("/api/inlong/manager/stream/exist/123/11.*"))
                        .willReturn(
                                okJson(
                                        JsonUtils.toJsonString(Response.success(true))
                                )
                        )
        );

        InlongStreamInfo streamInfo = new InlongStreamInfo();
        streamInfo.setInlongGroupId("123");
        streamInfo.setInlongStreamId("11");
        Boolean groupExists = innerInlongManagerClient.isStreamExists(streamInfo);

        Assertions.assertTrue(groupExists);
    }

    @Test
    void testGetStreamInfo() {
        InlongStreamResponse streamResponse = InlongStreamResponse.builder()
                .id(1)
                .inlongGroupId("123")
                .inlongStreamId("11")
                .name("name")
                .fieldList(
                        Lists.newArrayList(
                                StreamField.builder()
                                        .id(1)
                                        .inlongGroupId("123")
                                        .fieldType("fieldType")
                                        .build(),
                                StreamField.builder()
                                        .id(2)
                                        .inlongGroupId("123")
                                        .inlongGroupId("11")
                                        .isMetaField(1)
                                        .build()
                        )
                )
                .build();

        stubFor(
                get(urlMatching("/api/inlong/manager/stream/get.*"))
                        .willReturn(
                                okJson(
                                        JsonUtils.toJsonString(Response.success(streamResponse))
                                )
                        )
        );

        InlongStreamInfo request = new InlongStreamInfo();
        request.setInlongGroupId("123");
        request.setInlongStreamId("11");
        InlongStreamInfo inlongStreamInfo = innerInlongManagerClient.getStreamInfo(request);

        Assertions.assertNotNull(inlongStreamInfo);
    }

    @Test
    void testGetStreamInfo_null_notExist() {
        stubFor(
                get(urlMatching("/api/inlong/manager/stream/get.*"))
                        .willReturn(
                                okJson(
                                        JsonUtils.toJsonString(
                                                Response.fail("Inlong stream does not exist/no operation permission"))
                                )
                        )
        );

        InlongStreamInfo request = new InlongStreamInfo();
        request.setInlongGroupId("123");
        request.setInlongStreamId("11");
        InlongStreamInfo inlongStreamInfo = innerInlongManagerClient.getStreamInfo(request);

        Assertions.assertNull(inlongStreamInfo);
    }

    @Test
    void testListStreamInfo_allType() {
        FullStreamResponse fullStreamResponse = FullStreamResponse.builder()
                .streamInfo(
                        InlongStreamInfo.builder()
                                .id(1)
                                .inlongGroupId("11")
                                .inlongStreamId("11")
                                .fieldList(
                                        Lists.newArrayList(
                                                StreamField.builder()
                                                        .id(1)
                                                        .inlongGroupId("123")
                                                        .inlongGroupId("11")
                                                        .build(),
                                                StreamField.builder()
                                                        .id(2)
                                                        .isMetaField(1)
                                                        .fieldFormat("yyyy-MM-dd HH:mm:ss")
                                                        .build()
                                        )
                                )
                                .build()
                )
                .sourceInfo(
                        Lists.newArrayList(
                                AutoPushSource.builder()
                                        .id(1)
                                        .inlongStreamId("11")
                                        .inlongGroupId("11")
                                        .sourceType("AUTO_PUSH")
                                        .createTime(new Date())

                                        .dataProxyGroup("111")
                                        .build(),
                                MySQLBinlogSource.builder()
                                        .id(2)
                                        .sourceType("BINLOG")

                                        .user("user")
                                        .password("pwd")
                                        .build(),
                                FileSource.builder()
                                        .id(3)
                                        .sourceType("FILE")
                                        .agentIp("127.0.0.1")

                                        .pattern("pattern")
                                        .build(),
                                KafkaSource.builder()
                                        .id(4)
                                        .sourceType("KAFKA")

                                        .autoOffsetReset("11")
                                        .bootstrapServers("10.110.221.22")
                                        .build()
                        )
                )
                .sinkInfo(
                        Lists.newArrayList(
                                HiveSink.builder()
                                        .sinkType("HIVE")
                                        .id(1)

                                        .jdbcUrl("127.0.0.1")
                                        .build(),
                                ClickHouseSink.builder()
                                        .sinkType("CLICKHOUSE")
                                        .id(2)

                                        .flushInterval(11)
                                        .build(),
                                IcebergSink.builder()
                                        .sinkType("ICEBERG")
                                        .id(3)

                                        .dataPath("hdfs://aabb")
                                        .build(),
                                KafkaSink.builder()
                                        .sinkType("KAFKA")
                                        .id(4)

                                        .bootstrapServers("127.0.0.1")
                                        .build()
                        )
                )
                .build();

        stubFor(
                post(urlMatching("/api/inlong/manager/stream/listAll.*"))
                        .willReturn(
                                okJson(
                                        JsonUtils.toJsonString(
                                                Response.success(
                                                        new PageInfo<>(Lists.newArrayList(fullStreamResponse)))
                                        )
                                )
                        )
        );

        List<FullStreamResponse> fullStreamResponses = innerInlongManagerClient.listStreamInfo("11");

        Assertions.assertEquals(JsonUtils.toJsonString(fullStreamResponse),
                JsonUtils.toJsonString(fullStreamResponses.get(0)));
    }

    @Test
    void testListSinkInfo_allType() {
        List<SinkListResponse> listResponses = Lists.newArrayList(
                ClickHouseSinkListResponse.builder()
                        .id(1)
                        .sinkType("CLICKHOUSE")

                        .jdbcUrl("127.0.0.1")
                        .partitionStrategy("BALANCE")
                        .partitionFields("partitionFields")
                        .build(),
                ElasticsearchSinkListResponse.builder()
                        .id(2)
                        .sinkType("ELASTICSEARCH")

                        .host("127.0.0.1")
                        .flushInterval(2)
                        .build(),
                HBaseSinkListResponse.builder()
                        .id(3)
                        .sinkType("HBASE")

                        .tableName("tableName")
                        .rowKey("rowKey")
                        .build(),
                HiveSinkListResponse.builder()
                        .id(4)
                        .sinkType("HIVE")

                        .dataPath("hdfs://ip:port/user/hive/warehouse/test.db")
                        .hiveVersion("hiveVersion")
                        .build(),
                IcebergSinkListResponse.builder()
                        .id(5)
                        .sinkType("ICEBERG")

                        .partitionType("H-hour")
                        .build(),
                KafkaSinkListResponse.builder()
                        .id(6)
                        .sinkType("KAFKA")

                        .topicName("test")
                        .partitionNum("6")
                        .build(),
                PostgresSinkListResponse.builder()
                        .id(7)
                        .sinkType("POSTGRES")

                        .primaryKey("test")
                        .build()
        );

        stubFor(
                get(urlMatching("/api/inlong/manager/sink/list.*"))
                        .willReturn(
                                okJson(
                                        JsonUtils.toJsonString(
                                                Response.success(
                                                        new PageInfo<>(Lists.newArrayList(listResponses)))
                                        )
                                )
                        )
        );

        List<SinkListResponse> sinkListResponses = innerInlongManagerClient.listSinks("11", "11");

        Assertions.assertEquals(JsonUtils.toJsonString(listResponses), JsonUtils.toJsonString(sinkListResponses));
    }

}
