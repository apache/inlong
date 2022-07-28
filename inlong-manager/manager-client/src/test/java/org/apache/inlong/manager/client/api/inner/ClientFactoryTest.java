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

package org.apache.inlong.manager.client.api.inner;

import com.github.pagehelper.PageInfo;
import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.client.WireMock;
import com.google.common.collect.Lists;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.inlong.manager.client.api.ClientConfiguration;
import org.apache.inlong.manager.client.api.impl.InlongClientImpl;
import org.apache.inlong.manager.client.api.inner.client.ClientFactory;
import org.apache.inlong.manager.client.api.inner.client.InlongClusterClient;
import org.apache.inlong.manager.client.api.inner.client.InlongGroupClient;
import org.apache.inlong.manager.client.api.inner.client.InlongStreamClient;
import org.apache.inlong.manager.client.api.inner.client.StreamSinkClient;
import org.apache.inlong.manager.client.api.util.ClientUtils;
import org.apache.inlong.manager.common.auth.DefaultAuthentication;
import org.apache.inlong.manager.common.beans.Response;
import org.apache.inlong.manager.common.enums.ClusterType;
import org.apache.inlong.manager.common.enums.SinkType;
import org.apache.inlong.manager.common.pojo.cluster.BindTagRequest;
import org.apache.inlong.manager.common.pojo.cluster.ClusterInfo;
import org.apache.inlong.manager.common.pojo.cluster.ClusterNodeRequest;
import org.apache.inlong.manager.common.pojo.cluster.ClusterNodeResponse;
import org.apache.inlong.manager.common.pojo.cluster.ClusterRequest;
import org.apache.inlong.manager.common.pojo.cluster.ClusterTagRequest;
import org.apache.inlong.manager.common.pojo.cluster.ClusterTagResponse;
import org.apache.inlong.manager.common.pojo.cluster.pulsar.PulsarClusterInfo;
import org.apache.inlong.manager.common.pojo.cluster.pulsar.PulsarClusterRequest;
import org.apache.inlong.manager.common.pojo.group.InlongGroupBriefInfo;
import org.apache.inlong.manager.common.pojo.group.InlongGroupCountResponse;
import org.apache.inlong.manager.common.pojo.group.InlongGroupExtInfo;
import org.apache.inlong.manager.common.pojo.group.InlongGroupInfo;
import org.apache.inlong.manager.common.pojo.group.InlongGroupResetRequest;
import org.apache.inlong.manager.common.pojo.group.InlongGroupTopicInfo;
import org.apache.inlong.manager.common.pojo.group.pulsar.InlongPulsarInfo;
import org.apache.inlong.manager.common.pojo.group.pulsar.InlongPulsarRequest;
import org.apache.inlong.manager.common.pojo.sink.StreamSink;
import org.apache.inlong.manager.common.pojo.sink.ck.ClickHouseSink;
import org.apache.inlong.manager.common.pojo.sink.es.ElasticsearchSink;
import org.apache.inlong.manager.common.pojo.sink.hbase.HBaseSink;
import org.apache.inlong.manager.common.pojo.sink.hive.HiveSink;
import org.apache.inlong.manager.common.pojo.sink.iceberg.IcebergSink;
import org.apache.inlong.manager.common.pojo.sink.kafka.KafkaSink;
import org.apache.inlong.manager.common.pojo.sink.mysql.MySQLSink;
import org.apache.inlong.manager.common.pojo.sink.postgresql.PostgreSQLSink;
import org.apache.inlong.manager.common.pojo.source.StreamSource;
import org.apache.inlong.manager.common.pojo.source.autopush.AutoPushSource;
import org.apache.inlong.manager.common.pojo.source.file.FileSource;
import org.apache.inlong.manager.common.pojo.source.kafka.KafkaSource;
import org.apache.inlong.manager.common.pojo.source.mysql.MySQLBinlogSource;
import org.apache.inlong.manager.common.pojo.stream.InlongStreamBriefInfo;
import org.apache.inlong.manager.common.pojo.stream.InlongStreamInfo;
import org.apache.inlong.manager.common.pojo.stream.InlongStreamResponse;
import org.apache.inlong.manager.common.pojo.stream.StreamField;
import org.apache.inlong.manager.common.util.JsonUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.List;

import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.okJson;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.client.WireMock.stubFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlMatching;
import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.options;

/**
 * Unit test for {@link ClientFactory}.
 */
@Slf4j
class ClientFactoryTest {

    private static final int SERVICE_PORT = 8085;
    private static WireMockServer wireMockServer;
    private static InlongGroupClient groupClient;
    private static InlongStreamClient streamClient;
    private static StreamSinkClient sinkClient;
    private static InlongClusterClient clusterClient;

    @BeforeAll
    static void setup() {
        wireMockServer = new WireMockServer(options().port(SERVICE_PORT));
        wireMockServer.start();
        WireMock.configureFor(wireMockServer.port());

        String serviceUrl = "127.0.0.1:" + SERVICE_PORT;
        ClientConfiguration configuration = new ClientConfiguration();
        configuration.setAuthentication(new DefaultAuthentication("admin", "inlong"));
        InlongClientImpl inlongClient = new InlongClientImpl(serviceUrl, configuration);
        ClientFactory clientFactory = ClientUtils.getClientFactory(inlongClient.getConfiguration());
        groupClient = clientFactory.getGroupClient();
        streamClient = clientFactory.getStreamClient();
        sinkClient = clientFactory.getSinkClient();
        streamClient = clientFactory.getStreamClient();
        clusterClient = clientFactory.getClusterClient();
    }

    @AfterAll
    static void teardown() {
        wireMockServer.stop();
    }

    @Test
    void testGroupExist() {
        stubFor(
                get(urlMatching("/manager/group/exist/123.*"))
                        .willReturn(
                                okJson(JsonUtils.toJsonString(Response.success(true)))
                        )
        );
        Boolean groupExists = groupClient.isGroupExists("123");
        Assertions.assertTrue(groupExists);
    }

    @Test
    void testGetGroupInfo() {
        InlongPulsarInfo inlongGroupResponse = InlongPulsarInfo.builder()
                .id(1)
                .inlongGroupId("1")
                .mqType("PULSAR")
                .enableCreateResource(1)
                .extList(
                        Lists.newArrayList(InlongGroupExtInfo.builder()
                                .id(1)
                                .inlongGroupId("1")
                                .keyName("keyName")
                                .keyValue("keyValue")
                                .build()
                        )
                ).build();

        stubFor(
                get(urlMatching("/manager/group/get/1.*"))
                        .willReturn(
                                okJson(JsonUtils.toJsonString(Response.success(inlongGroupResponse)))
                        )
        );

        InlongGroupInfo groupInfo = groupClient.getGroupInfo("1");
        Assertions.assertTrue(groupInfo instanceof InlongPulsarInfo);
        Assertions.assertEquals(JsonUtils.toJsonString(inlongGroupResponse), JsonUtils.toJsonString(groupInfo));
    }

    @Test
    void testListGroup4AutoPushSource() {
        List<InlongGroupBriefInfo> groupBriefInfos = Lists.newArrayList(
                InlongGroupBriefInfo.builder()
                        .id(1)
                        .inlongGroupId("1")
                        .name("name")
                        .streamSources(
                                Lists.newArrayList(
                                        AutoPushSource.builder()
                                                .id(22)
                                                .inlongGroupId("1")
                                                .inlongStreamId("2")
                                                .sourceType("AUTO_PUSH")
                                                .dataProxyGroup("111")
                                                .build()
                                )
                        ).build()
        );

        stubFor(
                post(urlMatching("/manager/group/list.*"))
                        .willReturn(
                                okJson(JsonUtils.toJsonString(Response.success(new PageInfo<>(groupBriefInfos))))
                        )
        );

        PageInfo<InlongGroupBriefInfo> pageInfo = groupClient.listGroups("keyword", 1, 1, 10);
        Assertions.assertEquals(JsonUtils.toJsonString(groupBriefInfos),
                JsonUtils.toJsonString(pageInfo.getList()));
    }

    @Test
    void testListGroup4BinlogSource() {
        List<InlongGroupBriefInfo> groupBriefInfos = Lists.newArrayList(
                InlongGroupBriefInfo.builder()
                        .id(1)
                        .inlongGroupId("1")
                        .name("name")
                        .streamSources(
                                Lists.newArrayList(
                                        MySQLBinlogSource.builder()
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
                        ).build()
        );

        stubFor(
                post(urlMatching("/manager/group/list.*"))
                        .willReturn(
                                okJson(JsonUtils.toJsonString(Response.success(new PageInfo<>(groupBriefInfos))))
                        )
        );

        PageInfo<InlongGroupBriefInfo> pageInfo = groupClient.listGroups("keyword", 1, 1, 10);
        Assertions.assertEquals(JsonUtils.toJsonString(groupBriefInfos),
                JsonUtils.toJsonString(pageInfo.getList()));
    }

    @Test
    void testListGroup4FileSource() {
        List<InlongGroupBriefInfo> groupBriefInfos = Lists.newArrayList(
                InlongGroupBriefInfo.builder()
                        .id(1)
                        .inlongGroupId("1")
                        .name("name")
                        .inCharges("admin")
                        .status(1)
                        .createTime(new Date())
                        .modifyTime(new Date())
                        .streamSources(
                                Lists.newArrayList(
                                        FileSource.builder()
                                                .id(22)
                                                .inlongGroupId("1")
                                                .inlongStreamId("2")
                                                .sourceType("FILE")
                                                .status(1)
                                                .ip("127.0.0.1")
                                                .pattern("pattern")
                                                .build()
                                )
                        ).build()
        );

        stubFor(
                post(urlMatching("/manager/group/list.*"))
                        .willReturn(
                                okJson(JsonUtils.toJsonString(Response.success(new PageInfo<>(groupBriefInfos))))
                        )
        );

        PageInfo<InlongGroupBriefInfo> pageInfo = groupClient.listGroups("keyword", 1, 1, 10);
        Assertions.assertEquals(JsonUtils.toJsonString(groupBriefInfos),
                JsonUtils.toJsonString(pageInfo.getList()));
    }

    @Test
    void testListGroup4KafkaSource() {
        List<InlongGroupBriefInfo> groupBriefInfos = Lists.newArrayList(
                InlongGroupBriefInfo.builder()
                        .id(1)
                        .inlongGroupId("1")
                        .streamSources(
                                Lists.newArrayList(
                                        KafkaSource.builder()
                                                .id(22)
                                                .inlongGroupId("1")
                                                .inlongStreamId("2")
                                                .sourceType("KAFKA")
                                                .dataNodeName("dataNodeName")
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
                post(urlMatching("/manager/group/list.*"))
                        .willReturn(
                                okJson(JsonUtils.toJsonString(Response.success(new PageInfo<>(groupBriefInfos))))
                        )
        );

        PageInfo<InlongGroupBriefInfo> pageInfo = groupClient.listGroups("keyword", 1, 1, 10);
        Assertions.assertEquals(JsonUtils.toJsonString(groupBriefInfos),
                JsonUtils.toJsonString(pageInfo.getList()));
    }

    @Test
    void testListGroup4AllSource() {
        ArrayList<StreamSource> streamSources = Lists.newArrayList(
                AutoPushSource.builder()
                        .id(22)
                        .inlongGroupId("1")
                        .inlongStreamId("2")
                        .sourceType("AUTO_PUSH")
                        .version(1)
                        .build(),

                MySQLBinlogSource.builder()
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

                FileSource.builder()
                        .id(22)
                        .inlongGroupId("1")
                        .inlongStreamId("2")
                        .version(1)
                        .ip("127.0.0.1")
                        .pattern("pattern")
                        .timeOffset("timeOffset")
                        .build(),

                KafkaSource.builder()
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
        );
        List<InlongGroupBriefInfo> groupBriefInfos = Lists.newArrayList(
                InlongGroupBriefInfo.builder()
                        .id(1)
                        .inlongGroupId("1")
                        .name("name")
                        .inCharges("admin")
                        .streamSources(streamSources)
                        .build()
        );

        stubFor(
                post(urlMatching("/manager/group/list.*"))
                        .willReturn(
                                okJson(JsonUtils.toJsonString(Response.success(new PageInfo<>(groupBriefInfos)))
                                )
                        )
        );

        PageInfo<InlongGroupBriefInfo> pageInfo = groupClient.listGroups("keyword", 1, 1, 10);
        Assertions.assertEquals(JsonUtils.toJsonString(groupBriefInfos),
                JsonUtils.toJsonString(pageInfo.getList()));
    }

    @Test
    void testListGroup4NotExist() {
        stubFor(
                post(urlMatching("/manager/group/list.*"))
                        .willReturn(
                                okJson(JsonUtils.toJsonString(
                                        Response.fail("Inlong group does not exist/no operation authority"))
                                )
                        )
        );

        PageInfo<InlongGroupBriefInfo> pageInfo = groupClient.listGroups("keyword", 1, 1, 10);
        Assertions.assertNull(pageInfo);
    }

    @Test
    void testCreateGroup() {
        stubFor(
                post(urlMatching("/manager/group/save.*"))
                        .willReturn(
                                okJson(JsonUtils.toJsonString(Response.success("1111")))
                        )
        );

        String groupId = groupClient.createGroup(new InlongPulsarRequest());
        Assertions.assertEquals("1111", groupId);
    }

    @Test
    void testUpdateGroup() {
        stubFor(
                post(urlMatching("/manager/group/update.*"))
                        .willReturn(
                                okJson(JsonUtils.toJsonString(Response.success("1111")))
                        )
        );

        Pair<String, String> updateGroup = groupClient.updateGroup(new InlongPulsarRequest());
        Assertions.assertEquals("1111", updateGroup.getKey());
        Assertions.assertTrue(StringUtils.isBlank(updateGroup.getValue()));
    }

    @Test
    void testCountGroupByUser() {
        InlongGroupCountResponse expected = new InlongGroupCountResponse();
        expected.setRejectCount(102400L);
        expected.setTotalCount(834781232L);
        expected.setWaitApproveCount(34524L);
        expected.setWaitAssignCount(45678L);
        stubFor(
                get(urlMatching("/manager/group/countByStatus.*"))
                        .willReturn(
                                okJson(JsonUtils.toJsonString(
                                        Response.success(expected))
                                ))
        );
        InlongGroupCountResponse actual = groupClient.countGroupByUser();
        Assertions.assertEquals(expected.getRejectCount(), actual.getRejectCount());
        Assertions.assertEquals(expected.getTotalCount(), actual.getTotalCount());
        Assertions.assertEquals(expected.getWaitApproveCount(), actual.getWaitApproveCount());
        Assertions.assertEquals(expected.getWaitAssignCount(), actual.getWaitAssignCount());
    }

    @Test
    void getTopic() {
        InlongGroupTopicInfo expected = new InlongGroupTopicInfo();
        expected.setInlongGroupId("1");
        expected.setMqResource("testTopic");
        expected.setMqType("TUBE");
        expected.setPulsarAdminUrl("http://127.0.0.1:8080");
        expected.setPulsarServiceUrl("http://127.0.0.1:8081");
        expected.setTubeMasterUrl("http://127.0.0.1:8082");
        List<InlongStreamBriefInfo> list = new ArrayList<>();
        expected.setStreamTopics(list);
        InlongStreamBriefInfo briefInfo = new InlongStreamBriefInfo();
        briefInfo.setId(1);
        briefInfo.setInlongGroupId("testgroup");
        briefInfo.setModifyTime(new Date());
        stubFor(
                get(urlMatching("/manager/group/getTopic/1.*"))
                        .willReturn(
                                okJson(JsonUtils.toJsonString(
                                        Response.success(expected))
                                ))
        );
        InlongGroupTopicInfo actual = groupClient.getTopic("1");
        Assertions.assertEquals(expected.getInlongGroupId(), actual.getInlongGroupId());
        Assertions.assertEquals(expected.getMqType(), actual.getMqType());
        Assertions.assertEquals(expected.getTubeMasterUrl(), actual.getTubeMasterUrl());
        Assertions.assertEquals(expected.getPulsarAdminUrl(), actual.getPulsarAdminUrl());
        Assertions.assertEquals(expected.getPulsarServiceUrl(), actual.getPulsarServiceUrl());
        Assertions.assertEquals(expected.getStreamTopics(), actual.getStreamTopics());
    }

    @Test
    void testCreateStream() {
        stubFor(
                post(urlMatching("/manager/stream/save.*"))
                        .willReturn(
                                okJson(JsonUtils.toJsonString(Response.success(11)))
                        )
        );

        Integer groupId = streamClient.createStreamInfo(new InlongStreamInfo());
        Assertions.assertEquals(11, groupId);
    }

    @Test
    void testStreamExist() {
        stubFor(
                get(urlMatching("/manager/stream/exist/123/11.*"))
                        .willReturn(
                                okJson(JsonUtils.toJsonString(Response.success(true)))
                        )
        );

        InlongStreamInfo streamInfo = new InlongStreamInfo();
        streamInfo.setInlongGroupId("123");
        streamInfo.setInlongStreamId("11");
        Boolean groupExists = streamClient.isStreamExists(streamInfo);

        Assertions.assertTrue(groupExists);
    }

    @Test
    void testGetStream() {
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
                                        .fieldType("string")
                                        .build(),
                                StreamField.builder()
                                        .id(2)
                                        .inlongGroupId("123")
                                        .inlongGroupId("11")
                                        .isMetaField(1)
                                        .build()
                        )
                ).build();

        stubFor(
                get(urlMatching("/manager/stream/get.*"))
                        .willReturn(
                                okJson(JsonUtils.toJsonString(Response.success(streamResponse)))
                        )
        );

        InlongStreamInfo inlongStreamInfo = streamClient.getStreamInfo("123", "11");
        Assertions.assertNotNull(inlongStreamInfo);
    }

    @Test
    void testGetStream4NotExist() {
        stubFor(
                get(urlMatching("/manager/stream/get.*"))
                        .willReturn(
                                okJson(JsonUtils.toJsonString(
                                        Response.fail("Inlong stream does not exist/no operation permission")))
                        )
        );

        InlongStreamInfo inlongStreamInfo = streamClient.getStreamInfo("123", "11");
        Assertions.assertNull(inlongStreamInfo);
    }

    @Test
    void testListStream4AllSink() {
        InlongStreamInfo streamInfo = InlongStreamInfo.builder()
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
                ).build();

        ArrayList<StreamSource> sourceList = Lists.newArrayList(
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
                        .bootstrapServers("127.0.0.1")
                        .build()
        );

        ArrayList<StreamSink> sinkList = Lists.newArrayList(
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
        );

        streamInfo.setSourceList(sourceList);
        streamInfo.setSinkList(sinkList);

        stubFor(
                post(urlMatching("/manager/stream/listAll.*"))
                        .willReturn(
                                okJson(JsonUtils.toJsonString(
                                        Response.success(new PageInfo<>(Lists.newArrayList(streamInfo))))
                                )
                        )
        );

        List<InlongStreamInfo> streamInfos = streamClient.listStreamInfo("11");
        Assertions.assertEquals(JsonUtils.toJsonString(streamInfo), JsonUtils.toJsonString(streamInfos.get(0)));
    }

    @Test
    void testListSink4AllType() {
        List<StreamSink> sinkList = Lists.newArrayList(
                ClickHouseSink.builder()
                        .id(1)
                        .sinkType("CLICKHOUSE")
                        .jdbcUrl("127.0.0.1")
                        .partitionStrategy("BALANCE")
                        .partitionFields("partitionFields")
                        .build(),
                ElasticsearchSink.builder()
                        .id(2)
                        .sinkType("ELASTICSEARCH")
                        .host("127.0.0.1")
                        .flushInterval(2)
                        .build(),
                HBaseSink.builder()
                        .id(3)
                        .sinkType("HBASE")
                        .tableName("tableName")
                        .rowKey("rowKey")
                        .build(),
                HiveSink.builder()
                        .id(4)
                        .sinkType("HIVE")
                        .dataPath("hdfs://ip:port/user/hive/warehouse/test.db")
                        .hiveVersion("hiveVersion")
                        .build(),
                IcebergSink.builder()
                        .id(5)
                        .sinkType("ICEBERG")
                        .partitionType("H-hour")
                        .build(),
                KafkaSink.builder()
                        .id(6)
                        .sinkType("KAFKA")
                        .topicName("test")
                        .partitionNum("6")
                        .build(),
                PostgreSQLSink.builder()
                        .id(7)
                        .sinkType("POSTGRES")
                        .primaryKey("test")
                        .build()
        );

        stubFor(
                get(urlMatching("/manager/sink/list.*"))
                        .willReturn(
                                okJson(JsonUtils.toJsonString(
                                        Response.success(new PageInfo<>(Lists.newArrayList(sinkList))))
                                )
                        )
        );

        List<StreamSink> sinks = sinkClient.listSinks("11", "11");
        Assertions.assertEquals(JsonUtils.toJsonString(sinkList), JsonUtils.toJsonString(sinks));
    }

    @Test
    void testListSink4AllTypeShouldThrowException() {
        stubFor(
                get(urlMatching("/manager/sink/list.*"))
                        .willReturn(
                                okJson(JsonUtils.toJsonString(
                                        Response.fail("groupId should not empty"))
                                )
                        )
        );

        RuntimeException exception = Assertions.assertThrows(IllegalArgumentException.class,
                () -> sinkClient.listSinks("", "11"));
        Assertions.assertTrue(exception.getMessage().contains("groupId should not empty"));
    }

    @Test
    void testResetGroup() {
        stubFor(
                post(urlMatching("/manager/group/reset.*"))
                        .willReturn(
                                okJson(JsonUtils.toJsonString(
                                        Response.success(true))
                                )
                        )
        );

        boolean isReset = groupClient.resetGroup(new InlongGroupResetRequest());
        Assertions.assertTrue(isReset);
    }

    @Test
    void testSaveCluster() {
        stubFor(
                post(urlMatching("/manager/cluster/save.*"))
                        .willReturn(
                                okJson(JsonUtils.toJsonString(
                                        Response.success(1))
                                )
                        )
        );
        ClusterRequest request = new PulsarClusterRequest();
        request.setName("pulsar");
        request.setClusterTags("test_cluster");
        Integer clusterIndex = clusterClient.saveCluster(request);
        Assertions.assertEquals(1, (int) clusterIndex);
    }

    @Test
    void testGetCluster() {
        ClusterInfo cluster = PulsarClusterInfo.builder()
                .id(1)
                .name("test_cluster")
                .url("127.0.0.1")
                .clusterTags("test_cluster_tag")
                .type(ClusterType.PULSAR)
                .adminUrl("http://127.0.0.1:8080")
                .tenant("public")
                .build();

        stubFor(
                get(urlMatching("/manager/cluster/get/1.*"))
                        .willReturn(
                                okJson(JsonUtils.toJsonString(
                                        Response.success(cluster))
                                )
                        )
        );

        ClusterInfo clusterInfo = clusterClient.get(1);
        Assertions.assertEquals(1, clusterInfo.getId());
        Assertions.assertTrue(clusterInfo instanceof PulsarClusterInfo);
    }

    @Test
    void testGetMysqlSinkInfo() {
        StreamSink streamSink = MySQLSink.builder()
                // mysql field
                .jdbcUrl("127.0.0.1:3306")
                .username("test")
                .password("pwd")
                .tableName("tableName")
                .primaryKey("id")
                // streamSink field
                .id(1)
                .inlongGroupId("1")
                .inlongStreamId("1")
                .sinkType(SinkType.SINK_MYSQL)
                .sinkName("mysql_test")
                // streamNode field
                .preNodes(new HashSet<>())
                .postNodes(new HashSet<>())
                .fieldList(
                        Lists.newArrayList(StreamField.builder()
                                .fieldName("id")
                                .fieldType("int")
                                .build())
                )
                .build();

        stubFor(
                get(urlMatching("/manager/sink/get/1.*"))
                        .willReturn(
                                okJson(JsonUtils.toJsonString(
                                        Response.success(streamSink))
                                ))
        );

        StreamSink sinkInfo = sinkClient.getSinkInfo(1);
        Assertions.assertEquals(1, sinkInfo.getId());
        Assertions.assertTrue(sinkInfo instanceof MySQLSink);
    }

    @Test
    void testSaveClusterTag() {
        stubFor(
                post(urlMatching("/manager/cluster/tag/save.*"))
                        .willReturn(
                                okJson(JsonUtils.toJsonString(
                                        Response.success(1))
                                )
                        )
        );
        ClusterTagRequest request = new ClusterTagRequest();
        request.setClusterTag("test_cluster");
        Integer tagId = clusterClient.saveTag(request);
        Assertions.assertEquals(1, tagId);
    }

    @Test
    void testGetClusterTag() {
        ClusterTagResponse tagResponse = ClusterTagResponse.builder()
                .id(1)
                .clusterTag("test_cluster")
                .creator("admin")
                .inCharges("admin")
                .build();
        stubFor(
                get(urlMatching("/manager/cluster/tag/get/1.*"))
                        .willReturn(
                                okJson(JsonUtils.toJsonString(
                                        Response.success(tagResponse))
                                )
                        )
        );
        ClusterTagResponse clusterTagInfo = clusterClient.getTag(1);
        Assertions.assertNotNull(clusterTagInfo);
    }

    @Test
    void testBindTag() {
        stubFor(
                post(urlMatching("/manager/cluster/bindTag.*"))
                        .willReturn(
                                okJson(JsonUtils.toJsonString(
                                        Response.success(true))
                                )
                        )
        );
        BindTagRequest request = new BindTagRequest();
        request.setClusterTag("test_cluster_tag");
        Boolean isBind = clusterClient.bindTag(request);
        Assertions.assertTrue(isBind);
    }

    @Test
    void testSaveNode() {
        stubFor(
                post(urlMatching("/manager/cluster/node/save.*"))
                        .willReturn(
                                okJson(JsonUtils.toJsonString(
                                        Response.success(1))
                                )
                        )
        );
        ClusterNodeRequest request = new ClusterNodeRequest();
        request.setType(ClusterType.PULSAR);
        Integer nodeId = clusterClient.saveNode(request);
        Assertions.assertEquals(1, nodeId);
    }

    @Test
    void testGetNode() {
        ClusterNodeResponse response = ClusterNodeResponse.builder()
                .id(1)
                .type(ClusterType.DATA_PROXY)
                .ip("127.0.0.1")
                .port(46801)
                .build();
        stubFor(
                get(urlMatching("/manager/cluster/node/get/1.*"))
                        .willReturn(
                                okJson(JsonUtils.toJsonString(
                                        Response.success(response))
                                )
                        )
        );
        ClusterNodeResponse clientNode = clusterClient.getNode(1);
        Assertions.assertEquals(1, clientNode.getId());
    }
}
