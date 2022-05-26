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

import com.github.pagehelper.PageInfo;
import com.google.common.collect.Lists;
import com.google.common.reflect.TypeToken;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import lombok.experimental.UtilityClass;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.inlong.manager.common.beans.Response;
import org.apache.inlong.manager.common.enums.MQType;
import org.apache.inlong.manager.common.enums.SinkType;
import org.apache.inlong.manager.common.enums.SourceType;
import org.apache.inlong.manager.common.pojo.group.InlongGroupApproveRequest;
import org.apache.inlong.manager.common.pojo.group.InlongGroupInfo;
import org.apache.inlong.manager.common.pojo.group.InlongGroupListResponse;
import org.apache.inlong.manager.common.pojo.group.none.InlongNoneMqInfo;
import org.apache.inlong.manager.common.pojo.group.pulsar.InlongPulsarDTO;
import org.apache.inlong.manager.common.pojo.group.pulsar.InlongPulsarInfo;
import org.apache.inlong.manager.common.pojo.group.tube.InlongTubeInfo;
import org.apache.inlong.manager.common.pojo.sink.SinkListResponse;
import org.apache.inlong.manager.common.pojo.sink.SinkResponse;
import org.apache.inlong.manager.common.pojo.sink.ck.ClickHouseSinkResponse;
import org.apache.inlong.manager.common.pojo.sink.hive.HiveSinkResponse;
import org.apache.inlong.manager.common.pojo.sink.iceberg.IcebergSinkResponse;
import org.apache.inlong.manager.common.pojo.sink.kafka.KafkaSinkResponse;
import org.apache.inlong.manager.common.pojo.sink.postgres.PostgresSinkResponse;
import org.apache.inlong.manager.common.pojo.source.SourceListResponse;
import org.apache.inlong.manager.common.pojo.source.SourceResponse;
import org.apache.inlong.manager.common.pojo.source.autopush.AutoPushSourceListResponse;
import org.apache.inlong.manager.common.pojo.source.autopush.AutoPushSourceRequest;
import org.apache.inlong.manager.common.pojo.source.autopush.AutoPushSourceResponse;
import org.apache.inlong.manager.common.pojo.source.binlog.BinlogSourceListResponse;
import org.apache.inlong.manager.common.pojo.source.binlog.BinlogSourceResponse;
import org.apache.inlong.manager.common.pojo.source.file.FileSourceListResponse;
import org.apache.inlong.manager.common.pojo.source.file.FileSourceResponse;
import org.apache.inlong.manager.common.pojo.source.kafka.KafkaSourceListResponse;
import org.apache.inlong.manager.common.pojo.source.kafka.KafkaSourceResponse;
import org.apache.inlong.manager.common.pojo.source.postgres.PostgresSourceListResponse;
import org.apache.inlong.manager.common.pojo.source.postgres.PostgresSourceResponse;
import org.apache.inlong.manager.common.pojo.stream.FullStreamResponse;
import org.apache.inlong.manager.common.pojo.stream.InlongStreamApproveRequest;
import org.apache.inlong.manager.common.pojo.stream.InlongStreamConfigLogListResponse;
import org.apache.inlong.manager.common.pojo.stream.InlongStreamInfo;
import org.apache.inlong.manager.common.pojo.transform.TransformResponse;
import org.apache.inlong.manager.common.pojo.workflow.EventLogView;
import org.apache.inlong.manager.common.util.AssertUtils;

import java.util.List;

/**
 * Parser for Inlong entity
 */
@UtilityClass
public class InlongParser {

    public static final String GROUP_INFO = "groupInfo";
    public static final String MQ_EXT_INFO = "mqExtInfo";
    public static final String MQ_TYPE = "mqType";
    public static final String SINK_INFO = "sinkInfo";
    public static final String SOURCE_INFO = "sourceInfo";
    public static final String SINK_TYPE = "sinkType";
    public static final String SOURCE_TYPE = "sourceType";

    /**
     * Parse body to get the response.
     */
    public static Response parseResponse(String responseBody) {
        return GsonUtils.fromJson(responseBody, Response.class);
    }

    /**
     * Parse body to get the response.
     */
    public static <T> Response<T> parseResponse(Class<T> responseType, String responseBody) {
        AssertUtils.notNull(responseType, "responseType must not be null");
        return GsonUtils.fromJson(
                responseBody,
                com.google.gson.reflect.TypeToken.getParameterized(Response.class, responseType).getType()
        );
    }

    /**
     * Parse response to get the inlong group info.
     */
    public static InlongGroupInfo parseGroupInfo(Response response) {
        String dataJson = GsonUtils.toJson(response.getData());
        InlongGroupInfo groupInfo = GsonUtils.fromJson(dataJson, InlongGroupInfo.class);

        MQType mqType = MQType.forType(groupInfo.getMqType());
        if (mqType == MQType.PULSAR || mqType == MQType.TDMQ_PULSAR) {
            return GsonUtils.<InlongPulsarInfo>fromJson(dataJson, InlongPulsarInfo.class);
        } else if (mqType == MQType.TUBE) {
            return GsonUtils.<InlongTubeInfo>fromJson(dataJson, InlongTubeInfo.class);
        } else {
            return GsonUtils.<InlongTubeInfo>fromJson(dataJson, InlongNoneMqInfo.class);
        }
    }

    /**
     * Parse information of groups.
     */
    public static PageInfo<InlongGroupListResponse> parseGroupList(Response response) {
        Object data = response.getData();
        String pageInfoJson = GsonUtils.toJson(data);
        return GsonUtils.fromJson(pageInfoJson,
                new TypeToken<PageInfo<InlongGroupListResponse>>() {
                }.getType());
    }

    public static InlongStreamInfo parseStreamInfo(Response response) {
        Object data = response.getData();
        return GsonUtils.fromJson(GsonUtils.toJson(data), InlongStreamInfo.class);
    }

    /**
     * Parse list of stream.
     */
    public static List<FullStreamResponse> parseStreamList(Response response) {
        Object data = response.getData();
        JsonObject pageInfoJson = GsonUtils.fromJson(GsonUtils.toJson(data), JsonObject.class);
        JsonArray fullStreamArray = pageInfoJson.getAsJsonArray("list");
        List<FullStreamResponse> list = Lists.newArrayList();
        for (int streamIndex = 0; streamIndex < fullStreamArray.size(); streamIndex++) {
            JsonObject fullStreamJson = (JsonObject) fullStreamArray.get(streamIndex);
            FullStreamResponse fullStreamResponse = GsonUtils.fromJson(fullStreamJson.toString(),
                    FullStreamResponse.class);
            list.add(fullStreamResponse);
            //Parse sourceResponse in each stream
            JsonArray sourceJsonArr = fullStreamJson.getAsJsonArray(SOURCE_INFO);
            List<SourceResponse> sourceResponses = Lists.newArrayList();
            fullStreamResponse.setSourceInfo(sourceResponses);
            for (int sourceIndex = 0; sourceIndex < sourceJsonArr.size(); sourceIndex++) {
                JsonObject sourceJson = (JsonObject) sourceJsonArr.get(sourceIndex);
                String type = sourceJson.get(SOURCE_TYPE).getAsString();
                SourceType sourceType = SourceType.forType(type);
                switch (sourceType) {
                    case BINLOG:
                        BinlogSourceResponse binlogSourceResponse = GsonUtils.fromJson(sourceJson.toString(),
                                BinlogSourceResponse.class);
                        sourceResponses.add(binlogSourceResponse);
                        break;
                    case KAFKA:
                        KafkaSourceResponse kafkaSourceResponse = GsonUtils.fromJson(sourceJson.toString(),
                                KafkaSourceResponse.class);
                        sourceResponses.add(kafkaSourceResponse);
                        break;
                    case FILE:
                        FileSourceResponse fileSourceResponse = GsonUtils.fromJson(sourceJson.toString(),
                                FileSourceResponse.class);
                        sourceResponses.add(fileSourceResponse);
                        break;
                    case AUTO_PUSH:
                        AutoPushSourceResponse autoPushSourceResponse = GsonUtils.fromJson(sourceJson.toString(),
                                AutoPushSourceRequest.class);
                        sourceResponses.add(autoPushSourceResponse);
                        break;
                    case POSTGRES:
                        PostgresSourceResponse postgresSourceResponse = GsonUtils.fromJson(sourceJson.toString(),
                                PostgresSourceResponse.class);
                        sourceResponses.add(postgresSourceResponse);
                        break;
                    default:
                        throw new RuntimeException(String.format("Unsupported sourceType=%s for Inlong", sourceType));
                }
            }

            //Parse sinkResponse in each stream
            JsonArray sinkJsonArr = fullStreamJson.getAsJsonArray(SINK_INFO);
            List<SinkResponse> sinkResponses = Lists.newArrayList();
            fullStreamResponse.setSinkInfo(sinkResponses);
            for (int sinkIndex = 0; sinkIndex < sinkJsonArr.size(); sinkIndex++) {
                JsonObject sinkJson = (JsonObject) sinkJsonArr.get(sinkIndex);
                String type = sinkJson.get(SINK_TYPE).getAsString();
                SinkType sinkType = SinkType.forType(type);
                switch (sinkType) {
                    case HIVE:
                        HiveSinkResponse hiveSinkResponse = GsonUtils.fromJson(sinkJson.toString(),
                                HiveSinkResponse.class);
                        sinkResponses.add(hiveSinkResponse);
                        break;
                    case KAFKA:
                        KafkaSinkResponse kafkaSinkResponse = GsonUtils.fromJson(sinkJson.toString(),
                                KafkaSinkResponse.class);
                        sinkResponses.add(kafkaSinkResponse);
                        break;
                    case ICEBERG:
                        IcebergSinkResponse icebergSinkResponse = GsonUtils.fromJson(sinkJson.toString(),
                                IcebergSinkResponse.class);
                        sinkResponses.add(icebergSinkResponse);
                        break;
                    case CLICKHOUSE:
                        ClickHouseSinkResponse clickHouseSinkResponse = GsonUtils.fromJson(sinkJson.toString(),
                                ClickHouseSinkResponse.class);
                        sinkResponses.add(clickHouseSinkResponse);
                        break;
                    case POSTGRES:
                        PostgresSinkResponse postgresSinkResponse = GsonUtils.fromJson(sinkJson.toString(),
                                PostgresSinkResponse.class);
                        sinkResponses.add(postgresSinkResponse);
                        break;
                    default:
                        throw new RuntimeException(String.format("Unsupported sinkType=%s for Inlong", sinkType));
                }
            }
        }
        return list;
    }

    /**
     * Parse list of source.
     */
    public static PageInfo<SourceListResponse> parseSourceList(Response response) {
        Object data = response.getData();
        String pageInfoJson = GsonUtils.toJson(data);
        PageInfo<SourceListResponse> pageInfo = GsonUtils.fromJson(pageInfoJson,
                new TypeToken<PageInfo<SourceListResponse>>() {
                }.getType());
        if (pageInfo.getList() != null && !pageInfo.getList().isEmpty()) {
            SourceListResponse sourceListResponse = pageInfo.getList().get(0);
            SourceType sourceType = SourceType.forType(sourceListResponse.getSourceType());
            switch (sourceType) {
                case BINLOG:
                    return GsonUtils.fromJson(pageInfoJson,
                            new TypeToken<PageInfo<BinlogSourceListResponse>>() {
                            }.getType());
                case KAFKA:
                    return GsonUtils.fromJson(pageInfoJson,
                            new TypeToken<PageInfo<KafkaSourceListResponse>>() {
                            }.getType());
                case FILE:
                    return GsonUtils.fromJson(pageInfoJson,
                            new TypeToken<PageInfo<FileSourceListResponse>>() {
                            }.getType());
                case AUTO_PUSH:
                    return GsonUtils.fromJson(pageInfoJson,
                            new TypeToken<PageInfo<AutoPushSourceListResponse>>() {
                            }.getType());
                case POSTGRES:
                    return GsonUtils.fromJson(pageInfoJson,
                            new TypeToken<PageInfo<PostgresSourceListResponse>>() {
                            }.getType());
                default:
                    throw new IllegalArgumentException(
                            String.format("Unsupported sourceType=%s for Inlong", sourceType));
            }
        } else {
            return new PageInfo<>();
        }
    }

    /**
     * Parse list of transformation.
     */
    public static List<TransformResponse> parseTransformList(Response response) {
        Object data = response.getData();
        String pageInfoJson = GsonUtils.toJson(data);
        return GsonUtils.fromJson(pageInfoJson,
                new TypeToken<List<TransformResponse>>() {
                }.getType());
    }

    /**
     * Parse list of data sink.
     */
    public static PageInfo<SinkListResponse> parseSinkList(Response response) {
        Object data = response.getData();
        String pageInfoJson = GsonUtils.toJson(data);
        return GsonUtils.fromJson(pageInfoJson,
                new TypeToken<PageInfo<SinkListResponse>>() {
                }.getType());
    }

    /**
     * Parse forms of group.
     */
    public static Pair<InlongGroupApproveRequest, List<InlongStreamApproveRequest>> parseGroupForm(String formJson) {
        JsonObject formData = GsonUtils.fromJson(formJson, JsonObject.class);
        JsonObject groupJson = formData.getAsJsonObject(GROUP_INFO);
        InlongGroupApproveRequest groupApproveInfo = GsonUtils.fromJson(groupJson.toString(),
                InlongGroupApproveRequest.class);
        JsonObject mqExtInfo = groupJson.getAsJsonObject(MQ_EXT_INFO);
        if (mqExtInfo != null && mqExtInfo.get(MQ_TYPE) != null) {
            MQType mqType = MQType.forType(mqExtInfo.get(MQ_TYPE).getAsString());
            if (mqType == MQType.PULSAR || mqType == MQType.TDMQ_PULSAR) {
                InlongPulsarDTO pulsarInfo = GsonUtils.fromJson(mqExtInfo.toString(),
                        InlongPulsarDTO.class);
                groupApproveInfo.setAckQuorum(pulsarInfo.getAckQuorum());
                groupApproveInfo.setEnsemble(pulsarInfo.getEnsemble());
                groupApproveInfo.setWriteQuorum(pulsarInfo.getWriteQuorum());
                groupApproveInfo.setRetentionTime(pulsarInfo.getRetentionTime());
                groupApproveInfo.setRetentionTimeUnit(pulsarInfo.getRetentionTimeUnit());
                groupApproveInfo.setTtl(pulsarInfo.getTtl());
                groupApproveInfo.setTtlUnit(pulsarInfo.getTtlUnit());
                groupApproveInfo.setRetentionSize(pulsarInfo.getRetentionSize());
                groupApproveInfo.setRetentionSizeUnit(pulsarInfo.getRetentionSizeUnit());
            }
        }
        JsonArray streamJson = formData.getAsJsonArray("streamInfoList");
        List<InlongStreamApproveRequest> streamApproveList = GsonUtils.fromJson(streamJson.toString(),
                new TypeToken<List<InlongStreamApproveRequest>>() {
                }.getType());
        return Pair.of(groupApproveInfo, streamApproveList);
    }

    /**
     * Parse list of event about view log.
     */
    public static PageInfo<EventLogView> parseEventLogViewList(Response response) {
        Object data = response.getData();
        String pageInfoJson = GsonUtils.toJson(data);
        return GsonUtils.fromJson(pageInfoJson,
                new TypeToken<PageInfo<EventLogView>>() {
                }.getType());
    }

    /**
     * Parse list of stream log.
     */
    public static PageInfo<InlongStreamConfigLogListResponse> parseStreamLogList(Response response) {
        Object data = response.getData();
        String pageInfoJson = GsonUtils.toJson(data);
        return GsonUtils.fromJson(pageInfoJson,
                new TypeToken<PageInfo<InlongStreamConfigLogListResponse>>() {
                }.getType());
    }

}
