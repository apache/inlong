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

package org.apache.inlong.tubemq.manager.service;

import org.apache.inlong.tubemq.manager.controller.TubeMQResult;
import org.apache.inlong.tubemq.manager.controller.group.request.DeleteOffsetReq;
import org.apache.inlong.tubemq.manager.controller.group.request.QueryConsumerGroupReq;
import org.apache.inlong.tubemq.manager.controller.group.request.QueryOffsetReq;
import org.apache.inlong.tubemq.manager.controller.group.result.AllBrokersOffsetRes;
import org.apache.inlong.tubemq.manager.controller.group.result.AllBrokersOffsetRes.OffsetInfo;
import org.apache.inlong.tubemq.manager.controller.group.result.GroupOffsetRes;
import org.apache.inlong.tubemq.manager.controller.group.result.OffsetPartitionRes;
import org.apache.inlong.tubemq.manager.controller.group.result.OffsetQueryRes;
import org.apache.inlong.tubemq.manager.controller.group.result.TopicOffsetRes;
import org.apache.inlong.tubemq.manager.controller.node.request.CloneOffsetReq;
import org.apache.inlong.tubemq.manager.controller.topic.request.RebalanceConsumerReq;
import org.apache.inlong.tubemq.manager.controller.topic.request.RebalanceGroupReq;
import org.apache.inlong.tubemq.manager.entry.MasterEntry;
import org.apache.inlong.tubemq.manager.enums.ErrorCode;
import org.apache.inlong.tubemq.manager.service.interfaces.BrokerService;
import org.apache.inlong.tubemq.manager.service.interfaces.MasterService;
import org.apache.inlong.tubemq.manager.service.interfaces.TopicService;
import org.apache.inlong.tubemq.manager.service.tube.CleanOffsetResult;
import org.apache.inlong.tubemq.manager.service.tube.RebalanceGroupResult;
import org.apache.inlong.tubemq.manager.service.tube.TopicView;
import org.apache.inlong.tubemq.manager.service.tube.TubeHttpGroupDetailInfo;
import org.apache.inlong.tubemq.manager.service.tube.TubeHttpTopicInfoList;
import org.apache.inlong.tubemq.manager.utils.ConvertUtils;
import org.apache.inlong.tubemq.manager.utils.HttpUtils;

import com.google.gson.Gson;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Objects;

/**
 * node service to query broker/master/standby status of tube cluster.
 */
@Slf4j
@Component
public class TopicServiceImpl implements TopicService {

    public static final int FIRST_TOPIC_INDEX = 0;
    public static final int MINIMUN_TOPIC_RUN_PART = 1;
    private final CloseableHttpClient httpclient = HttpClients.createDefault();
    private final Gson gson = new Gson();
    private static final Logger LOGGER = LoggerFactory.getLogger(TopicServiceImpl.class);

    @Value("${manager.broker.webPort:8081}")
    private int brokerWebPort;

    @Autowired
    private MasterService masterService;

    @Autowired
    private BrokerService brokerService;

    @Override
    public TubeHttpGroupDetailInfo requestGroupRunInfo(MasterEntry masterEntry, String group) {
        String url = TubeConst.SCHEMA + masterEntry.getIp() + ":" + masterEntry.getWebPort()
                + TubeConst.QUERY_GROUP_DETAIL_INFO + TubeConst.CONSUME_GROUP + group;
        HttpGet httpget = new HttpGet(url);
        try (CloseableHttpResponse response = httpclient.execute(httpget)) {
            TubeHttpGroupDetailInfo groupDetailInfo =
                    gson.fromJson(new InputStreamReader(response.getEntity()
                            .getContent(), StandardCharsets.UTF_8),
                            TubeHttpGroupDetailInfo.class);
            if (groupDetailInfo.getErrCode() == 0) {
                return groupDetailInfo;
            }
        } catch (Exception ex) {
            log.error("exception caught while requesting group status", ex);
        }
        return null;
    }

    @Override
    public TubeMQResult queryGroupExist(QueryConsumerGroupReq req) {
        MasterEntry masterNode = masterService.getMasterNode(req);
        TubeHttpGroupDetailInfo groupDetailInfo = requestGroupRunInfo(masterNode,
                req.getConsumerGroup());
        List<String> topicSet = groupDetailInfo.getTopicSet();
        if (topicSet.stream().anyMatch(topic -> topic.equals(req.getTopicName()))) {
            return TubeMQResult.successResult();
        }
        return TubeMQResult.errorResult(TubeMQErrorConst.NO_SUCH_GROUP);
    }

    @Override
    public TopicView requestTopicViewInfo(Long clusterId, String topicName) {
        MasterEntry masterNode = masterService.getMasterNode(clusterId);
        String host = masterNode.getIp();
        int port = masterNode.getWebPort();

        try {
            String requestUrl = TubeConst.SCHEMA + host + ":" + port + TubeConst.TOPIC_VIEW;
            if (StringUtils.isNotBlank(topicName)) {
                requestUrl += TubeConst.TOPIC_NAME + topicName;
            }
            String response = HttpUtils.sendHttpGetRequest(requestUrl);
            return gson.fromJson(response, TopicView.class);
        } catch (Exception ex) {
            log.error("Exception caught while requesting group status", ex);
            throw new RuntimeException(ex.getMessage());
        }
    }

    @Override
    public TubeMQResult cloneOffsetToOtherGroups(CloneOffsetReq req) {
        MasterEntry master = masterService.getMasterNode(Long.valueOf(req.getClusterId()));
        if (master == null) {
            return TubeMQResult.errorResult("no such cluster");
        }
        // query the corresponding brokers having given topic
        TubeHttpTopicInfoList topicInfoList = requestTopicConfigInfo(master, req.getTopicName());
        TubeMQResult result = new TubeMQResult();
        if (topicInfoList == null) {
            return result;
        }
        List<TubeHttpTopicInfoList.TopicInfoList.TopicInfo> topicInfos = topicInfoList.getTopicInfo();
        // for each broker, request to clone offset
        for (TubeHttpTopicInfoList.TopicInfoList.TopicInfo topicInfo : topicInfos) {
            result = brokerService.cloneOffset(topicInfo.getBrokerIp(),
                    brokerWebPort, req);
            if (result.getErrCode() != TubeConst.SUCCESS_CODE) {
                return result;
            }
        }
        return result;
    }

    @Override
    public TubeHttpTopicInfoList requestTopicConfigInfo(MasterEntry masterEntry, String topic) {
        String schema = TubeConst.SCHEMA.toLowerCase();
        if (!"http://".equals(schema) && !"https://".equals(schema)) {
            log.error("Invalid protocol in the URL: {}", schema);
            return null;
        }
        // Validate and clean the host IP address and web port
        String ip = validateAndCleanInput(masterEntry.getIp());
        int webPort = validateAndCheckPort(masterEntry.getWebPort());
        // Validate and clean the topic
        topic = validateAndCleanInput(topic);
        // Construct the URL
        String url = schema + ip + ":" + webPort + TubeConst.TOPIC_CONFIG_INFO + "&topicName=" + topic;
        // Log audit: Record the URL being requested
        LOGGER.info("Requesting topic config info from URL: {}", url);

        HttpGet httpget = new HttpGet(url);
        try (CloseableHttpResponse response = httpclient.execute(httpget)) {
            // Check for redirection, and reject if detected
            if (isRedirect(response)) {
                log.error("URL redirection detected. Requested URL: {}", url);
                return null;
            }

            TubeHttpTopicInfoList topicInfoList =
                    gson.fromJson(new InputStreamReader(response.getEntity()
                            .getContent(), StandardCharsets.UTF_8),
                            TubeHttpTopicInfoList.class);
            if (topicInfoList.getErrCode() == TubeConst.SUCCESS_CODE) {
                // Log audit: Record a successful request
                LOGGER.info("Topic config info request successful for URL: {}", url);
                return topicInfoList;
            } else {
                // Log audit: Record a failed request
                LOGGER.error("Topic config info request failed for URL: {}", url);
            }
        } catch (Exception ex) {
            // Log audit: Record an exception
            LOGGER.error("Exception caught while requesting topic config info for URL: {}", url, ex);
        }
        return null;
    }

    private String validateAndCleanInput(String input) {
        input = input.replaceAll("[^a-zA-Z0-9.]", "");
        return input;
    }

    private int validateAndCheckPort(int port) {
        // Add port validation, ensuring it falls within a valid range
        if (port >= 1 && port <= 65535) {
            return port;
        } else {
            log.error("Invalid port number: {}", port);
            throw new IllegalArgumentException("Invalid port number.");
        }
    }

    private boolean isRedirect(HttpResponse response) {
        // Check if the response contains redirection status codes
        int statusCode = response.getStatusLine().getStatusCode();
        return statusCode == HttpStatus.SC_MOVED_PERMANENTLY || statusCode == HttpStatus.SC_MOVED_TEMPORARILY;
    }

    @Override
    public TubeMQResult rebalanceGroup(RebalanceGroupReq req) {

        MasterEntry master = masterService.getMasterNode(Long.valueOf(req.getClusterId()));
        if (master == null) {
            return TubeMQResult.errorResult("no such cluster");
        }

        // 1. get all consumer ids in group
        List<String> consumerIds = Objects
                .requireNonNull(requestGroupRunInfo(master, req.getGroupName())).getConsumerIds();
        RebalanceGroupResult rebalanceGroupResult = new RebalanceGroupResult();

        // 2. rebalance consumers in group
        consumerIds.forEach(consumerId -> {
            RebalanceConsumerReq rebalanceConsumerReq = ConvertUtils.convertToRebalanceConsumerReq(req,
                    consumerId);
            String url = TubeConst.SCHEMA + master.getIp() + ":" + master.getWebPort()
                    + "/" + TubeConst.TUBE_REQUEST_PATH + "?" + ConvertUtils
                            .convertReqToQueryStr(rebalanceConsumerReq);
            TubeMQResult result = masterService.requestMaster(url);
            if (result.getErrCode() != 0) {
                rebalanceGroupResult.getFailConsumers().add(consumerId);
            }
            rebalanceGroupResult.getSuccessConsumers().add(consumerId);
        });

        TubeMQResult tubeResult = new TubeMQResult();
        tubeResult.setData(gson.toJson(rebalanceGroupResult));
        return tubeResult;
    }

    @Override
    public TubeMQResult deleteOffset(DeleteOffsetReq req) {

        MasterEntry master = masterService.getMasterNode(Long.valueOf(req.getClusterId()));
        if (master == null) {
            return TubeMQResult.errorResult("no such cluster");
        }

        // 1. query the corresponding brokers having given topic
        TubeHttpTopicInfoList topicInfoList = requestTopicConfigInfo(master, req.getTopicName());
        TubeMQResult result = new TubeMQResult();
        CleanOffsetResult cleanOffsetResult = new CleanOffsetResult();
        if (topicInfoList == null) {
            return TubeMQResult.errorResult("no such topic");
        }

        List<TubeHttpTopicInfoList.TopicInfoList.TopicInfo> topicInfos = topicInfoList.getTopicInfo();
        // 2. for each broker, request to delete offset
        for (TubeHttpTopicInfoList.TopicInfoList.TopicInfo topicInfo : topicInfos) {
            String brokerIp = topicInfo.getBrokerIp();
            result = brokerService.deleteOffset(brokerIp, brokerWebPort, req);
            if (result.getErrCode() != TubeConst.SUCCESS_CODE) {
                cleanOffsetResult.getFailBrokers().add(brokerIp);
            } else {
                cleanOffsetResult.getSuccessBrokers().add(brokerIp);
            }
        }
        result.setData(gson.toJson(cleanOffsetResult));
        return result;
    }

    @Override
    public TubeMQResult queryOffset(QueryOffsetReq req) {

        MasterEntry master = masterService.getMasterNode(Long.valueOf(req.getClusterId()));
        if (master == null) {
            return TubeMQResult.errorResult("no such cluster");
        }

        // 1. query the corresponding brokers having given topic
        TubeHttpTopicInfoList topicInfoList = requestTopicConfigInfo(master, req.getTopicName());
        TubeMQResult result = new TubeMQResult();
        if (topicInfoList == null) {
            return TubeMQResult.errorResult("no such topic");
        }

        List<TubeHttpTopicInfoList.TopicInfoList.TopicInfo> topicInfos = topicInfoList.getTopicInfo();

        AllBrokersOffsetRes allBrokersOffsetRes = new AllBrokersOffsetRes();
        List<OffsetInfo> offsetPerBroker = allBrokersOffsetRes.getOffsetPerBroker();

        // 2. for each broker, request to query offset
        for (TubeHttpTopicInfoList.TopicInfoList.TopicInfo topicInfo : topicInfos) {
            OffsetQueryRes res = brokerService.queryOffset(topicInfo.getBrokerIp(), brokerWebPort, req);
            if (res.getErrCode() != TubeConst.SUCCESS_CODE) {
                return TubeMQResult.errorResult("query broker id" + topicInfo.getBrokerId() + " fail");
            }
            generateOffsetInfo(offsetPerBroker, topicInfo, res);
        }

        result.setData(allBrokersOffsetRes);
        return result;
    }

    @Override
    public TubeMQResult queryCanWrite(String topicName, Long clusterId) {
        TopicView topicView = requestTopicViewInfo(clusterId, topicName);
        List<TopicView.TopicViewInfo> data = topicView.getData();
        if (CollectionUtils.isEmpty(data)) {
            return TubeMQResult.errorResult(ErrorCode.NO_SUCH_TOPIC);
        }
        TopicView.TopicViewInfo topicViewInfo = data.get(FIRST_TOPIC_INDEX);
        if (topicViewInfo.getTotalRunNumPartCount() >= MINIMUN_TOPIC_RUN_PART) {
            return TubeMQResult.successResult();
        }
        return TubeMQResult.errorResult(ErrorCode.TOPIC_NOT_WRITABLE);
    }

    private void generateOffsetInfo(List<OffsetInfo> offsetPerBroker,
            TubeHttpTopicInfoList.TopicInfoList.TopicInfo topicInfo,
            OffsetQueryRes res) {
        OffsetInfo offsetInfo = new OffsetInfo();
        offsetInfo.setBrokerId(topicInfo.getBrokerId());
        offsetInfo.setBrokerIp(topicInfo.getBrokerIp());
        if (TubeConst.SUCCESS_CODE == res.getErrCode()) {
            List<GroupOffsetRes> dataSet = res.getDataSet();
            for (GroupOffsetRes groupOffsetRes : dataSet) {
                for (TopicOffsetRes topicOffsetRes : groupOffsetRes.getSubInfo()) {
                    List<OffsetPartitionRes> offsets = topicOffsetRes.getOffsets();
                    offsetInfo.setOffsets(offsets);
                }
            }
            offsetPerBroker.add(offsetInfo);
        }
    }
}
