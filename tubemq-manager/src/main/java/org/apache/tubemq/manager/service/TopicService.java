/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.tubemq.manager.service;


import static org.apache.tubemq.manager.service.TubeMQHttpConst.QUERY_GROUP_DETAIL_INFO;
import static org.apache.tubemq.manager.service.TubeMQHttpConst.SCHEMA;
import static org.apache.tubemq.manager.service.TubeMQHttpConst.TOPIC_CONFIG_INFO;
import static org.apache.tubemq.manager.utils.ConvertUtils.convertReqToQueryStr;
import static org.apache.tubemq.manager.utils.ConvertUtils.convertToRebalanceConsumerReq;
import static org.apache.tubemq.manager.service.MasterService.SUCCESS_CODE;
import static org.apache.tubemq.manager.service.MasterService.TUBE_REQUEST_PATH;
import static org.apache.tubemq.manager.service.MasterService.requestMaster;

import com.google.gson.Gson;
import java.io.InputStreamReader;
import java.util.List;
import java.util.Objects;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.tubemq.manager.controller.TubeMQResult;
import org.apache.tubemq.manager.controller.group.request.DeleteOffsetReq;
import org.apache.tubemq.manager.controller.node.request.CloneOffsetReq;
import org.apache.tubemq.manager.controller.topic.request.BatchAddGroupAuthReq;
import org.apache.tubemq.manager.controller.topic.request.DeleteGroupReq;
import org.apache.tubemq.manager.controller.topic.request.RebalanceConsumerReq;
import org.apache.tubemq.manager.controller.topic.request.RebalanceGroupReq;
import org.apache.tubemq.manager.entry.NodeEntry;
import org.apache.tubemq.manager.repository.NodeRepository;
import org.apache.tubemq.manager.service.tube.CleanOffsetResult;
import org.apache.tubemq.manager.service.tube.RebalanceGroupResult;
import org.apache.tubemq.manager.service.tube.TubeHttpGroupDetailInfo;
import org.apache.tubemq.manager.service.tube.TubeHttpTopicInfoList;
import org.apache.tubemq.manager.service.tube.TubeHttpTopicInfoList.TopicInfoList.TopicInfo;
import org.apache.tubemq.manager.utils.ConvertUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

/**
 * node service to query broker/master/standby status of tube cluster.
 */
@Slf4j
@Component
public class TopicService {

    @Autowired
    private NodeRepository nodeRepository;

    private final CloseableHttpClient httpclient = HttpClients.createDefault();
    private final Gson gson = new Gson();

    @Value("${manager.broker.webPort:8081}")
    private int brokerWebPort;


    public TubeMQResult addConsumer(
        BatchAddGroupAuthReq req) {
        NodeEntry nodeEntry =
            nodeRepository.findNodeEntryByClusterIdIsAndMasterIsTrue(req.getClusterId());
        if (nodeEntry == null) {
            return TubeMQResult.getErrorResult("no such cluster");
        }
        String url = SCHEMA + nodeEntry.getIp() + ":" + nodeEntry.getWebPort()
            + "/" + TUBE_REQUEST_PATH + "?" + ConvertUtils.convertReqToQueryStr(req);

        return requestMaster(url);
    }


    public TubeMQResult deleteConsumer(DeleteGroupReq req) {
        NodeEntry nodeEntry =
            nodeRepository.findNodeEntryByClusterIdIsAndMasterIsTrue(req.getClusterId());
        if (nodeEntry == null) {
            return TubeMQResult.getErrorResult("no such cluster");
        }
        String url = SCHEMA + nodeEntry.getIp() + ":" + nodeEntry.getWebPort()
            + "/" + TUBE_REQUEST_PATH + "?" + ConvertUtils.convertReqToQueryStr(req);
        return requestMaster(url);
    }


    public TubeMQResult cloneOffset(CloneOffsetReq req) {
        if (req.getClusterId() == null) {
            return TubeMQResult.getErrorResult("please input clusterId");
        }
        NodeEntry masterEntry = nodeRepository.findNodeEntryByClusterIdIsAndMasterIsTrue(
            req.getClusterId());
        if (masterEntry == null) {
            return TubeMQResult.getErrorResult("no such cluster");
        }
        return cloneOffsetToOtherGroups(req, masterEntry);
    }

    private TubeHttpGroupDetailInfo requestGroupRunInfo(NodeEntry nodeEntry, String group) {
        String url = SCHEMA + nodeEntry.getIp() + ":" + nodeEntry.getWebPort()
            + QUERY_GROUP_DETAIL_INFO + "&consumeGroup=" + group;
        HttpGet httpget = new HttpGet(url);
        try (CloseableHttpResponse response = httpclient.execute(httpget)) {
            TubeHttpGroupDetailInfo groupDetailInfo =
                gson.fromJson(new InputStreamReader(response.getEntity().getContent()),
                    TubeHttpGroupDetailInfo.class);
            if (groupDetailInfo.getErrCode() == 0) {
                return groupDetailInfo;
            }
        } catch (Exception ex) {
            log.error("exception caught while requesting group status", ex);
        }
        return null;
    }


    public TubeMQResult cloneOffsetToOtherGroups(CloneOffsetReq req, NodeEntry master) {

        // 1. query the corresponding brokers having given topic
        TubeHttpTopicInfoList topicInfoList = requestTopicConfigInfo(master, req.getTopicName());
        TubeMQResult result = new TubeMQResult();

        if (topicInfoList == null) {
            return result;
        }

        List<TopicInfo> topicInfos = topicInfoList.getTopicInfo();
        // 2. for each broker, request to clone offset
        for (TopicInfo topicInfo : topicInfos) {
            String brokerIp = topicInfo.getBrokerIp();
            String url = SCHEMA + brokerIp + ":" + brokerWebPort
                + "/" + TUBE_REQUEST_PATH + "?" + convertReqToQueryStr(req);
            result = requestMaster(url);
            if (result.getErrCode() != SUCCESS_CODE) {
                return result;
            }
        }

        return result;
    }


    public TubeHttpTopicInfoList requestTopicConfigInfo(NodeEntry nodeEntry, String topic) {
        String url = SCHEMA + nodeEntry.getIp() + ":" + nodeEntry.getWebPort()
            + TOPIC_CONFIG_INFO + "&topicName=" + topic;
        HttpGet httpget = new HttpGet(url);
        try (CloseableHttpResponse response = httpclient.execute(httpget)) {
            TubeHttpTopicInfoList topicInfoList =
                gson.fromJson(new InputStreamReader(response.getEntity().getContent()),
                    TubeHttpTopicInfoList.class);
            if (topicInfoList.getErrCode() == SUCCESS_CODE) {
                return topicInfoList;
            }
        } catch (Exception ex) {
            log.error("exception caught while requesting broker status", ex);
        }
        return null;
    }


    public TubeMQResult rebalanceGroup(RebalanceGroupReq req) {

        if (req.getClusterId() == null) {
            return TubeMQResult.getErrorResult("please input clusterId");
        }
        NodeEntry master = nodeRepository.findNodeEntryByClusterIdIsAndMasterIsTrue(
            req.getClusterId());
        if (master == null) {
            return TubeMQResult.getErrorResult("no such cluster");
        }

        // 1. get all consumer ids in group
        List<String> consumerIds = Objects
            .requireNonNull(requestGroupRunInfo(master, req.getGroupName())).getConsumerIds();
        RebalanceGroupResult rebalanceGroupResult = new RebalanceGroupResult();

        // 2. rebalance consumers in group
        consumerIds.forEach(consumerId -> {
            RebalanceConsumerReq rebalanceConsumerReq = convertToRebalanceConsumerReq(req,
                consumerId);
            String url = SCHEMA + master.getIp() + ":" + master.getWebPort()
                + "/" + TUBE_REQUEST_PATH + "?" + convertReqToQueryStr(rebalanceConsumerReq);
            TubeMQResult result = requestMaster(url);
            if (result.getErrCode() != 0) {
                rebalanceGroupResult.getFailConsumers().add(consumerId);
            }
            rebalanceGroupResult.getSuccessConsumers().add(consumerId);
        });

        TubeMQResult tubeResult = new TubeMQResult();
        tubeResult.setData(gson.toJson(rebalanceGroupResult));

        return tubeResult;
    }


    public TubeMQResult deleteOffset(DeleteOffsetReq req) {

        if (req.getClusterId() == null) {
            return TubeMQResult.getErrorResult("please input clusterId");
        }
        NodeEntry masterEntry = nodeRepository.findNodeEntryByClusterIdIsAndMasterIsTrue(
            req.getClusterId());
        if (masterEntry == null) {
            return TubeMQResult.getErrorResult("no such cluster");
        }

        // 1. query the corresponding brokers having given topic
        TubeHttpTopicInfoList topicInfoList = requestTopicConfigInfo(masterEntry, req.getTopicName());
        TubeMQResult result = new TubeMQResult();
        CleanOffsetResult cleanOffsetResult = new CleanOffsetResult();
        if (topicInfoList == null) {
            return TubeMQResult.getErrorResult("no such topic");
        }

        List<TopicInfo> topicInfos = topicInfoList.getTopicInfo();
        // 2. for each broker, request to delete offset
        for (TopicInfo topicInfo : topicInfos) {
            String brokerIp = topicInfo.getBrokerIp();
            String url = SCHEMA + brokerIp + ":" + brokerWebPort
                + "/" + TUBE_REQUEST_PATH + "?" + convertReqToQueryStr(req);
            result = requestMaster(url);
            if (result.getErrCode() != SUCCESS_CODE) {
                cleanOffsetResult.getFailBrokers().add(brokerIp);
            } else {
                cleanOffsetResult.getSuccessBrokers().add(brokerIp);
            }
        }

        result.setData(gson.toJson(cleanOffsetResult));

        return result;
    }

}
