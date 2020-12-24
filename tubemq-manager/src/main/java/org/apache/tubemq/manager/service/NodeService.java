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


import static org.apache.tubemq.manager.service.TubeMQHttpConst.ADD_TUBE_TOPIC;
import static org.apache.tubemq.manager.service.TubeMQHttpConst.BROKER_RUN_STATUS;
import static org.apache.tubemq.manager.service.TubeMQHttpConst.RELOAD_BROKER;
import static org.apache.tubemq.manager.service.TubeMQHttpConst.SCHEMA;
import static org.apache.tubemq.manager.service.TubeMQHttpConst.TOPIC_CONFIG_INFO;

import com.google.gson.Gson;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.tubemq.manager.controller.TubeMQResult;
import org.apache.tubemq.manager.entry.NodeEntry;
import org.apache.tubemq.manager.repository.NodeRepository;
import org.apache.tubemq.manager.service.tube.TubeHttpBrokerInfoList;
import org.apache.tubemq.manager.service.tube.TubeHttpClusterInfoList;
import org.apache.tubemq.manager.service.tube.TubeHttpResponse;
import org.apache.tubemq.manager.service.tube.TubeHttpTopicInfoList;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

/**
 * node service to query broker/master/standby status of tube cluster.
 */
@Slf4j
@Component
public class NodeService {

    private final CloseableHttpClient httpclient = HttpClients.createDefault();
    private final Gson gson = new Gson();

    @Value("${manager.max.configurable.broker.size:50}")
    private int maxConfigurableBrokerSize;

    @Value("${manager.max.retry.adding.topic:10}")
    private int maxRetryAddingTopic;

    private final TopicBackendWorker worker;

    @Autowired
    private NodeRepository nodeRepository;

    public NodeService(TopicBackendWorker worker) {
        this.worker = worker;
    }

    /**
     * request node status via http.
     *
     * @param nodeEntry - node entry
     * @return
     * @throws IOException
     */
    private TubeHttpBrokerInfoList requestClusterNodeStatus(NodeEntry nodeEntry) throws IOException {
        String url = SCHEMA + nodeEntry.getIp() + ":" + nodeEntry.getWebPort() + BROKER_RUN_STATUS;
        HttpGet httpget = new HttpGet(url);
        try (CloseableHttpResponse response = httpclient.execute(httpget)) {
            TubeHttpBrokerInfoList brokerInfoList =
                    gson.fromJson(new InputStreamReader(response.getEntity().getContent()),
                            TubeHttpBrokerInfoList.class);
            // request return normal.
            if (brokerInfoList.getCode() == 0) {
                // divide by state.
                brokerInfoList.divideBrokerListByState();
                return brokerInfoList;
            }
        } catch (Exception ex) {
            log.error("exception caught while requesting broker status", ex);
        }
        return null;
    }


    private TubeHttpTopicInfoList requestTopicConfigInfo(NodeEntry nodeEntry, String topic) {
        String url = SCHEMA + nodeEntry.getIp() + ":" + nodeEntry.getWebPort()
                + TOPIC_CONFIG_INFO + "&topicName=" + topic;
        HttpGet httpget = new HttpGet(url);
        try (CloseableHttpResponse response = httpclient.execute(httpget)) {
            TubeHttpTopicInfoList topicInfoList =
                    gson.fromJson(new InputStreamReader(response.getEntity().getContent()),
                            TubeHttpTopicInfoList.class);
            if (topicInfoList.getErrCode() == 0) {
                return topicInfoList;
            }
        } catch (Exception ex) {
            log.error("exception caught while requesting broker status", ex);
        }
        return null;
    }


    private boolean configBrokersForTopics(NodeEntry nodeEntry,
            Set<String> topics, List<Integer> brokerList, int maxBrokers) {
        List<Integer> finalBrokerList = brokerList.subList(0, maxBrokers);
        String brokerStr = StringUtils.join(finalBrokerList, ",");
        String topicStr = StringUtils.join(topics, ",");
        String url = SCHEMA + nodeEntry.getIp() + ":" + nodeEntry.getWebPort()
                + ADD_TUBE_TOPIC  + "&topicName=" + topicStr + "&brokerId=" + brokerStr;
        HttpGet httpget = new HttpGet(url);
        try (CloseableHttpResponse response = httpclient.execute(httpget)) {
            TubeHttpResponse result =
                    gson.fromJson(new InputStreamReader(response.getEntity().getContent()),
                            TubeHttpResponse.class);
            return result.getCode() == 0 && result.getErrCode() == 0;
        } catch (Exception ex) {
            log.error("exception caught while requesting broker status", ex);
        }
        return false;
    }

    /**
     * handle result, if success, complete it,
     * if not success, add back to queue without exceeding max retry,
     * otherwise complete it with exception.
     *
     * @param isSuccess
     * @param topics
     * @param pendingTopic
     */
    private void handleAddingResult(boolean isSuccess, Set<String> topics,
            Map<String, TopicFuture> pendingTopic) {
        for (String topic : topics) {
            TopicFuture future = pendingTopic.get(topic);
            if (future != null) {
                if (isSuccess) {
                    future.complete();
                } else {
                    future.increaseRetryTime();
                    if (future.getRetryTime() > maxRetryAddingTopic) {
                        future.completeExceptional();
                    } else {
                        // add back to queue.
                        worker.addTopicFuture(future);
                    }
                }
            }
        }
    }


    /**
     * Adding topic is an async operation, so this method should
     * 1. check whether pendingTopic contains topic that has failed/succeeded to be added.
     * 2. async add topic to tubemq cluster
     *
     * @param brokerInfoList - broker list
     * @param pendingTopic - topicMap
     */
    private void handleAddingTopic(NodeEntry nodeEntry,
            TubeHttpBrokerInfoList brokerInfoList,
            Map<String, TopicFuture> pendingTopic) {
        // 1. check tubemq cluster by topic name, remove pending topic if has added.
        Set<String> brandNewTopics = new HashSet<>();
        for (String topic : pendingTopic.keySet()) {
            TubeHttpTopicInfoList topicInfoList = requestTopicConfigInfo(nodeEntry, topic);
            if (topicInfoList != null) {
                // get broker list by topic request
                List<Integer> topicBrokerList = topicInfoList.getTopicBrokerIdList();
                if (topicBrokerList.isEmpty()) {
                    brandNewTopics.add(topic);
                } else {
                    // remove brokers which have been added.
                    List<Integer> configurableBrokerIdList =
                            brokerInfoList.getConfigurableBrokerIdList();
                    configurableBrokerIdList.removeAll(topicBrokerList);
                    // add topic to satisfy max broker number.
                    Set<String> singleTopic = new HashSet<>();
                    singleTopic.add(topic);
                    int maxBrokers = maxConfigurableBrokerSize - topicBrokerList.size();
                    boolean isSuccess = configBrokersForTopics(nodeEntry, singleTopic,
                            configurableBrokerIdList, maxBrokers);
                    handleAddingResult(isSuccess, singleTopic, pendingTopic);
                }
            }
        }
        // 2. add new topics to cluster
        List<Integer> configurableBrokerIdList = brokerInfoList.getConfigurableBrokerIdList();
        int maxBrokers = Math.min(maxConfigurableBrokerSize, configurableBrokerIdList.size());
        boolean isSuccess = configBrokersForTopics(nodeEntry, brandNewTopics,
                configurableBrokerIdList, maxBrokers);
        handleAddingResult(isSuccess, brandNewTopics, pendingTopic);
    }

    /**
     * reload broker list, cannot exceed maxConfigurableBrokerSize each time.
     *
     * @param nodeEntry
     * @param needReloadList
     */
    private void handleReloadBroker(NodeEntry nodeEntry, List<Integer> needReloadList) {
        // reload without exceed max broker.
        int begin = 0;
        int end = 0;
        do {
            end = Math.min(maxConfigurableBrokerSize + begin, needReloadList.size());
            List<Integer> brokerIdList = needReloadList.subList(begin, end);
            String brokerStr = StringUtils.join(brokerIdList, ",");
            String url = SCHEMA + nodeEntry.getIp() + ":" + nodeEntry.getWebPort()
                    + RELOAD_BROKER + "&brokerId=" + brokerStr;
            HttpGet httpget = new HttpGet(url);
            try (CloseableHttpResponse response = httpclient.execute(httpget)) {
                TubeHttpResponse result =
                        gson.fromJson(new InputStreamReader(response.getEntity().getContent()),
                                TubeHttpResponse.class);
                if (result.getErrCode() == 0 && result.getCode() == 0) {
                    log.info("reload tube broker cgi: " +
                            url + " ; return value : " + result.getCode());
                }
            } catch (Exception ex) {
                log.error("exception caught while requesting broker status", ex);
            }
            begin = end;
        } while (end >= needReloadList.size());
    }



    /**
     * update broker status
     */
    public void updateBrokerStatus(int clusterId, Map<String, TopicFuture> pendingTopic) {
        NodeEntry nodeEntry = nodeRepository.findNodeEntryByClusterIdIsAndMasterIsTrue(clusterId);
        if (nodeEntry != null) {
            try {
                TubeHttpBrokerInfoList brokerInfoList = requestClusterNodeStatus(nodeEntry);
                if (brokerInfoList != null) {
                    handleAddingTopic(nodeEntry, brokerInfoList, pendingTopic);
                }

                // refresh broker list
                brokerInfoList = requestClusterNodeStatus(nodeEntry);
                if (brokerInfoList != null) {
                    handleReloadBroker(nodeEntry, brokerInfoList.getNeedReloadList());
                }

            } catch (Exception ex) {
                log.error("exception caught while requesting broker status", ex);
            }
        } else {
            log.error("cannot get master ip by clusterId {}, please check it", clusterId);
        }
    }

    public String queryClusterInfo(Integer clusterId) {
        TubeHttpClusterInfoList clusterInfoList;
        try {
            // find all nodes by given clusterIds, show all nodes if clusterIds not provided
            List<NodeEntry> nodeEntries = clusterId == null ?
                    nodeRepository.findAll() : nodeRepository.findNodeEntriesByClusterIdIs(clusterId);
            // divide all entries by clusterId
            Map<Integer, List<NodeEntry>> nodeEntriesPerCluster =
                    nodeEntries.parallelStream().collect(Collectors.groupingBy(NodeEntry::getClusterId));

            clusterInfoList = TubeHttpClusterInfoList.getClusterInfoList(nodeEntriesPerCluster);
        } catch (Exception e) {
            log.error("query cluster info error", e);
            return gson.toJson(TubeMQResult.getErrorResult(""));
        }

        return gson.toJson(clusterInfoList);
    }



    public void close() throws IOException {
        httpclient.close();
    }
}
