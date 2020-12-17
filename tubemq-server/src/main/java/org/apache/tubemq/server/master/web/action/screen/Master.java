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

package org.apache.tubemq.server.master.web.action.screen;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import javax.servlet.http.HttpServletRequest;
import org.apache.tubemq.corebase.TokenConstants;
import org.apache.tubemq.corebase.cluster.BrokerInfo;
import org.apache.tubemq.corebase.cluster.ConsumerInfo;
import org.apache.tubemq.corebase.cluster.Partition;
import org.apache.tubemq.corebase.cluster.ProducerInfo;
import org.apache.tubemq.corebase.cluster.TopicInfo;
import org.apache.tubemq.corebase.utils.ConcurrentHashSet;
import org.apache.tubemq.corebase.utils.TStringUtils;
import org.apache.tubemq.corerpc.exception.StandbyException;
import org.apache.tubemq.server.master.TMaster;
import org.apache.tubemq.server.master.bdbstore.bdbentitys.BdbTopicConfEntity;
import org.apache.tubemq.server.master.nodemanage.nodebroker.BrokerConfManager;
import org.apache.tubemq.server.master.nodemanage.nodebroker.TopicPSInfoManager;
import org.apache.tubemq.server.master.nodemanage.nodeconsumer.ConsumerInfoHolder;
import org.apache.tubemq.server.master.web.simplemvc.Action;
import org.apache.tubemq.server.master.web.simplemvc.RequestContext;


public class Master implements Action {

    private TMaster master;

    public Master(TMaster master) {
        this.master = master;
    }

    @Override
    public void execute(RequestContext requestContext) {
        StringBuilder sBuilder = new StringBuilder(512);
        try {
            HttpServletRequest req = requestContext.getReq();
            if (this.master.isStopped()) {
                throw new Exception("Sever is stopping...");
            }
            BrokerConfManager brokerConfManager =
                    this.master.getMasterTopicManager();
            if (!brokerConfManager.isSelfMaster()) {
                throw new StandbyException("Please send your request to the master Node.");
            }
            String type = req.getParameter("type");
            if ("consumer".equals(type)) {
                getConsumerListInfo(req, sBuilder);
            } else if ("sub_info".equals(type)) {
                getConsumerSubInfo(req, sBuilder);
            } else if ("producer".equals(type)) {
                getProducerListInfo(req, sBuilder);
            } else if ("broker".equals(type)) {
                innGetBrokerInfo(req, sBuilder, true);
            } else if ("newBroker".equals(type)) {
                innGetBrokerInfo(req, sBuilder, false);
            } else if ("topic_pub".equals(type)) {
                getTopicPubInfo(req, sBuilder);
            } else if ("unbalance_group".equals(type)) {
                getUnbalanceGroupInfo(sBuilder);
            } else {
                sBuilder.append("Unsupported request type : ").append(type);
            }
            requestContext.put("sb", sBuilder.toString());
        } catch (Exception e) {
            requestContext.put("sb", "Bad request from client. " + e.getMessage());
        }
    }

    /**
     * Get consumer list info
     *
     * @param req
     * @param sBuilder
     * @return
     */
    private void getConsumerListInfo(final HttpServletRequest req, StringBuilder sBuilder) {
        ConsumerInfoHolder consumerHolder = master.getConsumerHolder();
        String group = req.getParameter("group");
        if (group != null) {
            List<ConsumerInfo> consumerList = consumerHolder.getConsumerList(group);
            int index = 1;
            if (consumerList != null && !consumerList.isEmpty()) {
                Collections.sort(consumerList);
                for (ConsumerInfo consumer : consumerList) {
                    sBuilder.append(index).append(". ").append(consumer.toString()).append("\n");
                    index++;
                }
            } else {
                sBuilder.append("No such group.\n\nCurrent all groups");
                List<String> groupList = consumerHolder.getAllGroup();
                sBuilder.append("(").append(groupList.size()).append("):\n");
                for (String currGroup : groupList) {
                    sBuilder.append(currGroup).append("\n");
                }
            }
        }
    }

    /**
     * Get consumer subscription info
     *
     * @param req
     * @param sBuilder
     * @return
     */
    private void getConsumerSubInfo(final HttpServletRequest req, StringBuilder sBuilder) {
        ConsumerInfoHolder consumerHolder = master.getConsumerHolder();
        String group = req.getParameter("group");
        if (group != null) {
            List<ConsumerInfo> consumerList = consumerHolder.getConsumerList(group);
            if (consumerList != null && !consumerList.isEmpty()) {
                Collections.sort(consumerList);
                sBuilder.append("\n########################## Subscribe Relationship ############################\n\n");
                Map<String, Map<String, Map<String, Partition>>> currentSubInfoMap =
                        master.getCurrentSubInfoMap();
                for (int i = 0; i < consumerList.size(); i++) {
                    ConsumerInfo consumer = consumerList.get(i);
                    sBuilder.append("*************** ").append(i + 1)
                            .append(". ").append(consumer.getConsumerId())
                            .append("#isOverTLS=").append(consumer.isOverTLS())
                            .append(" ***************");
                    Map<String, Map<String, Partition>> topicSubMap =
                            currentSubInfoMap.get(consumer.getConsumerId());
                    if (topicSubMap != null) {
                        int totalSize = 0;
                        for (Map.Entry<String, Map<String, Partition>> entry : topicSubMap.entrySet()) {
                            totalSize += entry.getValue().size();
                        }
                        sBuilder.append("(").append(totalSize).append(")\n\n");
                        for (Map.Entry<String, Map<String, Partition>> entry : topicSubMap.entrySet()) {
                            Map<String, Partition> partMap = entry.getValue();
                            if (partMap != null) {
                                for (Partition part : partMap.values()) {
                                    sBuilder.append(consumer.getConsumerId())
                                            .append("#").append(part.toString()).append("\n");
                                }
                            }
                        }
                    }
                    sBuilder.append("\n\n");
                }
            } else {
                sBuilder.append("No such group.\n\nCurrent all group");
                List<String> groupList = consumerHolder.getAllGroup();
                sBuilder.append("(").append(groupList.size()).append("):\n");
                for (String currGroup : groupList) {
                    sBuilder.append(currGroup).append("\n");
                }
            }
        }
    }

    /**
     * Get producer list info
     *
     * @param req
     * @param sBuilder
     * @return
     */
    private void getProducerListInfo(final HttpServletRequest req, StringBuilder sBuilder) {
        String producerId = req.getParameter("id");
        if (producerId != null) {
            ProducerInfo producer = master.getProducerHolder().getProducerInfo(producerId);
            if (producer != null) {
                sBuilder.append(producer.toString());
            } else {
                sBuilder.append("No such producer!");
            }
        } else {
            String topic = req.getParameter("topic");
            if (topic != null) {
                TopicPSInfoManager topicPSInfoManager =
                        master.getTopicPSInfoManager();
                ConcurrentHashSet<String> producerSet =
                        topicPSInfoManager.getTopicPubInfo(topic);
                if (producerSet != null && !producerSet.isEmpty()) {
                    int index = 1;
                    for (String producer : producerSet) {
                        sBuilder.append(index).append(". ").append(producer).append("\n");
                        index++;
                    }
                }
            }
        }
    }

    /**
     * Get broker info
     *
     * @param req
     * @param sBuilder
     * @param isOldRet
     * @return
     */
    private void innGetBrokerInfo(final HttpServletRequest req,
                                           StringBuilder sBuilder, boolean isOldRet) {
        Map<Integer, BrokerInfo> brokerInfoMap = null;
        String brokerIds = req.getParameter("ids");
        if (TStringUtils.isBlank(brokerIds)) {
            brokerInfoMap = master.getBrokerHolder().getBrokerInfoMap();
        } else {
            String[] brokerIdArr = brokerIds.split(",");
            List<Integer> idList = new ArrayList<>(brokerIdArr.length);
            for (String strId : brokerIdArr) {
                idList.add(Integer.parseInt(strId));
            }
            brokerInfoMap = master.getBrokerHolder().getBrokerInfos(idList);
        }
        if (brokerInfoMap != null) {
            int index = 1;
            for (BrokerInfo broker : brokerInfoMap.values()) {
                sBuilder.append("\n################################## ")
                        .append(index).append(". ").append(broker.toString())
                        .append(" ##################################\n");
                TopicPSInfoManager topicPSInfoManager = master.getTopicPSInfoManager();
                List<TopicInfo> topicInfoList = topicPSInfoManager.getBrokerPubInfoList(broker);
                ConcurrentHashMap<String, BdbTopicConfEntity> topicConfigMap =
                        master.getMasterTopicManager().getBrokerTopicConfEntitySet(broker.getBrokerId());
                if (topicConfigMap == null) {
                    for (TopicInfo info : topicInfoList) {
                        sBuilder = info.toStrBuilderString(sBuilder);
                        sBuilder.append("\n");

                    }
                } else {
                    for (TopicInfo info : topicInfoList) {
                        BdbTopicConfEntity bdbEntity = topicConfigMap.get(info.getTopic());
                        if (bdbEntity == null) {
                            sBuilder = info.toStrBuilderString(sBuilder);
                            sBuilder.append("\n");
                        } else {
                            if (isOldRet) {
                                if (bdbEntity.isValidTopicStatus()) {
                                    sBuilder = info.toStrBuilderString(sBuilder);
                                    sBuilder.append("\n");
                                }
                            } else {
                                sBuilder = info.toStrBuilderString(sBuilder);
                                sBuilder.append(TokenConstants.SEGMENT_SEP)
                                        .append(bdbEntity.getTopicStatusId()).append("\n");
                            }
                        }
                    }
                }
                index++;
            }
        }
    }

    /**
     * Get topic publish info
     *
     * @param req
     * @param sBuilder
     * @return
     */
    private void getTopicPubInfo(final HttpServletRequest req, StringBuilder sBuilder) {
        String topic = req.getParameter("topic");
        Set<String> producerIds = master.getTopicPSInfoManager().getTopicPubInfo(topic);
        if (producerIds != null && !producerIds.isEmpty()) {
            for (String producerId : producerIds) {
                sBuilder.append(producerId).append("\n");
            }
        } else {
            sBuilder.append("No producer has publish this topic.");
        }
    }

    /**
     * Get un-balanced group info
     *
     * @param sBuilder
     * @return
     */
    private void getUnbalanceGroupInfo(StringBuilder sBuilder) {
        ConsumerInfoHolder consumerHolder = master.getConsumerHolder();
        TopicPSInfoManager topicPSInfoManager = master.getTopicPSInfoManager();
        Map<String, Map<String, Map<String, Partition>>> currentSubInfoMap =
                master.getCurrentSubInfoMap();
        List<String> groupList = consumerHolder.getAllGroup();
        for (String group : groupList) {
            Set<String> topicSet = consumerHolder.getGroupTopicSet(group);
            for (String topic : topicSet) {
                int currPartSize = 0;
                List<ConsumerInfo> consumerList = consumerHolder.getConsumerList(group);
                for (ConsumerInfo consumer : consumerList) {
                    Map<String, Map<String, Partition>> consumerSubInfoMap =
                            currentSubInfoMap.get(consumer.getConsumerId());
                    if (consumerSubInfoMap != null) {
                        Map<String, Partition> topicSubInfoMap =
                                consumerSubInfoMap.get(topic);
                        if (topicSubInfoMap != null) {
                            currPartSize += topicSubInfoMap.size();
                        }
                    }
                }
                List<Partition> partList = topicPSInfoManager.getPartitionList(topic);
                if (currPartSize != partList.size()) {
                    sBuilder.append(group).append(":").append(topic).append("\n");
                }
            }
        }
    }
}
