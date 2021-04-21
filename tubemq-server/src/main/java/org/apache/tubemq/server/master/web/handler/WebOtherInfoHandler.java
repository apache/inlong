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

package org.apache.tubemq.server.master.web.handler;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import javax.servlet.http.HttpServletRequest;
import org.apache.tubemq.corebase.cluster.ConsumerInfo;
import org.apache.tubemq.corebase.cluster.Partition;
import org.apache.tubemq.corebase.utils.Tuple2;
import org.apache.tubemq.server.common.fielddef.WebFieldDef;
import org.apache.tubemq.server.common.utils.ProcessResult;
import org.apache.tubemq.server.common.utils.WebParameterUtils;
import org.apache.tubemq.server.master.TMaster;
import org.apache.tubemq.server.master.nodemanage.nodebroker.TopicPSInfoManager;
import org.apache.tubemq.server.master.nodemanage.nodeconsumer.ConsumerBandInfo;
import org.apache.tubemq.server.master.nodemanage.nodeconsumer.ConsumerInfoHolder;
import org.apache.tubemq.server.master.nodemanage.nodeconsumer.NodeRebInfo;



public class WebOtherInfoHandler extends AbstractWebHandler {

    /**
     * Constructor
     *
     * @param master tube master
     */
    public WebOtherInfoHandler(TMaster master) {
        super(master);
    }

    @Override
    public void registerWebApiMethod() {
        // register query method
        registerQueryWebMethod("admin_query_sub_info",
                "getSubscribeInfo");
        registerQueryWebMethod("admin_query_consume_group_detail",
                "getConsumeGroupDetailInfo");
    }

    /**
     * Get subscription info
     *
     * @param req
     * @return
     */
    public StringBuilder getSubscribeInfo(HttpServletRequest req) {
        ProcessResult result = new ProcessResult();
        StringBuilder sBuffer = new StringBuilder(1024);
        // get group list
        if (!WebParameterUtils.getStringParamValue(req,
                WebFieldDef.COMPSGROUPNAME, false, null, sBuffer, result)) {
            WebParameterUtils.buildFailResult(sBuffer, result.errInfo);
            return sBuffer;
        }
        Set<String> inGroupNameSet = (Set<String>) result.retData1;
        if (inGroupNameSet.isEmpty()) {
            if (!WebParameterUtils.getStringParamValue(req,
                    WebFieldDef.COMPSCONSUMEGROUP, false, null, sBuffer, result)) {
                WebParameterUtils.buildFailResult(sBuffer, result.errInfo);
                return sBuffer;
            }
            inGroupNameSet = (Set<String>) result.retData1;
        }
        if (!WebParameterUtils.getStringParamValue(req,
                WebFieldDef.COMPSTOPICNAME, false, null, sBuffer, result)) {
            WebParameterUtils.buildFailResult(sBuffer, result.errInfo);
            return sBuffer;
        }
        Set<String> topicNameSet = (Set<String>) result.retData1;
        TopicPSInfoManager topicPSInfoManager = master.getTopicPSInfoManager();
        Set<String> queryGroupSet =
                topicPSInfoManager.getGroupSetWithSubTopic(inGroupNameSet, topicNameSet);
        int totalCnt = 0;
        int topicCnt = 0;
        Tuple2<Set<String>, Integer> queryInfo = new Tuple2<>();
        ConsumerInfoHolder consumerHolder = master.getConsumerHolder();
        WebParameterUtils.buildSuccessWithDataRetBegin(sBuffer);
        for (String group : queryGroupSet) {
            if (!consumerHolder.getGroupTopicSetAndClientCnt(group, queryInfo)) {
                continue;
            }
            if (totalCnt++ > 0) {
                sBuffer.append(",");
            }
            sBuffer.append("{\"consumeGroup\":\"").append(group).append("\",\"topicSet\":[");
            topicCnt = 0;
            for (String tmpTopic : queryInfo.getF0()) {
                if (topicCnt++ > 0) {
                    sBuffer.append(",");
                }
                sBuffer.append("\"").append(tmpTopic).append("\"");
            }
            sBuffer.append("],\"consumerNum\":").append(queryInfo.getF1()).append("}");
        }
        WebParameterUtils.buildSuccessWithDataRetEnd(sBuffer, totalCnt);
        return sBuffer;
    }

    /**
     * Get consume group detail info
     *
     * @param req
     * @return output as JSON
     */
    // #lizard forgives
    public StringBuilder getConsumeGroupDetailInfo(HttpServletRequest req) {
        ProcessResult result = new ProcessResult();
        StringBuilder sBuffer = new StringBuilder(1024);
        // get group name
        if (!WebParameterUtils.getStringParamValue(req,
                WebFieldDef.GROUPNAME, true, null, sBuffer, result)) {
            if (!WebParameterUtils.getStringParamValue(req,
                    WebFieldDef.CONSUMEGROUP, true, null, sBuffer, result)) {
                WebParameterUtils.buildFailResult(sBuffer, result.errInfo);
                return sBuffer;
            }
        }
        String strConsumeGroup = (String) result.retData1;
        try {
            boolean isBandConsume = false;
            boolean isNotAllocate = false;
            boolean isSelectBig = true;
            String sessionKey = "";
            int reqSourceCount = -1;
            int curSourceCount = -1;
            long rebalanceCheckTime = -1;
            int defBClientRate = -2;
            int confBClientRate = -2;
            int curBClientRate = -2;
            int minRequireClientCnt = -2;
            int rebalanceStatus = -2;
            Set<String> topicSet = new HashSet<>();
            List<ConsumerInfo> consumerList = new ArrayList<>();
            Map<String, NodeRebInfo> nodeRebInfoMap = new ConcurrentHashMap<>();
            Map<String, TreeSet<String>> existedTopicConditions = new HashMap<>();
            ConsumerInfoHolder consumerHolder = master.getConsumerHolder();
            ConsumerBandInfo consumerBandInfo = consumerHolder.getConsumerBandInfo(strConsumeGroup);
            if (consumerBandInfo != null) {
                if (consumerBandInfo.getTopicSet() != null) {
                    topicSet = consumerBandInfo.getTopicSet();
                }
                if (consumerBandInfo.getConsumerInfoList() != null) {
                    consumerList = consumerBandInfo.getConsumerInfoList();
                }
                if (consumerBandInfo.getTopicConditions() != null) {
                    existedTopicConditions = consumerBandInfo.getTopicConditions();
                }
                nodeRebInfoMap = consumerBandInfo.getRebalanceMap();
                isBandConsume = consumerBandInfo.isBandConsume();
                rebalanceStatus = consumerBandInfo.getRebalanceCheckStatus();
                defBClientRate = consumerBandInfo.getDefBClientRate();
                confBClientRate = consumerBandInfo.getConfBClientRate();
                curBClientRate = consumerBandInfo.getCurBClientRate();
                minRequireClientCnt = consumerBandInfo.getMinRequireClientCnt();
                if (isBandConsume) {
                    isNotAllocate = consumerBandInfo.isNotAllocate();
                    isSelectBig = consumerBandInfo.isSelectedBig();
                    sessionKey = consumerBandInfo.getSessionKey();
                    reqSourceCount = consumerBandInfo.getSourceCount();
                    curSourceCount = consumerBandInfo.getGroupCnt();
                    rebalanceCheckTime = consumerBandInfo.getCurCheckCycle();
                }
            }
            sBuffer.append("{\"result\":true,\"errCode\":0,\"errMsg\":\"OK\"")
                    .append(",\"count\":").append(consumerList.size()).append(",\"topicSet\":[");
            int itemCnt = 0;
            for (String topicItem : topicSet) {
                if (itemCnt++ > 0) {
                    sBuffer.append(",");
                }
                sBuffer.append("\"").append(topicItem).append("\"");
            }
            sBuffer.append("],\"consumeGroup\":\"").append(strConsumeGroup).append("\",\"re-rebalance\":{");
            itemCnt = 0;
            for (Map.Entry<String, NodeRebInfo> entry : nodeRebInfoMap.entrySet()) {
                if (itemCnt++ > 0) {
                    sBuffer.append(",");
                }
                sBuffer.append("\"").append(entry.getKey()).append("\":");
                sBuffer = entry.getValue().toJsonString(sBuffer);
            }
            sBuffer.append("},\"isBandConsume\":").append(isBandConsume);
            // Append band consume info
            if (isBandConsume) {
                sBuffer.append(",\"isNotAllocate\":").append(isNotAllocate)
                        .append(",\"sessionKey\":\"").append(sessionKey)
                        .append("\",\"isSelectBig\":").append(isSelectBig)
                        .append(",\"reqSourceCount\":").append(reqSourceCount)
                        .append(",\"curSourceCount\":").append(curSourceCount)
                        .append(",\"rebalanceCheckTime\":").append(rebalanceCheckTime);
            }
            sBuffer.append(",\"rebInfo\":{");
            if (rebalanceStatus == -2) {
                sBuffer.append("\"isRebalanced\":false");
            } else if (rebalanceStatus == 0) {
                sBuffer.append("\"isRebalanced\":true,\"checkPasted\":false")
                        .append(",\"defBClientRate\":").append(defBClientRate)
                        .append(",\"confBClientRate\":").append(confBClientRate)
                        .append(",\"curBClientRate\":").append(curBClientRate)
                        .append(",\"minRequireClientCnt\":").append(minRequireClientCnt);
            } else {
                sBuffer.append("\"isRebalanced\":true,\"checkPasted\":true")
                        .append(",\"defBClientRate\":").append(defBClientRate)
                        .append(",\"confBClientRate\":").append(confBClientRate)
                        .append(",\"curBClientRate\":").append(curBClientRate);
            }
            sBuffer.append("},\"filterConds\":{");
            if (existedTopicConditions != null) {
                int keyCount = 0;
                for (Map.Entry<String, TreeSet<String>> entry : existedTopicConditions.entrySet()) {
                    if (keyCount++ > 0) {
                        sBuffer.append(",");
                    }
                    sBuffer.append("\"").append(entry.getKey()).append("\":[");
                    if (entry.getValue() != null) {
                        int itemCount = 0;
                        for (String filterCond : entry.getValue()) {
                            if (itemCount++ > 0) {
                                sBuffer.append(",");
                            }
                            sBuffer.append("\"").append(filterCond).append("\"");
                        }
                    }
                    sBuffer.append("]");
                }
            }
            sBuffer.append("}");
            // Append consumer info of the group
            getConsumerInfoList(consumerList, isBandConsume, sBuffer);
            sBuffer.append("}");
        } catch (Exception e) {
            sBuffer.append("{\"result\":false,\"errCode\":400,\"errMsg\":\"")
                    .append(e.getMessage()).append("\",\"count\":0,\"data\":[]}");
        }
        return sBuffer;
    }

    /**
     * Private method to append consumer info of the give list to a string builder
     *
     * @param consumerList
     * @param isBandConsume
     * @param strBuffer
     */
    private void getConsumerInfoList(final List<ConsumerInfo> consumerList,
                                     boolean isBandConsume, final StringBuilder strBuffer) {
        strBuffer.append(",\"data\":[");
        if (!consumerList.isEmpty()) {
            Collections.sort(consumerList);
            Map<String, Map<String, Map<String, Partition>>> currentSubInfoMap =
                    master.getCurrentSubInfoMap();
            for (int i = 0; i < consumerList.size(); i++) {
                ConsumerInfo consumer = consumerList.get(i);
                if (consumer == null) {
                    continue;
                }
                if (i > 0) {
                    strBuffer.append(",");
                }
                strBuffer.append("{\"consumerId\":\"").append(consumer.getConsumerId())
                        .append("\"").append(",\"isOverTLS\":").append(consumer.isOverTLS());
                if (isBandConsume) {
                    Map<String, Long> requiredPartition = consumer.getRequiredPartition();
                    if (requiredPartition == null || requiredPartition.isEmpty()) {
                        strBuffer.append(",\"initReSetPartCount\":0,\"initReSetPartInfo\":[]");
                    } else {
                        strBuffer.append(",\"initReSetPartCount\":").append(requiredPartition.size())
                                .append(",\"initReSetPartInfo\":[");
                        int totalPart = 0;
                        for (Map.Entry<String, Long> entry : requiredPartition.entrySet()) {
                            if (totalPart++ > 0) {
                                strBuffer.append(",");
                            }
                            strBuffer.append("{\"partitionKey\":\"").append(entry.getKey())
                                    .append("\",\"Offset\":").append(entry.getValue()).append("}");
                        }
                        strBuffer.append("]");
                    }
                }
                Map<String, Map<String, Partition>> topicSubMap =
                        currentSubInfoMap.get(consumer.getConsumerId());
                if (topicSubMap == null || topicSubMap.isEmpty()) {
                    strBuffer.append(",\"parCount\":0,\"parInfo\":[]}");
                } else {
                    int totalSize = 0;
                    for (Map.Entry<String, Map<String, Partition>> entry : topicSubMap.entrySet()) {
                        totalSize += entry.getValue().size();
                    }
                    strBuffer.append(",\"parCount\":").append(totalSize).append(",\"parInfo\":[");
                    int totalPart = 0;
                    for (Map.Entry<String, Map<String, Partition>> entry : topicSubMap.entrySet()) {
                        Map<String, Partition> partMap = entry.getValue();
                        if (partMap != null) {
                            for (Partition part : partMap.values()) {
                                if (totalPart++ > 0) {
                                    strBuffer.append(",");
                                }
                                strBuffer.append("{\"partId\":").append(part.getPartitionId())
                                        .append(",\"brokerAddr\":\"").append(part.getBroker().toString())
                                        .append("\",\"topicName\":\"").append(part.getTopic()).append("\"}");
                            }
                        }
                    }
                    strBuffer.append("]}");
                }
            }
        }
        strBuffer.append("]");
    }

}
