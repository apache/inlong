/*
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

package org.apache.tubemq.server.broker.web;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import javax.servlet.http.HttpServletRequest;
import org.apache.tubemq.corebase.TokenConstants;
import org.apache.tubemq.corebase.utils.TStringUtils;
import org.apache.tubemq.corebase.utils.Tuple2;
import org.apache.tubemq.server.broker.TubeBroker;
import org.apache.tubemq.server.broker.metadata.TopicMetadata;
import org.apache.tubemq.server.broker.msgstore.MessageStore;
import org.apache.tubemq.server.broker.msgstore.MessageStoreManager;
import org.apache.tubemq.server.broker.nodeinfo.ConsumerNodeInfo;
import org.apache.tubemq.server.broker.offset.OffsetService;
import org.apache.tubemq.server.broker.utils.GroupOffsetInfo;
import org.apache.tubemq.server.broker.utils.TopicPubStoreInfo;
import org.apache.tubemq.server.common.fielddef.WebFieldDef;
import org.apache.tubemq.server.common.utils.ProcessResult;
import org.apache.tubemq.server.common.utils.WebParameterUtils;

/***
 * Broker's web servlet. Used for admin operation, like query consumer's status etc.
 */
public class BrokerAdminServlet extends AbstractWebHandler {


    public BrokerAdminServlet(TubeBroker broker) {
        super(broker);
        registerWebApiMethod();
    }

    @Override
    public void registerWebApiMethod() {
        // query consumer group's offset
        innRegisterWebMethod("admin_query_group_offset",
                "adminQueryCurrentGroupOffSet");
        // query snapshot message
        innRegisterWebMethod("admin_snapshot_message",
                "adminQuerySnapshotMessageSet");
        // query broker's all consumer info
        innRegisterWebMethod("admin_query_broker_all_consumer_info",
                "adminQueryBrokerAllConsumerInfo");
        // get memory store status info
        innRegisterWebMethod("admin_query_broker_memstore_info",
                "adminGetMemStoreStatisInfo");
        // query broker's all message store info
        innRegisterWebMethod("admin_query_broker_all_store_info",
                "adminQueryBrokerAllMessageStoreInfo");
        // query consumer register info
        innRegisterWebMethod("admin_query_consumer_regmap",
                "adminQueryConsumerRegisterInfo");
        // manual set offset
        innRegisterWebMethod("admin_manual_set_current_offset",
                "adminManualSetCurrentOffSet");
        // get all registered methods
        innRegisterWebMethod("admin_get_methods",
                "adminQueryAllMethods");
        // query topic's publish info
        innRegisterWebMethod("admin_query_pubinfo",
                "adminQueryPubInfo");
        // Query all consumer groups booked on the Broker.
        innRegisterWebMethod("admin_query_group",
                "adminQueryBookedGroup");
        // query consumer group's offset
        innRegisterWebMethod("admin_query_offset",
                "adminQueryGroupOffSet");
        // clone consumer group's offset from source to target
        innRegisterWebMethod("admin_clone_offset",
                "adminCloneGroupOffSet");
    }

    public void adminQueryAllMethods(HttpServletRequest req,
                                     StringBuilder sBuilder) throws Exception {
        int index = 0;
        List<String> methods = getSupportedMethod();
        sBuilder.append("{\"result\":true,\"errCode\":0,\"errMsg\":\"Success!\",\"dataSet\":[");
        for (index = 0; index < methods.size(); index++) {
            if (index > 0) {
                sBuilder.append(",");
            }
            sBuilder.append("{\"id\":").append(index + 1)
                    .append(",\"method\":\"").append(methods.get(index)).append("\"}");
        }
        sBuilder.append("],\"totalCnt\":").append(index + 1).append("}");
    }

    /***
     * Query broker's all consumer info.
     *
     * @param req
     * @param sBuilder process result
     * @throws Exception
     */
    public void adminQueryBrokerAllConsumerInfo(HttpServletRequest req,
                                                StringBuilder sBuilder) throws Exception {
        int index = 0;
        ProcessResult result = WebParameterUtils.getStringParamValue(req,
                WebFieldDef.COMPSGROUPNAME, false, null);
        if (!result.success) {
            WebParameterUtils.buildFailResult(sBuilder, result.errInfo);
            return;
        }
        Set<String> groupNameSet = (Set<String>) result.retData1;
        sBuilder.append("{\"result\":true,\"errCode\":0,\"errMsg\":\"Success!\",\"dataSet\":[");
        Map<String, ConsumerNodeInfo> map =
                broker.getBrokerServiceServer().getConsumerRegisterMap();
        for (Entry<String, ConsumerNodeInfo> entry : map.entrySet()) {
            if (TStringUtils.isBlank(entry.getKey()) || entry.getValue() == null) {
                continue;
            }
            String[] partitionIdArr =
                    entry.getKey().split(TokenConstants.ATTR_SEP);
            String groupName = partitionIdArr[0];
            if (!groupNameSet.isEmpty() && !groupNameSet.contains(groupName)) {
                continue;
            }
            String topicName = partitionIdArr[1];
            int partitionId = Integer.parseInt(partitionIdArr[2]);
            String consumerId = entry.getValue().getConsumerId();
            boolean ifFilterConsume = entry.getValue().isFilterConsume();
            if (index > 0) {
                sBuilder.append(",");
            }
            sBuilder.append("{\"index\":").append(++index).append(",\"groupName\":\"")
                    .append(groupName).append("\",\"topicName\":\"").append(topicName)
                    .append("\",\"partitionId\":").append(partitionId);
            Long regTime =
                    broker.getBrokerServiceServer().getConsumerRegisterTime(consumerId, entry.getKey());
            if (regTime == null || regTime <= 0) {
                sBuilder.append(",\"consumerId\":\"").append(consumerId)
                        .append("\",\"isRegOk\":false")
                        .append(",\"isFilterConsume\":")
                        .append(ifFilterConsume);
            } else {
                sBuilder.append(",\"consumerId\":\"").append(consumerId)
                        .append("\",\"isRegOk\":true,\"lastRegTime\":")
                        .append(regTime).append(",\"isFilterConsume\":")
                        .append(ifFilterConsume);
            }
            sBuilder.append(",\"qryPriorityId\":").append(entry.getValue().getQryPriorityId())
                    .append(",\"curDataLimitInM\":").append(entry.getValue().getCurFlowCtrlLimitSize())
                    .append(",\"curFreqLimit\":").append(entry.getValue().getCurFlowCtrlFreqLimit())
                    .append(",\"totalSentSec\":").append(entry.getValue().getSentMsgSize())
                    .append(",\"isSupportLimit\":").append(entry.getValue().isSupportLimit())
                    .append(",\"sentUnitSec\":").append(entry.getValue().getTotalUnitSec())
                    .append(",\"totalSentMin\":").append(entry.getValue().getTotalUnitMin())
                    .append(",\"sentUnit\":").append(entry.getValue().getSentUnit());
            MessageStoreManager storeManager = broker.getStoreManager();
            OffsetService offsetService = broker.getOffsetManager();
            MessageStore store = null;
            try {
                store = storeManager.getOrCreateMessageStore(topicName, partitionId);
            } catch (Throwable e) {
                //
            }
            if (store == null) {
                sBuilder.append(",\"isMessageStoreOk\":false}");
            } else {
                long tmpOffset = offsetService.getTmpOffset(groupName, topicName, partitionId);
                long minDataOffset = store.getDataMinOffset();
                long maxDataOffset = store.getDataMaxOffset();
                long minPartOffset = store.getIndexMinOffset();
                long maxPartOffset = store.getIndexMaxOffset();
                long zkOffset = offsetService.getOffset(groupName, topicName, partitionId);
                sBuilder.append(",\"isMessageStoreOk\":true,\"tmpOffset\":").append(tmpOffset)
                        .append(",\"minOffset\":").append(minPartOffset)
                        .append(",\"maxOffset\":").append(maxPartOffset)
                        .append(",\"zkOffset\":").append(zkOffset)
                        .append(",\"minDataOffset\":").append(minDataOffset)
                        .append(",\"maxDataOffset\":").append(maxDataOffset).append("}");
            }
        }
        sBuilder.append("],\"totalCnt\":").append(index).append("}");
    }

    /***
     * Query broker's all message store info.
     *
     * @param req
     * @param sBuilder process result
     * @throws Exception
     */
    public void adminQueryBrokerAllMessageStoreInfo(HttpServletRequest req,
                                                    StringBuilder sBuilder) throws Exception {
        ProcessResult result = WebParameterUtils.getStringParamValue(req,
                WebFieldDef.COMPSTOPICNAME, false, null);
        if (!result.success) {
            WebParameterUtils.buildFailResult(sBuilder, result.errInfo);
            return;
        }
        Set<String> topicNameSet = (Set<String>) result.retData1;
        sBuilder.append("{\"result\":true,\"errCode\":0,\"errMsg\":\"Success!\",\"dataSet\":[");
        Map<String, ConcurrentHashMap<Integer, MessageStore>> messageTopicStores =
                broker.getStoreManager().getMessageStores();
        int index = 0;
        int recordId = 0;
        for (Map.Entry<String, ConcurrentHashMap<Integer, MessageStore>> entry : messageTopicStores.entrySet()) {
            if (TStringUtils.isBlank(entry.getKey())
                    || (!topicNameSet.isEmpty() && !topicNameSet.contains(entry.getKey()))) {
                continue;
            }
            if (recordId > 0) {
                sBuilder.append(",");
            }
            index = 0;
            sBuilder.append("{\"index\":").append(++recordId).append(",\"topicName\":\"")
                    .append(entry.getKey()).append("\",\"storeInfo\":[");
            ConcurrentHashMap<Integer, MessageStore> partStoreMap = entry.getValue();
            if (partStoreMap != null) {
                for (Entry<Integer, MessageStore> subEntry : partStoreMap.entrySet()) {
                    MessageStore msgStore = subEntry.getValue();
                    if (msgStore == null) {
                        continue;
                    }
                    if (index++ > 0) {
                        sBuilder.append(",");
                    }
                    int numPartId = msgStore.getPartitionNum();
                    sBuilder.append("{\"storeId\":").append(subEntry.getKey())
                            .append(",\"numPartition\":").append(numPartId)
                            .append(",\"minDataOffset\":").append(msgStore.getDataMinOffset())
                            .append(",\"maxDataOffset\":").append(msgStore.getDataMaxOffset())
                            .append(",\"sizeInBytes\":").append(msgStore.getDataStoreSize())
                            .append(",\"partitionInfo\":[");
                    for (int partitionId = 0; partitionId < numPartId; partitionId++) {
                        if (partitionId > 0) {
                            sBuilder.append(",");
                        }
                        sBuilder.append("{\"partitionId\":").append(partitionId)
                                .append(",\"minOffset\":").append(msgStore.getIndexMinOffset())
                                .append(",\"maxOffset\":").append(msgStore.getIndexMaxOffset())
                                .append(",\"sizeInBytes\":").append(msgStore.getIndexStoreSize())
                                .append("}");
                    }
                    sBuilder.append("]}");
                }
            }
            sBuilder.append("]}");
        }
        sBuilder.append("],\"totalCnt\":").append(recordId).append("}");
    }

    /***
     * Get memory store status info.
     *
     * @param req
     * @param sBuilder process result
     * @throws Exception
     */
    public void adminGetMemStoreStatisInfo(HttpServletRequest req,
                                           StringBuilder sBuilder) throws Exception {
        ProcessResult result = WebParameterUtils.getStringParamValue(req,
                WebFieldDef.COMPSTOPICNAME, false, null);
        if (!result.success) {
            WebParameterUtils.buildFailResult(sBuilder, result.errInfo);
            return;
        }
        Set<String> topicNameSet = (Set<String>) result.retData1;
        result = WebParameterUtils.getBooleanParamValue(req,
                WebFieldDef.NEEDREFRESH, false, false);
        if (!result.success) {
            WebParameterUtils.buildFailResult(sBuilder, result.errInfo);
            return;
        }
        boolean requireRefresh = (boolean) result.retData1;
        sBuilder.append("{\"result\":true,\"errCode\":0,\"errMsg\":\"Success!\",\"detail\":[");
        Map<String, ConcurrentHashMap<Integer, MessageStore>> messageTopicStores =
                broker.getStoreManager().getMessageStores();
        int recordId = 0, index = 0;
        for (Map.Entry<String, ConcurrentHashMap<Integer, MessageStore>> entry : messageTopicStores.entrySet()) {
            if (TStringUtils.isBlank(entry.getKey())
                    || (!topicNameSet.isEmpty() && !topicNameSet.contains(entry.getKey()))) {
                continue;
            }
            String topicName = entry.getKey();
            if (recordId++ > 0) {
                sBuilder.append(",");
            }
            index = 0;
            sBuilder.append("{\"topicName\":\"").append(topicName).append("\",\"storeStatisInfo\":[");
            ConcurrentHashMap<Integer, MessageStore> partStoreMap = entry.getValue();
            if (partStoreMap != null) {
                for (Entry<Integer, MessageStore> subEntry : partStoreMap.entrySet()) {
                    MessageStore msgStore = subEntry.getValue();
                    if (msgStore == null) {
                        continue;
                    }
                    if (index++ > 0) {
                        sBuilder.append(",");
                    }
                    sBuilder.append("{\"storeId\":").append(subEntry.getKey())
                            .append(",\"memStatis\":").append(msgStore.getCurMemMsgSizeStatisInfo(requireRefresh))
                            .append(",\"fileStatis\":")
                            .append(msgStore.getCurFileMsgSizeStatisInfo(requireRefresh)).append("}");
                }
            }
            sBuilder.append("]}");
        }
        sBuilder.append("],\"totalCount\":").append(recordId).append("}");
    }

    /***
     * Manual set offset.
     *
     * @param req
     * @param sBuilder process result
     * @throws Exception
     */
    public void adminManualSetCurrentOffSet(HttpServletRequest req,
                                            StringBuilder sBuilder) throws Exception {
        ProcessResult result = WebParameterUtils.getStringParamValue(req,
                WebFieldDef.TOPICNAME, true, null);
        if (!result.success) {
            WebParameterUtils.buildFailResult(sBuilder, result.errInfo);
            return;
        }
        final String topicName = (String) result.retData1;
        result = WebParameterUtils.getStringParamValue(req,
                WebFieldDef.GROUPNAME, true, null);
        if (!result.success) {
            WebParameterUtils.buildFailResult(sBuilder, result.errInfo);
            return;
        }
        final String groupName = (String) result.retData1;
        result = WebParameterUtils.getStringParamValue(req,
                WebFieldDef.MODIFYUSER, true, null);
        if (!result.success) {
            WebParameterUtils.buildFailResult(sBuilder, result.errInfo);
            return;
        }
        final String modifyUser = (String) result.retData1;
        result = WebParameterUtils.getIntParamValue(req,
                WebFieldDef.PARTITIONID, true, -1, 0);
        if (!result.success) {
            WebParameterUtils.buildFailResult(sBuilder, result.errInfo);
            return;
        }
        int partitionId = (Integer) result.retData1;
        result = WebParameterUtils.getLongParamValue(req,
                WebFieldDef.MANUALOFFSET, true, -1);
        if (!result.success) {
            WebParameterUtils.buildFailResult(sBuilder, result.errInfo);
            return;
        }
        long manualOffset = (Long) result.retData1;
        List<String> topicList = broker.getMetadataManager().getTopics();
        if (!topicList.contains(topicName)) {
            sBuilder.append("{\"result\":false,\"errCode\":400,\"errMsg\":\"")
                    .append("Invalid parameter: not found the topicName configure!")
                    .append("\"}");
            return;
        }
        MessageStoreManager storeManager = broker.getStoreManager();
        MessageStore store = null;
        try {
            store = storeManager.getOrCreateMessageStore(topicName, partitionId);
        } catch (Throwable e) {
            //
        }
        if (store == null) {
            sBuilder.append("{\"result\":false,\"errCode\":400,\"errMsg\":\"")
                    .append("Invalid parameter: not found the store by topicName!")
                    .append("\"}");
            return;
        }
        if (manualOffset < store.getIndexMinOffset()) {
            sBuilder.append("{\"result\":false,\"errCode\":400,\"errMsg\":\"")
                    .append("Invalid parameter: manualOffset lower than Current MinOffset:(")
                    .append(manualOffset).append("<").append(store.getIndexMinOffset())
                    .append(")\"}");
            return;
        }
        if (manualOffset > store.getIndexMaxOffset()) {
            sBuilder.append("{\"result\":false,\"errCode\":400,\"errMsg\":\"")
                    .append("Invalid parameter: manualOffset bigger than Current MaxOffset:(")
                    .append(manualOffset).append(">").append(store.getIndexMaxOffset())
                    .append(")\"}");
            return;
        }
        OffsetService offsetService = broker.getOffsetManager();
        long oldOffset =
                offsetService.resetOffset(store, groupName,
                        topicName, partitionId, manualOffset, modifyUser);
        if (oldOffset < 0) {
            sBuilder.append("{\"result\":false,\"errCode\":401,\"errMsg\":\"")
                    .append("Manual update current Offset failure!")
                    .append("\"}");
        } else {
            sBuilder.append("{\"result\":true,\"errCode\":0,\"errMsg\":\"")
                    .append("Manual update current Offset success!")
                    .append("\",\"oldOffset\":").append(oldOffset).append("}");
        }
    }

    /***
     * Query snapshot message set.
     *
     * @param req
     * @param sBuilder process result
     * @throws Exception
     */
    public void adminQuerySnapshotMessageSet(HttpServletRequest req,
                                             StringBuilder sBuilder) throws Exception {
        ProcessResult result = WebParameterUtils.getStringParamValue(req,
                WebFieldDef.TOPICNAME, true, null);
        if (!result.success) {
            WebParameterUtils.buildFailResult(sBuilder, result.errInfo);
            return;
        }
        final String topicName = (String) result.retData1;
        result = WebParameterUtils.getIntParamValue(req,
                WebFieldDef.PARTITIONID, true, -1, 0);
        if (!result.success) {
            WebParameterUtils.buildFailResult(sBuilder, result.errInfo);
            return;
        }
        int partitionId = (Integer) result.retData1;
        result = WebParameterUtils.getIntParamValue(req,
                WebFieldDef.MSGCOUNT, false, 3, 3);
        if (!result.success) {
            WebParameterUtils.buildFailResult(sBuilder, result.errInfo);
            return;
        }
        int msgCount = (Integer) result.retData1;
        msgCount = Math.max(msgCount, 1);
        if (msgCount > 50) {
            sBuilder.append("{\"result\":false,\"errCode\":400,\"errMsg\":\"")
                    .append("Over max allowed msgCount value, allowed count is 50!")
                    .append("\"}");
            return;
        }
        result = WebParameterUtils.getStringParamValue(req,
                WebFieldDef.FILTERCONDS, false, null);
        if (!result.success) {
            WebParameterUtils.buildFailResult(sBuilder, result.errInfo);
            return;
        }
        Set<String> filterCondStrSet = (Set<String>) result.retData1;
        sBuilder = broker.getBrokerServiceServer()
                .getMessageSnapshot(topicName, partitionId, msgCount, filterCondStrSet, sBuilder);
    }

    /***
     * Query consumer group offset.
     *
     * @param req
     * @param sBuilder process result
     * @throws Exception
     */
    public void adminQueryCurrentGroupOffSet(HttpServletRequest req,
                                             StringBuilder sBuilder) {
        ProcessResult result = WebParameterUtils.getStringParamValue(req,
                WebFieldDef.TOPICNAME, true, null);
        if (!result.success) {
            WebParameterUtils.buildFailResult(sBuilder, result.errInfo);
            return;
        }
        final String topicName = (String) result.retData1;
        result = WebParameterUtils.getStringParamValue(req,
                WebFieldDef.GROUPNAME, true, null);
        if (!result.success) {
            WebParameterUtils.buildFailResult(sBuilder, result.errInfo);
            return;
        }
        final String groupName = (String) result.retData1;
        result = WebParameterUtils.getIntParamValue(req,
                WebFieldDef.PARTITIONID, true, -1, 0);
        if (!result.success) {
            WebParameterUtils.buildFailResult(sBuilder, result.errInfo);
            return;
        }
        int partitionId = (Integer) result.retData1;

        result = WebParameterUtils.getBooleanParamValue(req,
                WebFieldDef.REQUIREREALOFFSET, false, false);
        if (!result.success) {
            WebParameterUtils.buildFailResult(sBuilder, result.errInfo);
            return;
        }
        boolean requireRealOffset = (Boolean) result.retData1;
        List<String> topicList = broker.getMetadataManager().getTopics();
        if (!topicList.contains(topicName)) {
            sBuilder.append("{\"result\":false,\"errCode\":400,\"errMsg\":\"")
                    .append("Invalid parameter: not found the topicName configure!")
                    .append("\"}");
            return;
        }
        MessageStoreManager storeManager = broker.getStoreManager();
        OffsetService offsetService = broker.getOffsetManager();
        MessageStore store = null;
        try {
            store = storeManager.getOrCreateMessageStore(topicName, partitionId);
        } catch (Throwable e) {
            //
        }
        if (store == null) {
            sBuilder.append("{\"result\":false,\"errCode\":400,\"errMsg\":\"")
                    .append("Invalid parameter: not found the store by topicName!")
                    .append("\"}");
            return;
        }
        long tmpOffset = offsetService.getTmpOffset(groupName, topicName, partitionId);
        long minDataOffset = store.getDataMinOffset();
        long maxDataOffset = store.getDataMaxOffset();
        long minPartOffset = store.getIndexMinOffset();
        long maxPartOffset = store.getIndexMaxOffset();
        sBuilder.append("{\"result\":true,\"errCode\":0,\"errMsg\":\"")
                .append("OK!")
                .append("\",\"tmpOffset\":").append(tmpOffset)
                .append(",\"minOffset\":").append(minPartOffset)
                .append(",\"maxOffset\":").append(maxPartOffset)
                .append(",\"minDataOffset\":").append(minDataOffset)
                .append(",\"maxDataOffset\":").append(maxDataOffset);
        if (requireRealOffset) {
            long curReadDataOffset = -2;
            long curRdDltDataOffset = -2;
            long zkOffset = offsetService.getOffset(groupName, topicName, partitionId);
            String queryKey =
                    groupName + TokenConstants.ATTR_SEP + topicName + TokenConstants.ATTR_SEP + partitionId;
            ConsumerNodeInfo consumerNodeInfo = broker.getConsumerNodeInfo(queryKey);
            if (consumerNodeInfo != null) {
                curReadDataOffset = consumerNodeInfo.getLastDataRdOffset();
                curRdDltDataOffset = curReadDataOffset < 0 ? -2 : maxDataOffset - curReadDataOffset;
            }
            if (curReadDataOffset < 0) {
                sBuilder.append(",\"zkOffset\":").append(zkOffset)
                        .append(",\"curReadDataOffset\":-1,\"curRdDltDataOffset\":-1");
            } else {
                sBuilder.append(",\"zkOffset\":").append(zkOffset)
                        .append(",\"curReadDataOffset\":").append(curReadDataOffset)
                        .append(",\"curRdDltDataOffset\":").append(curRdDltDataOffset);
            }
        }
        sBuilder.append("}");
    }

    public void adminQueryConsumerRegisterInfo(HttpServletRequest req,
                                               StringBuilder sBuilder) {
        Map<String, ConsumerNodeInfo> map =
                broker.getBrokerServiceServer().getConsumerRegisterMap();
        int totalCnt = 0;
        sBuilder.append("{\"result\":true,\"errCode\":0,\"errMsg\":\"Success!\",")
                .append(",\"dataSet\":[");
        for (Entry<String, ConsumerNodeInfo> entry : map.entrySet()) {
            if (entry.getKey() == null || entry.getValue() == null) {
                continue;
            }
            if (totalCnt > 0) {
                sBuilder.append(",");
            }
            sBuilder.append("{\"Partition\":\"").append(entry.getKey())
                    .append("\",\"Consumer\":\"")
                    .append(entry.getValue().getConsumerId())
                    .append("\",\"index\":").append(++totalCnt).append("}");
        }
        sBuilder.append("],\"totalCnt\":").append(totalCnt).append("}");
    }

    /***
     * Query topic's publish info on the Broker.
     *
     * @param req
     * @param sBuilder process result
     */
    public void adminQueryPubInfo(HttpServletRequest req,
                                  StringBuilder sBuilder) {
        // get the topic set to be queried
        ProcessResult result = WebParameterUtils.getStringParamValue(req,
                WebFieldDef.COMPSTOPICNAME, false, null);
        if (!result.success) {
            WebParameterUtils.buildFailResult(sBuilder, result.errInfo);
            return;
        }
        // get target consume group name
        Set<String> topicSet = (Set<String>) result.retData1;
        // get topic's publish info
        Map<String, Map<Integer, TopicPubStoreInfo>> topicStorePubInfoMap =
                broker.getStoreManager().getTopicPublishInfos(topicSet);
        // builder result
        int totalCnt = 0;
        sBuilder.append("{\"result\":true,\"errCode\":0,\"errMsg\":\"Success!\",\"dataSet\":[");
        for (Map.Entry<String, Map<Integer, TopicPubStoreInfo>> entry
                : topicStorePubInfoMap.entrySet()) {
            if (totalCnt++ > 0) {
                sBuilder.append(",");
            }
            sBuilder.append("{\"topicName\":\"").append(entry.getKey())
                    .append("\",\"offsetInfo\":[");
            Map<Integer, TopicPubStoreInfo> storeInfoMap = entry.getValue();
            int itemCnt = 0;
            for (Map.Entry<Integer, TopicPubStoreInfo> entry1 : storeInfoMap.entrySet()) {
                if (itemCnt++ > 0) {
                    sBuilder.append(",");
                }
                TopicPubStoreInfo pubStoreInfo = entry1.getValue();
                pubStoreInfo.buildPubStoreInfo(sBuilder);
            }
            sBuilder.append("],\"itemCount\":").append(itemCnt).append("}");
        }
        sBuilder.append("],\"dataCount\":").append(totalCnt).append("}");
    }

    /***
     * Query all consumer groups booked on the Broker.
     *
     * @param req
     * @param sBuilder process result
     */
    public void adminQueryBookedGroup(HttpServletRequest req,
                                      StringBuilder sBuilder) {
        // get divide info
        ProcessResult result = WebParameterUtils.getBooleanParamValue(req,
                WebFieldDef.WITHDIVIDE, false, false);
        if (!result.success) {
            WebParameterUtils.buildFailResult(sBuilder, result.errInfo);
            return;
        }
        boolean withDivide = (boolean) result.retData1;
        // get offset service
        int itemCnt = 0;
        int totalCnt = 0;
        OffsetService offsetService = broker.getOffsetManager();
        sBuilder.append("{\"result\":true,\"errCode\":0,\"errMsg\":\"Success!\",\"dataSet\":[");
        if (withDivide) {
            // query in-memory group name set
            Set<String> onlineGroups = offsetService.getInMemoryGroups();
            sBuilder.append("{\"type\":\"in-cache\",\"groupName\":[");
            for (String group : onlineGroups) {
                if (itemCnt++ > 0) {
                    sBuilder.append(",");
                }
                sBuilder.append("\"").append(group).append("\"");
            }
            sBuilder.append("],\"groupCount\":").append(itemCnt).append("}");
            totalCnt++;
            sBuilder.append(",");
            // query in-zk group name set
            itemCnt = 0;
            Set<String> onZKGroup = offsetService.getUnusedGroupInfo();
            sBuilder.append("{\"type\":\"in-zk\",\"groupName\":[");
            for (String group : onZKGroup) {
                if (itemCnt++ > 0) {
                    sBuilder.append(",");
                }
                sBuilder.append("\"").append(group).append("\"");
            }
            sBuilder.append("],\"groupCount\":").append(itemCnt).append("}");
            totalCnt++;
        } else {
            Set<String> allGroups = offsetService.getBookedGroups();
            sBuilder.append("{\"type\":\"all\",\"groupName\":[");
            for (String group : allGroups) {
                if (itemCnt++ > 0) {
                    sBuilder.append(",");
                }
                sBuilder.append("\"").append(group).append("\"");
            }
            sBuilder.append("],\"groupCount\":").append(itemCnt).append("}");
            totalCnt++;
        }
        sBuilder.append("],\"dataCount\":").append(totalCnt).append("}");
    }

    /***
     * Query consumer group offset.
     *
     * @param req
     * @param sBuilder process result
     */
    public void adminQueryGroupOffSet(HttpServletRequest req,
                                      StringBuilder sBuilder) {
        // get group list
        ProcessResult result = WebParameterUtils.getStringParamValue(req,
                WebFieldDef.COMPSGROUPNAME, false, null);
        if (!result.success) {
            WebParameterUtils.buildFailResult(sBuilder, result.errInfo);
            return;
        }
        // filter invalid groups
        Set<String> qryGroupNameSet = new HashSet<>();
        Set<String> inGroupNameSet = (Set<String>) result.retData1;
        Set<String> bookedGroupSet = broker.getOffsetManager().getBookedGroups();
        if (inGroupNameSet.isEmpty()) {
            qryGroupNameSet = bookedGroupSet;
        } else {
            for (String group : inGroupNameSet) {
                if (bookedGroupSet.contains(group)) {
                    qryGroupNameSet.add(group);
                }
            }
        }
        // get the topic set to be queried
        result = WebParameterUtils.getStringParamValue(req,
                WebFieldDef.COMPSTOPICNAME, false, null);
        if (!result.success) {
            WebParameterUtils.buildFailResult(sBuilder, result.errInfo);
            return;
        }
        // get target consume group name
        Set<String> topicSet = (Set<String>) result.retData1;
        // verify the acquired Topic set and
        //   query the corresponding offset information
        Map<String, Map<String, Map<Integer, GroupOffsetInfo>>> groupOffsetMaps =
                getGroupOffsetInfo(qryGroupNameSet, topicSet);
        // builder result
        int totalCnt = 0;
        sBuilder.append("{\"result\":true,\"errCode\":0,\"errMsg\":\"Success!\",\"dataSet\":[");
        for (Map.Entry<String, Map<String, Map<Integer, GroupOffsetInfo>>> entry
                : groupOffsetMaps.entrySet()) {
            if (totalCnt++ > 0) {
                sBuilder.append(",");
            }
            Map<String, Map<Integer, GroupOffsetInfo>> topicPartMap = entry.getValue();
            sBuilder.append("{\"groupName\":\"").append(entry.getKey())
                    .append("\",\"subInfo\":[");
            int topicCnt = 0;
            for (Map.Entry<String, Map<Integer, GroupOffsetInfo>> entry1 : topicPartMap.entrySet()) {
                if (topicCnt++ > 0) {
                    sBuilder.append(",");
                }
                Map<Integer, GroupOffsetInfo> partOffMap = entry1.getValue();
                sBuilder.append("{\"topicName\":\"").append(entry1.getKey())
                        .append("\",\"offsets\":[");
                int partCnt = 0;
                for (Map.Entry<Integer, GroupOffsetInfo> entry2 : partOffMap.entrySet()) {
                    if (partCnt++ > 0) {
                        sBuilder.append(",");
                    }
                    GroupOffsetInfo offsetInfo = entry2.getValue();
                    offsetInfo.buildOffsetInfo(sBuilder);
                }
                sBuilder.append("],\"partCount\":").append(partCnt).append("}");
            }
            sBuilder.append("],\"topicCount\":").append(topicCnt).append("}");
        }
        sBuilder.append("],\"totalCnt\":").append(totalCnt).append("}");
    }

    /***
     * Clone consume group offset, clone A group's offset to other group.
     *
     * @param req
     * @param sBuilder process result
     */
    public void adminCloneGroupOffSet(HttpServletRequest req,
                                      StringBuilder sBuilder) {
        // get source consume group name
        ProcessResult result = WebParameterUtils.getStringParamValue(req,
                WebFieldDef.SRCGROUPNAME, true, null);
        if (!result.success) {
            WebParameterUtils.buildFailResult(sBuilder, result.errInfo);
            return;
        }
        final String srcGroupName = (String) result.retData1;
        // get modify user
        result = WebParameterUtils.getStringParamValue(req,
                WebFieldDef.MODIFYUSER, true, null);
        if (!result.success) {
            WebParameterUtils.buildFailResult(sBuilder, result.errInfo);
            return;
        }
        final String modifier = (String) result.retData1;
        // get source consume group's topic set cloned to target group
        result = WebParameterUtils.getStringParamValue(req,
                WebFieldDef.COMPSTOPICNAME, false, null);
        if (!result.success) {
            WebParameterUtils.buildFailResult(sBuilder, result.errInfo);
            return;
        }
        // get target consume group name
        Set<String> srcTopicNameSet = (Set<String>) result.retData1;
        result = WebParameterUtils.getStringParamValue(req,
                WebFieldDef.TGTCOMPSGROUPNAME, true, null);
        if (!result.success) {
            WebParameterUtils.buildFailResult(sBuilder, result.errInfo);
            return;
        }
        Set<String> tgtGroupNameSet = (Set<String>) result.retData1;
        // check sourceGroup if existed
        Set<String> bookedGroups = broker.getOffsetManager().getBookedGroups();
        if (!bookedGroups.contains(srcGroupName)) {
            WebParameterUtils.buildFailResult(sBuilder,
                    new StringBuilder(512).append("Parameter ")
                            .append(WebFieldDef.SRCGROUPNAME.name).append(": ")
                            .append(srcGroupName)
                            .append(" has not been registered on this Broker!").toString());
            return;
        }
        // valid topic and get topic's partitionIds
        Map<String, Set<Integer>> topicPartMap =
                validAndGetPartitions(srcGroupName, srcTopicNameSet);
        if (topicPartMap.isEmpty()) {
            WebParameterUtils.buildFailResult(sBuilder,
                    new StringBuilder(512).append("Parameter ")
                            .append(WebFieldDef.SRCGROUPNAME.name).append(": not found ")
                            .append(srcGroupName).append(" subscribed topic set!").toString());
            return;
        }
        // query offset from source group
        Map<String, Map<Integer, Tuple2<Long, Long>>> srcGroupOffsets =
                broker.getOffsetManager().queryGroupOffset(srcGroupName, topicPartMap);
        boolean changed = broker.getOffsetManager().modifyGroupOffset(
                broker.getStoreManager(), tgtGroupNameSet, srcGroupOffsets, modifier);
        // builder return result
        sBuilder.append("{\"result\":true,\"errCode\":0,\"errMsg\":\"OK\"}");
    }

    // builder group's offset info
    private Map<String, Map<String, Map<Integer, GroupOffsetInfo>>> getGroupOffsetInfo(
            Set<String> groupSet, Set<String> topicSet) {
        long curReadDataOffset = -2;
        long curDataLag = -2;
        Map<String, Map<String, Map<Integer, GroupOffsetInfo>>> groupOffsetMaps = new HashMap<>();
        for (String group : groupSet) {
            Map<String, Map<Integer, GroupOffsetInfo>> topicOffsetRet = new HashMap<>();
            // valid and get topic's partitionIds
            Map<String, Set<Integer>> topicPartMap = validAndGetPartitions(group, topicSet);
            // get topic's publish info
            Map<String, Map<Integer, TopicPubStoreInfo>> topicStorePubInfoMap =
                    broker.getStoreManager().getTopicPublishInfos(topicPartMap.keySet());
            // get group's booked offset info
            Map<String, Map<Integer, Tuple2<Long, Long>>> groupOffsetMap =
                    broker.getOffsetManager().queryGroupOffset(group, topicPartMap);
            // get offset info array
            for (Map.Entry<String, Set<Integer>> entry : topicPartMap.entrySet()) {
                String topic = entry.getKey();
                Map<Integer, GroupOffsetInfo> partOffsetRet = new HashMap<>();
                Map<Integer, TopicPubStoreInfo> storeInfoMap = topicStorePubInfoMap.get(topic);
                Map<Integer, Tuple2<Long, Long>> partBookedMap = groupOffsetMap.get(topic);
                for (Integer partitionId : entry.getValue()) {
                    GroupOffsetInfo offsetInfo = new GroupOffsetInfo(partitionId);
                    offsetInfo.setPartPubStoreInfo(storeInfoMap.get(partitionId));
                    offsetInfo.setConsumeOffsetInfo(partBookedMap.get(partitionId));
                    String queryKey = buildQueryID(group, topic, partitionId);
                    ConsumerNodeInfo nodeInfo = broker.getConsumerNodeInfo(queryKey);
                    if (nodeInfo != null) {
                        offsetInfo.setConsumeDataOffsetInfo(nodeInfo.getLastDataRdOffset());
                    }
                    offsetInfo.calculateLag();
                    partOffsetRet.put(partitionId, offsetInfo);
                }
                topicOffsetRet.put(topic, partOffsetRet);
            }
            groupOffsetMaps.put(group, topicOffsetRet);
        }
        return groupOffsetMaps;
    }


    private Map<String, Set<Integer>> validAndGetPartitions(String group, Set<String> topicSet) {
        Map<String, Set<Integer>> topicPartMap = new HashMap<>();
        // query stored topic set stored in memory or zk
        if (topicSet.isEmpty()) {
            topicSet = broker.getOffsetManager().getGroupSubInfo(group);
        }
        // get topic's partitionIds
        if (topicSet != null) {
            Map<String, TopicMetadata> topicConfigMap =
                    broker.getMetadataManager().getTopicConfigMap();
            if (topicConfigMap != null) {
                for (String topic : topicSet) {
                    TopicMetadata topicMetadata = topicConfigMap.get(topic);
                    if (topicMetadata != null) {
                        topicPartMap.put(topic, topicMetadata.getAllPartitionIds());
                    }
                }
            }
        }
        return topicPartMap;
    }

    private String buildQueryID(String group, String topic, int partitionId) {
        return new StringBuilder(512).append(group)
                .append(TokenConstants.ATTR_SEP).append(topic)
                .append(TokenConstants.ATTR_SEP).append(partitionId).toString();
    }

}
