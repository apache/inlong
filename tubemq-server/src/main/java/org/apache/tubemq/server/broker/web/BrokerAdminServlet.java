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

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import javax.servlet.http.HttpServletRequest;
import org.apache.tubemq.corebase.TokenConstants;
import org.apache.tubemq.corebase.utils.TStringUtils;
import org.apache.tubemq.server.broker.TubeBroker;
import org.apache.tubemq.server.broker.msgstore.MessageStore;
import org.apache.tubemq.server.broker.msgstore.MessageStoreManager;
import org.apache.tubemq.server.broker.nodeinfo.ConsumerNodeInfo;
import org.apache.tubemq.server.broker.offset.OffsetService;
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
    }

    public StringBuilder adminQueryAllMethods(HttpServletRequest req) throws Exception {
        int index = 0;
        List<String> methods = getSupportedMethod();
        StringBuilder sBuilder = new StringBuilder(1024);
        sBuilder.append("{\"result\":true,\"errCode\":0,\"errMsg\":\"Success!\",\"dataSet\":[");
        for (index = 0; index < methods.size(); index++) {
            if (index > 0) {
                sBuilder.append(",");
            }
            sBuilder.append("{\"id\":").append(index + 1)
                    .append(",\"method\":\"").append(methods.get(index)).append("\"}");
        }
        sBuilder.append("],\"totalCnt\":").append(index + 1).append("}");
        return sBuilder;
    }

    /***
     * Query broker's all consumer info.
     *
     * @param req
     * @return
     * @throws Exception
     */
    public StringBuilder adminQueryBrokerAllConsumerInfo(HttpServletRequest req) throws Exception {
        int index = 0;
        StringBuilder sBuilder = new StringBuilder(1024);
        ProcessResult result = WebParameterUtils.getStringParamValue(req,
                WebFieldDef.COMPSGROUPNAME, false, null);
        if (!result.success) {
            return WebParameterUtils.buildFailResult(sBuilder, result.errInfo);
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
        return sBuilder;
    }

    /***
     * Query broker's all message store info.
     *
     * @param req
     * @return
     * @throws Exception
     */
    public StringBuilder adminQueryBrokerAllMessageStoreInfo(HttpServletRequest req)
            throws Exception {
        StringBuilder sBuilder = new StringBuilder(1024);
        ProcessResult result = WebParameterUtils.getStringParamValue(req,
                WebFieldDef.COMPSTOPICNAME, false, null);
        if (!result.success) {
            return WebParameterUtils.buildFailResult(sBuilder, result.errInfo);
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
        return sBuilder;
    }

    /***
     * Get memory store status info.
     *
     * @param req
     * @return
     * @throws Exception
     */
    public StringBuilder adminGetMemStoreStatisInfo(HttpServletRequest req) throws Exception {
        StringBuilder sBuilder = new StringBuilder(1024);
        ProcessResult result = WebParameterUtils.getStringParamValue(req,
                WebFieldDef.COMPSTOPICNAME, false, null);
        if (!result.success) {
            return WebParameterUtils.buildFailResult(sBuilder, result.errInfo);
        }
        Set<String> topicNameSet = (Set<String>) result.retData1;
        result = WebParameterUtils.getBooleanParamValue(req,
                WebFieldDef.NEEDREFRESH, false, false);
        if (!result.success) {
            return WebParameterUtils.buildFailResult(sBuilder, result.errInfo);
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
        return sBuilder;
    }

    /***
     * Manual set offset.
     *
     * @param req
     * @return
     * @throws Exception
     */
    public StringBuilder adminManualSetCurrentOffSet(HttpServletRequest req) throws Exception {
        StringBuilder sBuilder = new StringBuilder(512);
        ProcessResult result = WebParameterUtils.getStringParamValue(req,
                WebFieldDef.TOPICNAME, true, null);
        if (!result.success) {
            return WebParameterUtils.buildFailResult(sBuilder, result.errInfo);
        }
        final String topicName = (String) result.retData1;
        result = WebParameterUtils.getStringParamValue(req,
                WebFieldDef.GROUPNAME, true, null);
        if (!result.success) {
            return WebParameterUtils.buildFailResult(sBuilder, result.errInfo);
        }
        final String groupName = (String) result.retData1;
        result = WebParameterUtils.getStringParamValue(req,
                WebFieldDef.MODIFYUSER, true, null);
        if (!result.success) {
            return WebParameterUtils.buildFailResult(sBuilder, result.errInfo);
        }
        final String modifyUser = (String) result.retData1;
        result = WebParameterUtils.getIntParamValue(req,
                WebFieldDef.PARTITIONID, true, -1, 0);
        if (!result.success) {
            return WebParameterUtils.buildFailResult(sBuilder, result.errInfo);
        }
        int partitionId = (Integer) result.retData1;
        result = WebParameterUtils.getLongParamValue(req,
                WebFieldDef.MANUALOFFSET, true, -1);
        if (!result.success) {
            return WebParameterUtils.buildFailResult(sBuilder, result.errInfo);
        }
        long manualOffset = (Long) result.retData1;
        List<String> topicList = broker.getMetadataManager().getTopics();
        if (!topicList.contains(topicName)) {
            sBuilder.append("{\"result\":false,\"errCode\":400,\"errMsg\":\"")
                    .append("Invalid parameter: not found the topicName configure!")
                    .append("\"}");
            return sBuilder;
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
            return sBuilder;
        }
        if (manualOffset < store.getIndexMinOffset()) {
            sBuilder.append("{\"result\":false,\"errCode\":400,\"errMsg\":\"")
                    .append("Invalid parameter: manualOffset lower than Current MinOffset:(")
                    .append(manualOffset).append("<").append(store.getIndexMinOffset())
                    .append(")\"}");
            return sBuilder;
        }
        if (manualOffset > store.getIndexMaxOffset()) {
            sBuilder.append("{\"result\":false,\"errCode\":400,\"errMsg\":\"")
                    .append("Invalid parameter: manualOffset bigger than Current MaxOffset:(")
                    .append(manualOffset).append(">").append(store.getIndexMaxOffset())
                    .append(")\"}");
            return sBuilder;
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
        return sBuilder;
    }

    /***
     * Query snapshot message set.
     *
     * @param req
     * @return
     * @throws Exception
     */
    public StringBuilder adminQuerySnapshotMessageSet(HttpServletRequest req) throws Exception {
        StringBuilder sBuilder = new StringBuilder(1024);
        ProcessResult result = WebParameterUtils.getStringParamValue(req,
                WebFieldDef.TOPICNAME, true, null);
        if (!result.success) {
            return WebParameterUtils.buildFailResult(sBuilder, result.errInfo);
        }
        final String topicName = (String) result.retData1;
        result = WebParameterUtils.getIntParamValue(req,
                WebFieldDef.PARTITIONID, true, -1, 0);
        if (!result.success) {
            return WebParameterUtils.buildFailResult(sBuilder, result.errInfo);
        }
        int partitionId = (Integer) result.retData1;
        result = WebParameterUtils.getIntParamValue(req,
                WebFieldDef.MSGCOUNT, false, 3, 3);
        if (!result.success) {
            return WebParameterUtils.buildFailResult(sBuilder, result.errInfo);
        }
        int msgCount = (Integer) result.retData1;
        msgCount = Math.max(msgCount, 1);
        if (msgCount > 50) {
            sBuilder.append("{\"result\":false,\"errCode\":400,\"errMsg\":\"")
                    .append("Over max allowed msgCount value, allowed count is 50!")
                    .append("\"}");
            return sBuilder;
        }
        result = WebParameterUtils.getStringParamValue(req,
                WebFieldDef.FILTERCONDS, false, null);
        if (!result.success) {
            return WebParameterUtils.buildFailResult(sBuilder, result.errInfo);
        }
        Set<String> filterCondStrSet = (Set<String>) result.retData1;
        sBuilder = broker.getBrokerServiceServer()
                .getMessageSnapshot(topicName, partitionId, msgCount, filterCondStrSet, sBuilder);
        return sBuilder;
    }

    /***
     * Query consumer group offset.
     *
     * @param req
     * @return
     * @throws Exception
     */
    public StringBuilder adminQueryCurrentGroupOffSet(HttpServletRequest req)
            throws Exception {
        StringBuilder sBuilder = new StringBuilder(1024);
        ProcessResult result = WebParameterUtils.getStringParamValue(req,
                WebFieldDef.TOPICNAME, true, null);
        if (!result.success) {
            return WebParameterUtils.buildFailResult(sBuilder, result.errInfo);
        }
        final String topicName = (String) result.retData1;
        result = WebParameterUtils.getStringParamValue(req,
                WebFieldDef.GROUPNAME, true, null);
        if (!result.success) {
            return WebParameterUtils.buildFailResult(sBuilder, result.errInfo);
        }
        final String groupName = (String) result.retData1;
        result = WebParameterUtils.getIntParamValue(req,
                WebFieldDef.PARTITIONID, true, -1, 0);
        if (!result.success) {
            return WebParameterUtils.buildFailResult(sBuilder, result.errInfo);
        }
        int partitionId = (Integer) result.retData1;

        result = WebParameterUtils.getBooleanParamValue(req,
                WebFieldDef.REQUIREREALOFFSET, false, false);
        if (!result.success) {
            return WebParameterUtils.buildFailResult(sBuilder, result.errInfo);
        }
        boolean requireRealOffset = (Boolean) result.retData1;
        List<String> topicList = broker.getMetadataManager().getTopics();
        if (!topicList.contains(topicName)) {
            sBuilder.append("{\"result\":false,\"errCode\":400,\"errMsg\":\"")
                    .append("Invalid parameter: not found the topicName configure!")
                    .append("\"}");
            return sBuilder;
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
            return sBuilder;
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
        return sBuilder;
    }

    public StringBuilder adminQueryConsumerRegisterInfo(HttpServletRequest req) {
        StringBuilder sBuilder = new StringBuilder(1024);
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
        return sBuilder;
    }


}
