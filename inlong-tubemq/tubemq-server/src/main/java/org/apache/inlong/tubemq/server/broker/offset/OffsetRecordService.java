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

package org.apache.inlong.tubemq.server.broker.offset;

import org.apache.inlong.tubemq.corebase.TokenConstants;
import org.apache.inlong.tubemq.corebase.daemon.AbstractDaemonService;
import org.apache.inlong.tubemq.corebase.utils.AddressUtils;
import org.apache.inlong.tubemq.corebase.utils.ServiceStatusHolder;
import org.apache.inlong.tubemq.server.broker.BrokerServiceServer;
import org.apache.inlong.tubemq.server.broker.TubeBroker;
import org.apache.inlong.tubemq.server.broker.metadata.TopicMetadata;
import org.apache.inlong.tubemq.server.broker.msgstore.MessageStoreManager;
import org.apache.inlong.tubemq.server.broker.nodeinfo.ConsumerNodeInfo;
import org.apache.inlong.tubemq.server.broker.offset.topicpub.OffsetPubItem;
import org.apache.inlong.tubemq.server.broker.offset.topicpub.TopicPubInfo;
import org.apache.inlong.tubemq.server.common.TServerConstants;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * History offset service
 *
 */
public class OffsetRecordService extends AbstractDaemonService {

    private static final Logger logger =
            LoggerFactory.getLogger(OffsetRecordService.class);
    private final TubeBroker broker;
    // tube broker's store manager
    private final MessageStoreManager storeManager;
    // tube broker's offset manager
    private final OffsetService offsetManager;
    // tube broker addressId
    private final int brokerAddrId;

    public OffsetRecordService(TubeBroker broker, long scanIntervalMs) {
        super("History-Offset", scanIntervalMs);
        this.broker = broker;
        this.storeManager = this.broker.getStoreManager();
        this.offsetManager = this.broker.getOffsetManager();
        this.brokerAddrId = AddressUtils.ipToInt(broker.getTubeConfig().getHostName());
        super.start();
    }

    @Override
    protected void loopProcess(StringBuilder strBuff) {
        try {
            // get group offset information
            storeRecord2LocalTopic(strBuff);
        } catch (Throwable throwable) {
            logger.error("[Offset Record] Daemon commit thread throw error ", throwable);
        }
    }

    public void close() {
        if (super.stop()) {
            return;
        }
        StringBuilder strBuff = new StringBuilder(2048);
        storeRecord2LocalTopic(strBuff);
        logger.info("[Offset-Record Service] offset-record service stopped!");
    }

    private void storeRecord2LocalTopic(StringBuilder strBuff) {
        // check node writable status
        if (ServiceStatusHolder.isWriteServiceStop()) {
            return;
        }
        // check topic writable status
        TopicMetadata csmHistoryMeta = storeManager.getMetadataManager()
                .getTopicMetadata(TServerConstants.OFFSET_HISTORY_NAME);
        if (csmHistoryMeta == null || !csmHistoryMeta.isAcceptPublish()) {
            return;
        }
        // get broker service
        BrokerServiceServer brokerService = broker.getBrokerServiceServer();
        // get all topic's produce offset information
        Map<String, TopicPubInfo> topicPubInfoMap = storeManager.getTopicPublishInfos();
        // get group offset information
        Map<String, OffsetHistoryInfo> onlineGroupOffsetMap = offsetManager.getOnlineGroupOffsetInfo();
        // add consume history offset info
        if (!onlineGroupOffsetMap.isEmpty()) {
            // add group client info
            addGroupOnlineInfo(brokerService, onlineGroupOffsetMap);
        }
        // get group offset information
        Map<String, OffsetHistoryInfo> offlineGroupOffsetMap = offsetManager.getOfflineGroupOffsetInfo();
        // append registered group topic's publish information
        appendTopicProduceOffsets(topicPubInfoMap, onlineGroupOffsetMap);
        // append unregistered group topic's publish information
        appendTopicProduceOffsets(topicPubInfoMap, offlineGroupOffsetMap);
        long currTime = System.currentTimeMillis();
        // store online group offset records to offset storage topic
        brokerService.appendGroupOffsetInfo(onlineGroupOffsetMap,
                brokerAddrId, currTime, 20, 5, strBuff);
        // store unregistered group offset records to offset storage topic
        brokerService.appendGroupOffsetInfo(offlineGroupOffsetMap,
                brokerAddrId, currTime, 20, 5, strBuff);
    }

    private void appendTopicProduceOffsets(Map<String, TopicPubInfo> topicPubInfoMap,
            Map<String, OffsetHistoryInfo> groupOffsetMap) {
        OffsetPubItem offsetPub;
        TopicPubInfo topicPubInfo;
        Map<String, Map<Integer, OffsetCsmRecord>> topicOffsetMap;
        for (Map.Entry<String, OffsetHistoryInfo> entry : groupOffsetMap.entrySet()) {
            if (entry == null || entry.getKey() == null || entry.getValue() == null) {
                continue;
            }
            topicOffsetMap = entry.getValue().getOffsetMap();
            // Get offset records by topic
            for (Map.Entry<String, Map<Integer, OffsetCsmRecord>> entryTopic : topicOffsetMap.entrySet()) {
                if (entryTopic == null
                        || entryTopic.getKey() == null
                        || entryTopic.getValue() == null) {
                    continue;
                }
                // Get message store instance
                topicPubInfo = topicPubInfoMap.get(entryTopic.getKey());
                if (topicPubInfo == null) {
                    continue;
                }
                for (Map.Entry<Integer, OffsetCsmRecord> entryRcd : entryTopic.getValue().entrySet()) {
                    offsetPub = topicPubInfo.getTopicStorePubInfo(entryRcd.getValue().getStoreId());
                    if (offsetPub == null) {
                        continue;
                    }
                    // Append current max, min offset
                    entryRcd.getValue().addStoreInfo(offsetPub.getIndexMin(),
                            offsetPub.getIndexMax(), offsetPub.getDataMin(), offsetPub.getDataMax());
                }
            }
        }
    }

    private void addGroupOnlineInfo(BrokerServiceServer brokerService,
            Map<String, OffsetHistoryInfo> groupOffsetMap) {
        String groupName;
        String[] partitionIdArr;
        OffsetHistoryInfo historyInfo;
        Map<String, ConsumerNodeInfo> regMap = brokerService.getConsumerRegisterMap();
        for (Map.Entry<String, ConsumerNodeInfo> entry : regMap.entrySet()) {
            if (entry == null || entry.getKey() == null || entry.getValue() == null) {
                continue;
            }
            partitionIdArr = entry.getKey().split(TokenConstants.ATTR_SEP);
            groupName = partitionIdArr[0];
            historyInfo = groupOffsetMap.get(groupName);
            if (historyInfo == null) {
                continue;
            }
            historyInfo.addGroupOnlineInfo(true, entry.getValue().isFilterConsume(),
                    entry.getValue().getConsumerId(), brokerService.getConsumerRegisterTime(
                            entry.getValue().getConsumerId(), entry.getKey()),
                    partitionIdArr[1], Integer.parseInt(partitionIdArr[2]));
        }
    }
}
