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


package org.apache.tubemq.server.broker.offset;

import static org.apache.tubemq.corebase.TBaseConstants.MAX_CHAR_NUM_PER_MESSAGE;
import static org.apache.tubemq.corebase.TBaseConstants.OFFSET_TIME_FORMAT;
import static org.apache.tubemq.corebase.TBaseConstants.OFFSET_TOPIC;
import static org.apache.tubemq.corebase.TBaseConstants.OFFSET_TOPIC_PARTITION;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.commons.codec.binary.StringUtils;
import org.apache.tubemq.corebase.TokenConstants;
import org.apache.tubemq.corebase.daemon.AbstractDaemonService;
import org.apache.tubemq.corebase.utils.CheckSum;
import org.apache.tubemq.corebase.utils.TStringUtils;
import org.apache.tubemq.server.broker.BrokerConfig;
import org.apache.tubemq.server.broker.TubeBroker;
import org.apache.tubemq.server.broker.metadata.MetadataManager;
import org.apache.tubemq.server.broker.metadata.TopicMetadata;
import org.apache.tubemq.server.broker.msgstore.MessageStore;
import org.apache.tubemq.server.broker.msgstore.MessageStoreManager;
import org.apache.tubemq.server.broker.utils.GroupOffsetInfo;
import org.apache.tubemq.server.common.fielddef.WebFieldDef;
import org.apache.tubemq.server.common.utils.AppendResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OffsetTimeManager extends AbstractDaemonService{

    private static final Logger logger = LoggerFactory.getLogger(DefaultOffsetManager.class);
    private static final Long offsetInterval = 60000L;
    private final BrokerConfig brokerConfig;
    private final TubeBroker broker;
    private final String offsetTimeTopicName;


    public OffsetTimeManager(final BrokerConfig brokerConfig, final TubeBroker broker) {
        super("[Offset Time Manager]", offsetInterval);
        this.brokerConfig = brokerConfig;
        this.broker = broker;
        this.offsetTimeTopicName = new StringBuilder(OFFSET_TOPIC)
            .append(broker.getTubeConfig().getBrokerId()).toString();
        super.start();
    }

    @Override
    protected void loopProcess(long intervalMs) {
        while (!super.isStopped()) {
            // for every one minute, save produce offsets and consume offset
            try {
                Thread.sleep(intervalMs);
            } catch (Exception e) {
                logger.error("sleep fail");
                return;
            }
            // get all groups booked in this broker
            Set<String> bookedGroupSet = broker.getOffsetManager().getBookedGroups();
            Set<String> topicSet = new HashSet<>();

            // get group offset information
            Map<String, Map<String, Map<Integer, GroupOffsetInfo>>> groupOffsetMaps =
                broker.getGroupOffsetInfo(WebFieldDef.GROUPNAME.name, bookedGroupSet, topicSet);

            logger.info("start to process offset time store");
            getAndSendMessages(groupOffsetMaps);
        }
    }

    private void getAndSendMessages(Map<String, Map<String, Map<Integer, GroupOffsetInfo>>> groupOffsetMaps) {
        StringBuilder offsetMsgBuilder = new StringBuilder(MAX_CHAR_NUM_PER_MESSAGE);

        for (Map.Entry<String, Map<String, Map<Integer, GroupOffsetInfo>>> groupOffsetEntry
            : groupOffsetMaps.entrySet()) {
            // for every group send one set of message
            Map<String, Map<Integer, GroupOffsetInfo>> topicPartMap = groupOffsetEntry.getValue();
            if (topicPartMap.isEmpty()) {
                // no topic consumed by this group
                continue;
            }
            List<String> pendingMessages = formGroupMessages(offsetMsgBuilder, groupOffsetEntry,
                topicPartMap);
            pendingMessages.add(offsetMsgBuilder.toString());
            offsetMsgBuilder.delete(0, offsetMsgBuilder.length());
            sendMessageForOneGroup(pendingMessages);
        }
        logger.info("send offset to offset topic finish, group num: {}", groupOffsetMaps.size());
    }

    private List<String> formGroupMessages(StringBuilder offsetMsgBuilder,
        Map.Entry<String, Map<String, Map<Integer, GroupOffsetInfo>>> groupOffsetEntry,
        Map<String, Map<Integer, GroupOffsetInfo>> topicPartMap) {
        // first put group name
        offsetMsgBuilder.append(groupOffsetEntry.getKey());
        List<String> pendingMessages = new ArrayList<>();
        for (Map.Entry<String, Map<Integer, GroupOffsetInfo>> topicPartEntry : topicPartMap.entrySet()) {
            Map<Integer, GroupOffsetInfo> partOffMap = topicPartEntry.getValue();
            for (GroupOffsetInfo groupOffsetInfo : partOffMap.values()) {
                if (offsetMsgBuilder.length() > MAX_CHAR_NUM_PER_MESSAGE) {
                    // create new message if one message is too large
                    pendingMessages.add(offsetMsgBuilder.toString());
                    offsetMsgBuilder.delete(0, offsetMsgBuilder.length());
                    // first put group name
                    offsetMsgBuilder.append(groupOffsetEntry.getKey());
                }
                // put topic name and brokerId
                offsetMsgBuilder.append(" ").append(topicPartEntry.getKey()).
                    append(" ").append(brokerConfig.getBrokerId());
                // put partitionId, produce offset and consume offset
                groupOffsetInfo.buildOffsetInfoMsg(offsetMsgBuilder);
            }
        }
        return pendingMessages;
    }


    private void sendMessageForOneGroup(List<String> pendingMessages) {
        MetadataManager metadataManager = broker.getMetadataManager();
        MessageStoreManager storeManager = broker.getStoreManager();
        TopicMetadata topicMetadata = metadataManager.getTopicMetadata(offsetTimeTopicName);

        Date messageTime = new Date();
        SimpleDateFormat f = new SimpleDateFormat(OFFSET_TIME_FORMAT);
        String sendTime = f.format(messageTime);

        for (String message : pendingMessages) {
            byte[] body = message.getBytes(StandardCharsets.UTF_8);
            if (topicMetadata == null) {
                logger.warn("no offsetTime Topic is present, please check");
                return;
            }
            if (metadataManager.isClosedTopic(offsetTimeTopicName)) {
                logger.warn("offsetTime Topic is closed, please check");
                return;
            }
            doSendMessage(sendTime, storeManager, body);
        }
    }

    private void doSendMessage(String sendTime, MessageStoreManager storeManager,
        byte[] body) {
        String attributes = TokenConstants.TOKEN_MSG_TYPE + TokenConstants.EQ + sendTime;
        byte[] bytes = encodePayload(body, attributes);
        // use sendTime as filter item
        int msgTypeCode = sendTime.hashCode();
        final int dataLength = bytes.length;
        final int msgFlag = 1;
        // no use of sendAddr in this situation
        final int sendAddr = -1;
        int checkSum = CheckSum.crc32(bytes);

        try {
            final MessageStore store =
                storeManager.getOrCreateMessageStore(offsetTimeTopicName, OFFSET_TOPIC_PARTITION);
            final AppendResult appendResult = new AppendResult();
            boolean sendRes = store.appendMsg(appendResult, dataLength, checkSum, bytes,
                msgTypeCode, msgFlag, OFFSET_TOPIC_PARTITION, sendAddr);
            if (!sendRes) {
                logger.error("send offset sendTime message {}, fail!", body);
            }
        } catch (final Throwable ex) {
            logger.error("send offset sendTime message {}, fail with ex", body);
        }
    }

    private byte[] encodePayload(final byte[] payload, String attribute) {
        if (TStringUtils.isBlank(attribute)) {
            return payload;
        }
        byte[] attrData = StringUtils.getBytesUtf8(attribute);
        final ByteBuffer buffer =
            ByteBuffer.allocate(4 + attrData.length + payload.length);
        buffer.putInt(attrData.length);
        buffer.put(attrData);
        buffer.put(payload);
        return buffer.array();
    }


}
