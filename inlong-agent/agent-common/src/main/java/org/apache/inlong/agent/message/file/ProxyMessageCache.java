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

package org.apache.inlong.agent.message.file;

import org.apache.inlong.agent.conf.InstanceProfile;
import org.apache.inlong.agent.constant.CycleUnitType;
import org.apache.inlong.agent.utils.AgentUtils;
import org.apache.inlong.common.msg.AttributeConstants;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static org.apache.inlong.agent.constant.CommonConstants.DEFAULT_PROXY_INLONG_STREAM_ID_QUEUE_MAX_NUMBER;
import static org.apache.inlong.agent.constant.CommonConstants.DEFAULT_PROXY_PACKAGE_MAX_SIZE;
import static org.apache.inlong.agent.constant.CommonConstants.PROXY_INLONG_STREAM_ID_QUEUE_MAX_NUMBER;
import static org.apache.inlong.agent.constant.CommonConstants.PROXY_PACKAGE_MAX_SIZE;
import static org.apache.inlong.agent.constant.TaskConstants.TASK_AUDIT_VERSION;
import static org.apache.inlong.agent.constant.TaskConstants.TASK_CYCLE_UNIT;
import static org.apache.inlong.common.msg.AttributeConstants.AUDIT_VERSION;

/**
 * Handle List of Proxy Message, which belong to the same stream id.
 */
public class ProxyMessageCache {

    private static final Logger LOGGER = LoggerFactory.getLogger(ProxyMessageCache.class);

    private final String taskId;
    private final String instanceId;
    private final int maxPackSize;
    private final int maxQueueNumber;
    private final String groupId;
    // streamId -> list of proxyMessage
    private final ConcurrentHashMap<String, LinkedBlockingQueue<ProxyMessage>> messageQueueMap;
    private long lastPrintTime = 0;
    private long dataTime;
    private boolean isRealTime = false;
    /**
     * extra map used when sending to dataproxy
     */
    private Map<String, String> extraMap = new HashMap<>();

    public ProxyMessageCache(InstanceProfile instanceProfile, String groupId, String streamId) {
        this.taskId = instanceProfile.getTaskId();
        this.instanceId = instanceProfile.getInstanceId();
        this.groupId = groupId;
        this.maxPackSize = instanceProfile.getInt(PROXY_PACKAGE_MAX_SIZE, DEFAULT_PROXY_PACKAGE_MAX_SIZE);
        this.maxQueueNumber = instanceProfile.getInt(PROXY_INLONG_STREAM_ID_QUEUE_MAX_NUMBER,
                DEFAULT_PROXY_INLONG_STREAM_ID_QUEUE_MAX_NUMBER);
        messageQueueMap = new ConcurrentHashMap<>();
        dataTime = instanceProfile.getSinkDataTime();
        extraMap.put(AttributeConstants.MESSAGE_SYNC_SEND, "false");
        extraMap.putAll(AgentUtils.parseAddAttrToMap(instanceProfile.getPredefineFields()));
        extraMap.put(AUDIT_VERSION, instanceProfile.get(TASK_AUDIT_VERSION));
        String cycleUnit = instanceProfile.get(TASK_CYCLE_UNIT);
        if (cycleUnit.equalsIgnoreCase(CycleUnitType.REAL_TIME)) {
            isRealTime = true;
        }
    }

    /**
     * Check whether queue is nearly full
     *
     * @return true if is nearly full else false.
     */
    private boolean queueIsFull(LinkedBlockingQueue<ProxyMessage> messageQueue) {
        return messageQueue.size() >= maxQueueNumber - 1;
    }

    /**
     * Add proxy message to cache, proxy message should belong to the same stream id.
     */
    public boolean add(ProxyMessage message) {
        String streamId = message.getInlongStreamId();
        LinkedBlockingQueue<ProxyMessage> messageQueue = makeSureQueueExist(streamId);
        try {
            if (queueIsFull(messageQueue)) {
                printQueueFull();
                return false;
            }
            messageQueue.put(message);
            return true;
        } catch (Exception ex) {
            LOGGER.error("exception caught", ex);
        }
        return false;
    }

    private void printQueueFull() {
        if (AgentUtils.getCurrentTime() - lastPrintTime > TimeUnit.SECONDS.toMillis(1)) {
            lastPrintTime = AgentUtils.getCurrentTime();
            LOGGER.warn("message queue is greater than {}, stop adding message, "
                    + "maybe proxy get stuck", maxQueueNumber);
        }
    }

    public ConcurrentHashMap<String, LinkedBlockingQueue<ProxyMessage>> getMessageQueueMap() {
        return messageQueueMap;
    }

    private LinkedBlockingQueue<ProxyMessage> makeSureQueueExist(String streamId) {
        LinkedBlockingQueue<ProxyMessage> messageQueue = messageQueueMap.get(streamId);
        if (messageQueue == null) {
            messageQueue = new LinkedBlockingQueue<>();
            messageQueueMap.put(streamId, messageQueue);
        }
        return messageQueue;
    }

    /**
     * Fetch batch of proxy message, timeout message or max number of list satisfied.
     *
     * @return map of message list, key is stream id for the batch; return null if there are no valid messages.
     */
    public SenderMessage fetchSenderMessage(String streamId, LinkedBlockingQueue<ProxyMessage> messageQueue) {
        int resultBatchSize = 0;
        List<byte[]> bodyList = new ArrayList<>();
        List<OffsetAckInfo> offsetList = new ArrayList<>();
        while (!messageQueue.isEmpty()) {
            // pre check message size
            ProxyMessage peekMessage = messageQueue.peek();
            int peekMessageLength = peekMessage.getBody().length;
            if (resultBatchSize + peekMessageLength > maxPackSize) {
                break;
            }
            ProxyMessage message = messageQueue.remove();
            int bodySize = message.getBody().length;
            if (peekMessageLength > maxPackSize) {
                LOGGER.warn("message size is {}, greater than max pack size {}, drop it!",
                        peekMessage.getBody().length, maxPackSize);
                messageQueue.remove();
                break;
            }
            resultBatchSize += bodySize;
            // decrease queue size.
            bodyList.add(message.getBody());
            offsetList.add(message.getAckInfo());
        }
        // make sure result is not empty.
        long auditTime = 0;
        if (isRealTime) {
            auditTime = AgentUtils.getCurrentTime();
        } else {
            auditTime = dataTime;
        }
        if (!bodyList.isEmpty()) {
            SenderMessage senderMessage = new SenderMessage(taskId, instanceId, groupId, streamId, bodyList,
                    auditTime, extraMap, offsetList);
            return senderMessage;
        }
        return null;
    }
}
