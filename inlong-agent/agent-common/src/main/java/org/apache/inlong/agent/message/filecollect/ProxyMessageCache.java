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

package org.apache.inlong.agent.message.filecollect;

import org.apache.inlong.agent.conf.InstanceProfile;
import org.apache.inlong.agent.constant.TaskConstants;
import org.apache.inlong.agent.message.ProxyMessage;
import org.apache.inlong.agent.utils.AgentUtils;
import org.apache.inlong.common.msg.AttributeConstants;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.apache.inlong.agent.constant.CommonConstants.DEFAULT_PROXY_INLONG_STREAM_ID_QUEUE_MAX_NUMBER;
import static org.apache.inlong.agent.constant.CommonConstants.DEFAULT_PROXY_PACKAGE_MAX_SIZE;
import static org.apache.inlong.agent.constant.CommonConstants.DEFAULT_PROXY_PACKAGE_MAX_TIMEOUT_MS;
import static org.apache.inlong.agent.constant.CommonConstants.PROXY_INLONG_STREAM_ID_QUEUE_MAX_NUMBER;
import static org.apache.inlong.agent.constant.CommonConstants.PROXY_PACKAGE_MAX_SIZE;
import static org.apache.inlong.agent.constant.CommonConstants.PROXY_PACKAGE_MAX_TIMEOUT_MS;

/**
 * Handle List of BusMessage, which belong to the same stream id.
 */
public class ProxyMessageCache {

    private static final Logger LOGGER = LoggerFactory.getLogger(ProxyMessageCache.class);

    private final String groupId;
    private final String streamId;
    private final String taskId;
    private final String instanceId;
    private final int maxPackSize;
    private final int maxQueueNumber;
    private final String inodeInfo;
    // ms
    private final int cacheTimeout;
    // streamId -> list of proxyMessage
    private final LinkedBlockingQueue<ProxyMessage> messageQueue;
    private final AtomicLong cacheSize = new AtomicLong(0);
    private Long packageIndex = 0L;
    private long lastPrintTime = 0;
    /**
     * extra map used when sending to dataproxy
     */
    private Map<String, String> extraMap = new HashMap<>();

    /**
     * Init PackBusMessage
     */
    public ProxyMessageCache(InstanceProfile instanceProfile, String groupId, String streamId) {
        this.taskId = instanceProfile.getTaskId();
        this.instanceId = instanceProfile.getInstanceId();
        this.maxPackSize = instanceProfile.getInt(PROXY_PACKAGE_MAX_SIZE, DEFAULT_PROXY_PACKAGE_MAX_SIZE);
        this.maxQueueNumber = instanceProfile.getInt(PROXY_INLONG_STREAM_ID_QUEUE_MAX_NUMBER,
                DEFAULT_PROXY_INLONG_STREAM_ID_QUEUE_MAX_NUMBER);
        this.cacheTimeout = instanceProfile.getInt(PROXY_PACKAGE_MAX_TIMEOUT_MS, DEFAULT_PROXY_PACKAGE_MAX_TIMEOUT_MS);
        // double size of package
        this.messageQueue = new LinkedBlockingQueue<>(maxQueueNumber);
        this.groupId = groupId;
        this.streamId = streamId;
        this.inodeInfo = instanceProfile.get(TaskConstants.INODE_INFO);
        extraMap.put(AttributeConstants.MESSAGE_SYNC_SEND, "false");
    }

    public void generateExtraMap(String dataKey) {
        this.extraMap.put(AttributeConstants.MESSAGE_PARTITION_KEY, dataKey);
    }

    /**
     * Check whether queue is nearly full
     *
     * @return true if is nearly full else false.
     */
    private boolean queueIsFull() {
        return messageQueue.size() >= maxQueueNumber - 1;
    }

    /**
     * Add proxy message to cache, proxy message should belong to the same stream id.
     */
    public boolean addProxyMessage(ProxyMessage message) {
        assert streamId.equals(message.getInlongStreamId());
        try {
            if (queueIsFull()) {
                if (AgentUtils.getCurrentTime() - lastPrintTime > TimeUnit.SECONDS.toMillis(1)) {
                    lastPrintTime = AgentUtils.getCurrentTime();
                    LOGGER.warn("message queue is greater than {}, stop adding message, "
                            + "maybe proxy get stuck", maxQueueNumber);
                }
                return false;
            }
            messageQueue.put(message);
            cacheSize.addAndGet(message.getBody().length);
            return true;
        } catch (Exception ex) {
            LOGGER.error("exception caught", ex);
        }
        return false;
    }

    /**
     * check message queue is empty or not
     */
    public boolean isEmpty() {
        return messageQueue.isEmpty();
    }

    /**
     * Fetch batch of proxy message, timeout message or max number of list satisfied.
     *
     * @return map of message list, key is stream id for the batch; return null if there are no valid messages.
     */
    public SenderMessage fetchSenderMessage() {
        int resultBatchSize = 0;
        List<byte[]> bodyList = new ArrayList<>();
        Long packageOffset = TaskConstants.DEFAULT_OFFSET;
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
                cacheSize.addAndGet(-bodySize);
                messageQueue.remove();
                break;
            }
            resultBatchSize += bodySize;
            // decrease queue size.
            cacheSize.addAndGet(-bodySize);
            bodyList.add(message.getBody());
            Long newOffset = Long.parseLong(message.getHeader().get(TaskConstants.OFFSET));
            if (packageOffset < newOffset) {
                packageOffset = newOffset;
            }
        }
        // make sure result is not empty.
        if (!bodyList.isEmpty()) {
            PackageAckInfo ackInfo = new PackageAckInfo(packageIndex, packageOffset, resultBatchSize, false);
            SenderMessage senderMessage = new SenderMessage(taskId, instanceId, groupId, streamId, bodyList,
                    AgentUtils.getCurrentTime(), extraMap, ackInfo);
            packageIndex++;
            return senderMessage;
        }
        return null;
    }

    public Map<String, String> getExtraMap() {
        return extraMap;
    }

    public long getCacheSize() {
        return cacheSize.get();
    }

}
