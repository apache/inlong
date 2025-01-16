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

package org.apache.inlong.sdk.dataproxy.threads;

import org.apache.inlong.sdk.dataproxy.ProxyClientConfig;
import org.apache.inlong.sdk.dataproxy.codec.EncodeObject;
import org.apache.inlong.sdk.dataproxy.common.SendMessageCallback;
import org.apache.inlong.sdk.dataproxy.common.SendResult;
import org.apache.inlong.sdk.dataproxy.metric.MessageRecord;
import org.apache.inlong.sdk.dataproxy.metric.MetricConfig;
import org.apache.inlong.sdk.dataproxy.metric.MetricTimeNumSummary;
import org.apache.inlong.sdk.dataproxy.network.Sender;
import org.apache.inlong.sdk.dataproxy.network.SequentialID;
import org.apache.inlong.sdk.dataproxy.utils.ProxyUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.util.Collections;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * metric worker
 */
public class MetricWorkerThread extends Thread implements Closeable {

    private static final long DEF_METRIC_DELAY_TIME_MS = 20 * 1000L;
    private static final String DEFAULT_KEY_ITEM = "";
    private static final String DEFAULT_KEY_SPLITTER = "#";
    private final Logger logger = LoggerFactory.getLogger(MetricWorkerThread.class);

    private final SequentialID idGenerator = new SequentialID();
    private final ConcurrentHashMap<String, MessageRecord> metricValueCache = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, MetricTimeNumSummary> metricPackTimeMap = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, MetricTimeNumSummary> metricDtMap = new ConcurrentHashMap<>();
    private final MetricConfig metricConfig;
    private final long delayTime = DEF_METRIC_DELAY_TIME_MS;
    private final Sender sender;
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    private volatile boolean bShutdown = false;

    public MetricWorkerThread(ProxyClientConfig proxyClientConfig, Sender sender) {
        this.metricConfig = proxyClientConfig.getMetricConfig();
        this.sender = sender;
        this.setDaemon(true);
        this.setName("MetricWorkerThread");
    }

    public long getFormatKeyTime(long keyTime) {
        return keyTime - keyTime % metricConfig.getDateFormatIntvlMs();
    }

    /**
     * get string key
     */
    private String getKeyStringByConfig(String groupId, String streamId, String localIp, long keyTime) {
        StringBuilder builder = new StringBuilder();
        String groupIdStr = metricConfig.isUseGroupIdAsKey() ? groupId : DEFAULT_KEY_ITEM;
        String streamIdStr = metricConfig.isUseStreamIdAsKey() ? streamId : DEFAULT_KEY_ITEM;
        String localIpStr = metricConfig.isUseLocalIpAsKey() ? localIp : DEFAULT_KEY_ITEM;

        builder.append(groupIdStr).append(DEFAULT_KEY_SPLITTER)
                .append(streamIdStr).append(DEFAULT_KEY_SPLITTER)
                .append(localIpStr).append(DEFAULT_KEY_SPLITTER)
                .append(keyTime);
        return builder.toString();
    }

    /**
     * record num
     *
     * @param msgId msg uuid
     * @param groupId groupId
     * @param streamId streamId
     * @param localIp ip
     * @param packTime package time
     * @param dt dt
     * @param num num
     */
    public void recordNumByKey(String msgId, String groupId, String streamId,
            String localIp, long packTime, long dt, int num) {
        if (!metricConfig.isEnableMetric()) {
            return;
        }
        MessageRecord messageRecord = new MessageRecord(groupId, streamId, localIp, msgId,
                getFormatKeyTime(dt), getFormatKeyTime(packTime), num);

        metricValueCache.putIfAbsent(msgId, messageRecord);
    }

    private MetricTimeNumSummary getMetricSummary(String keyName, MetricTimeNumSummary summary,
            ConcurrentHashMap<String, MetricTimeNumSummary> cacheMap) {
        MetricTimeNumSummary finalSummary = cacheMap.putIfAbsent(keyName, summary);
        if (finalSummary == null) {
            finalSummary = summary;
        }
        return finalSummary;
    }

    /**
     * record success num
     *
     * @param msgId msg id
     */
    public void recordSuccessByMessageId(String msgId) {
        if (!metricConfig.isEnableMetric()) {
            return;
        }
        MessageRecord messageRecord = metricValueCache.remove(msgId);
        if (messageRecord != null) {
            String packTimeKeyName = getKeyStringByConfig(messageRecord.getGroupId(), messageRecord.getStreamId(),
                    messageRecord.getLocalIp(), messageRecord.getPackTime());
            String dtKeyName = getKeyStringByConfig(messageRecord.getGroupId(), messageRecord.getStreamId(),
                    messageRecord.getLocalIp(), messageRecord.getDt());

            MetricTimeNumSummary packTimeSummary = getMetricSummary(packTimeKeyName,
                    new MetricTimeNumSummary(messageRecord.getPackTime()), metricPackTimeMap);

            MetricTimeNumSummary dtSummary = getMetricSummary(dtKeyName,
                    new MetricTimeNumSummary(messageRecord.getDt()), metricDtMap);

            packTimeSummary.recordSuccessSendTime(messageRecord.getMessageTime(), messageRecord.getMsgCount());
            dtSummary.increaseSuccessNum(messageRecord.getMsgCount());
        }
    }

    /**
     * record failed num
     *
     * @param msgId msg id
     */
    public void recordFailedByMessageId(String msgId) {
        MessageRecord messageRecord = metricValueCache.remove(msgId);
        if (messageRecord != null) {
            String packTimeKeyName = getKeyStringByConfig(messageRecord.getGroupId(), messageRecord.getStreamId(),
                    messageRecord.getLocalIp(), messageRecord.getPackTime());
            String dtKeyName = getKeyStringByConfig(messageRecord.getGroupId(), messageRecord.getStreamId(),
                    messageRecord.getLocalIp(), messageRecord.getDt());

            MetricTimeNumSummary packTimeSummary = getMetricSummary(packTimeKeyName,
                    new MetricTimeNumSummary(messageRecord.getMessageTime()), metricPackTimeMap);

            MetricTimeNumSummary dtSummary = getMetricSummary(dtKeyName,
                    new MetricTimeNumSummary(messageRecord.getDt()), metricDtMap);

            packTimeSummary.increaseFailedNum(messageRecord.getMsgCount());
            dtSummary.increaseFailedNum(messageRecord.getMsgCount());
        }
    }

    @Override
    public void close() {
        bShutdown = true;
        flushMetric(true);
        logger.info("MetricWorkerThread closed!");
    }

    @Override
    public void run() {
        logger.info("MetricWorkerThread thread=" + Thread.currentThread().getId() + " started!");
        while (!bShutdown) {
            // check metric
            try {
                checkCacheRecords();
                flushMetric(false);
                TimeUnit.MILLISECONDS.sleep(metricConfig.getMetricRptIntvlMs());
            } catch (Throwable ex) {
                // exception happens
            }
        }
        logger.info("MetricWorkerThread thread existed!");
    }

    private void tryToSendMetricToManager(EncodeObject encodeObject, MetricSendCallBack callBack) {
        callBack.increaseRetry();
        try {
            if (callBack.getRetryCount() < 4) {
                sender.asyncSendMessage(encodeObject, callBack, String.valueOf(System.currentTimeMillis()));
            } else {
                logger.error("Send metric failure: {}", encodeObject.getBodylist());
            }
        } catch (Throwable ex) {
            logger.warn("Send metric throw exception", ex);
            tryToSendMetricToManager(encodeObject, callBack);
        }
    }

    private void sendSingleLine(String line, String streamId, long dtTime) {
        EncodeObject encodeObject = new EncodeObject(Collections.singletonList(line.getBytes()), 7,
                false, false, false,
                dtTime, idGenerator.getNextInt(),
                metricConfig.getMetricGroupId(), streamId, "", "", ProxyUtils.getLocalIp());
        MetricSendCallBack callBack = new MetricSendCallBack(encodeObject);
        tryToSendMetricToManager(encodeObject, callBack);
    }

    private void flushMapRecords(boolean isClosing, ConcurrentHashMap<String, MetricTimeNumSummary> cacheMap) {
        for (String keyName : cacheMap.keySet()) {
            MetricTimeNumSummary summary = cacheMap.get(keyName);
            if (isClosing || (summary != null && summary.getSummaryTime()
                    + delayTime > metricConfig.getMetricRptIntvlMs())) {
                summary = cacheMap.remove(keyName);
                if (summary != null) {
                    long metricDtTime = summary.getStartCalculateTime() / 1000;
                    // send to manager cluster.
                    String countLine = keyName + DEFAULT_KEY_SPLITTER + summary.getSuccessNum()
                            + DEFAULT_KEY_SPLITTER + summary.getFailedNum()
                            + DEFAULT_KEY_SPLITTER + summary.getTotalNum();
                    String timeLine = keyName + DEFAULT_KEY_SPLITTER + summary.getTimeString();
                    logger.info("Send metric countLine={}, timeLine={}", countLine, timeLine);
                    sendSingleLine(countLine, "count", metricDtTime);
                    sendSingleLine(timeLine, "time", metricDtTime);
                }
            }
        }
    }

    /**
     * flush records
     */
    private void flushRecords(boolean isClosing) {
        flushMapRecords(isClosing, metricDtMap);
        flushMapRecords(isClosing, metricPackTimeMap);
    }

    /**
     * check cache records
     */
    private void checkCacheRecords() {
        for (String msgId : metricValueCache.keySet()) {
            MessageRecord record = metricValueCache.get(msgId);
            if (record != null && record.getMessageTime() + delayTime > metricConfig.getMetricRptIntvlMs()) {
                recordFailedByMessageId(msgId);
            }
        }
    }

    /**
     * flush metric
     *
     * @param isClosing whether is closing
     */
    private void flushMetric(boolean isClosing) {
        lock.writeLock().lock();
        try {
            flushRecords(isClosing);
        } finally {
            lock.writeLock().unlock();
        }
    }

    private class MetricSendCallBack implements SendMessageCallback {

        private final EncodeObject encodeObject;
        private int retryCount = 0;

        public MetricSendCallBack(EncodeObject encodeObject) {
            this.encodeObject = encodeObject;
        }

        public void increaseRetry() {
            retryCount += 1;
        }

        public int getRetryCount() {
            return retryCount;
        }

        @Override
        public void onMessageAck(SendResult result) {
            if (SendResult.OK.equals(result)) {
                logger.debug("Send metric is ok!");
            } else {
                tryToSendMetricToManager(encodeObject, this);
            }
        }

        @Override
        public void onException(Throwable e) {
            //
        }
    }
}
