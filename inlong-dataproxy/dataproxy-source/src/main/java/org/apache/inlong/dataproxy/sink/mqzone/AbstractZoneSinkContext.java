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

package org.apache.inlong.dataproxy.sink.mqzone;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import org.apache.commons.lang3.StringUtils;
import org.apache.flume.Channel;
import org.apache.flume.Context;
import org.apache.inlong.common.metric.MetricRegister;
import org.apache.inlong.dataproxy.config.RemoteConfigManager;
import org.apache.inlong.dataproxy.config.holder.CacheClusterConfigHolder;
import org.apache.inlong.dataproxy.config.holder.CommonPropertiesHolder;
import org.apache.inlong.dataproxy.config.holder.IdTopicConfigHolder;
import org.apache.inlong.dataproxy.dispatch.DispatchProfile;
import org.apache.inlong.dataproxy.metrics.DataProxyMetricItem;
import org.apache.inlong.dataproxy.metrics.DataProxyMetricItemSet;
import org.apache.inlong.dataproxy.metrics.audit.AuditUtils;
import org.apache.inlong.sdk.commons.protocol.ProxySdk;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.Timer;
import java.util.TimerTask;

/**
 * SinkContext
 */
public abstract class AbstractZoneSinkContext {

    public static final Logger LOG = LoggerFactory.getLogger(AbstractZoneSinkContext.class);

    public static final String KEY_MAX_THREADS = "maxThreads";
    public static final String KEY_PROCESS_INTERVAL = "processInterval";
    public static final String KEY_RELOAD_INTERVAL = "reloadInterval";

    protected final String sinkName;
    protected final Context sinkContext;

    protected final Channel channel;

    protected final int maxThreads;
    protected final long processInterval;
    protected final long reloadInterval;

    protected final DataProxyMetricItemSet metricItemSet;
    protected Timer reloadTimer;


    public static final String KEY_NODE_ID = "nodeId";
    public static final String PREFIX_PRODUCER = "producer.";
    public static final String KEY_COMPRESS_TYPE = "compressType";

    protected ArrayList<LinkedBlockingQueue<DispatchProfile>> dispatchQueues = new ArrayList<>();

    protected final String proxyClusterId;
    protected final String nodeId;
    protected final Context producerContext;
    protected final IdTopicConfigHolder idTopicHolder;
    protected final CacheClusterConfigHolder cacheHolder;
    protected final ProxySdk.INLONG_COMPRESSED_TYPE compressType;

    /**
     * Constructor
     */
    public AbstractZoneSinkContext(String sinkName, Context context, Channel channel,
                                   ArrayList<LinkedBlockingQueue<DispatchProfile>> dispatchQueues) {
        this.sinkName = sinkName;
        this.sinkContext = context;
        this.channel = channel;
        this.maxThreads = sinkContext.getInteger(KEY_MAX_THREADS, 10);
        this.processInterval = sinkContext.getInteger(KEY_PROCESS_INTERVAL, 100);
        this.reloadInterval = sinkContext.getLong(KEY_RELOAD_INTERVAL, 60000L);
        //
        this.metricItemSet = new DataProxyMetricItemSet(sinkName);
        MetricRegister.register(this.metricItemSet);

        this.dispatchQueues = dispatchQueues;
        // proxyClusterId
        this.proxyClusterId = CommonPropertiesHolder.getString(RemoteConfigManager.KEY_PROXY_CLUSTER_NAME);
        // nodeId
        this.nodeId = CommonPropertiesHolder.getString(KEY_NODE_ID, "127.0.0.1");
        // compressionType
        String strCompressionType = CommonPropertiesHolder.getString(KEY_COMPRESS_TYPE,
                ProxySdk.INLONG_COMPRESSED_TYPE.INLONG_SNAPPY.name());
        this.compressType = ProxySdk.INLONG_COMPRESSED_TYPE.valueOf(strCompressionType);
        // producerContext
        Map<String, String> producerParams = context.getSubProperties(PREFIX_PRODUCER);
        this.producerContext = new Context(producerParams);
        // idTopicHolder
        Context commonPropertiesContext = new Context(CommonPropertiesHolder.get());
        this.idTopicHolder = new IdTopicConfigHolder();
        this.idTopicHolder.configure(commonPropertiesContext);
        // cacheHolder
        this.cacheHolder = new CacheClusterConfigHolder();
        this.cacheHolder.configure(commonPropertiesContext);
    }

    /**
     * start
     */
    public void start() {
        try {
            this.reload();
            this.setReloadTimer();
            this.idTopicHolder.start();
            this.cacheHolder.start();
        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
        }
    }

    /**
     * close
     */
    public void close() {
        try {
            this.reloadTimer.cancel();
            this.idTopicHolder.close();
            this.cacheHolder.close();
        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
        }
    }

    /**
     * setReloadTimer
     */
    protected void setReloadTimer() {
        reloadTimer = new Timer(true);
        TimerTask task = new TimerTask() {

            public void run() {
                reload();
            }
        };
        reloadTimer.schedule(task, new Date(System.currentTimeMillis() + reloadInterval), reloadInterval);
    }

    /**
     * reload
     */
    public void reload() {
    }

    /**
     * get sinkName
     *
     * @return the sinkName
     */
    public String getSinkName() {
        return sinkName;
    }

    /**
     * get sinkContext
     *
     * @return the sinkContext
     */
    public Context getSinkContext() {
        return sinkContext;
    }

    /**
     * get channel
     *
     * @return the channel
     */
    public Channel getChannel() {
        return channel;
    }

    /**
     * get maxThreads
     *
     * @return the maxThreads
     */
    public int getMaxThreads() {
        return maxThreads;
    }

    /**
     * get processInterval
     *
     * @return the processInterval
     */
    public long getProcessInterval() {
        return processInterval;
    }

    /**
     * get reloadInterval
     *
     * @return the reloadInterval
     */
    public long getReloadInterval() {
        return reloadInterval;
    }

    /**
     * get metricItemSet
     *
     * @return the metricItemSet
     */
    public DataProxyMetricItemSet getMetricItemSet() {
        return metricItemSet;
    }

    /**
     * get proxyClusterId
     *
     * @return the proxyClusterId
     */
    public String getProxyClusterId() {
        return proxyClusterId;
    }

    /**
     * get producerContext
     *
     * @return the producerContext
     */
    public Context getProducerContext() {
        return producerContext;
    }

    /**
     * get idTopicHolder
     *
     * @return the idTopicHolder
     */
    public IdTopicConfigHolder getIdTopicHolder() {
        return idTopicHolder;
    }

    /**
     * get cacheHolder
     *
     * @return the cacheHolder
     */
    public CacheClusterConfigHolder getCacheHolder() {
        return cacheHolder;
    }

    /**
     * get compressType
     *
     * @return the compressType
     */
    public ProxySdk.INLONG_COMPRESSED_TYPE getCompressType() {
        return compressType;
    }

    /**
     * get nodeId
     *
     * @return the nodeId
     */
    public String getNodeId() {
        return nodeId;
    }

    /**
     * fillInlongId
     *
     * @param currentRecord
     * @param dimensions
     */
    public static void fillInlongId(DispatchProfile currentRecord, Map<String, String> dimensions) {
        String inlongGroupId = currentRecord.getInlongGroupId();
        inlongGroupId = (StringUtils.isBlank(inlongGroupId)) ? "-" : inlongGroupId;
        String inlongStreamId = currentRecord.getInlongStreamId();
        inlongStreamId = (StringUtils.isBlank(inlongStreamId)) ? "-" : inlongStreamId;
        dimensions.put(DataProxyMetricItem.KEY_INLONG_GROUP_ID, inlongGroupId);
        dimensions.put(DataProxyMetricItem.KEY_INLONG_STREAM_ID, inlongStreamId);
    }

    /**
     * addSendResultMetric
     *
     * @param currentRecord
     * @param bid
     * @param result
     * @param sendTime
     */
    public void addSendResultMetric(DispatchProfile currentRecord, String bid, boolean result, long sendTime) {
        Map<String, String> dimensions = new HashMap<>();
        dimensions.put(DataProxyMetricItem.KEY_CLUSTER_ID, this.getProxyClusterId());
        // metric
        fillInlongId(currentRecord, dimensions);
        dimensions.put(DataProxyMetricItem.KEY_SINK_ID, this.getSinkName());
        dimensions.put(DataProxyMetricItem.KEY_SINK_DATA_ID, bid);
        long dispatchTime = currentRecord.getDispatchTime();
        long auditFormatTime = dispatchTime - dispatchTime % CommonPropertiesHolder.getAuditFormatInterval();
        dimensions.put(DataProxyMetricItem.KEY_MESSAGE_TIME, String.valueOf(auditFormatTime));
        DataProxyMetricItem metricItem = this.getMetricItemSet().findMetricItem(dimensions);
        long count = currentRecord.getCount();
        long size = currentRecord.getSize();
        if (result) {
            metricItem.sendSuccessCount.addAndGet(count);
            metricItem.sendSuccessSize.addAndGet(size);
            currentRecord.getEvents().forEach((event) -> {
                AuditUtils.add(AuditUtils.AUDIT_ID_DATAPROXY_SEND_SUCCESS, event);
            });
            if (sendTime > 0) {
                long currentTime = System.currentTimeMillis();
                currentRecord.getEvents().forEach((event) -> {
                    long sinkDuration = currentTime - sendTime;
                    long nodeDuration = currentTime - event.getSourceTime();
                    long wholeDuration = currentTime - event.getMsgTime();
                    metricItem.sinkDuration.addAndGet(sinkDuration);
                    metricItem.nodeDuration.addAndGet(nodeDuration);
                    metricItem.wholeDuration.addAndGet(wholeDuration);
                });
            }
        } else {
            metricItem.sendFailCount.addAndGet(count);
            metricItem.sendFailSize.addAndGet(size);
        }
    }

    /**
     * get dispatchQueue
     *
     * @return the dispatchQueue
     */
    public ArrayList<LinkedBlockingQueue<DispatchProfile>> getDispatchQueues() {
        return dispatchQueues;
    }

    public void setDispatchQueues(
            ArrayList<LinkedBlockingQueue<DispatchProfile>> dispatchQueues) {
        this.dispatchQueues = dispatchQueues;
    }

    /**
     * processSendFail
     * @param currentRecord
     * @param producerTopic
     * @param sendTime
     */
    public void processSendFail(DispatchProfile currentRecord, String producerTopic, long sendTime) {
        if (currentRecord.isResend()) {
            dispatchQueues.get(currentRecord.getSendIndex() % maxThreads).offer(currentRecord);
            this.addSendResultMetric(currentRecord, producerTopic, false, sendTime);
        } else {
            currentRecord.fail();
        }
    }

    /**
     * addSendFailMetric
     */
    public void addSendFailMetric() {
        Map<String, String> dimensions = new HashMap<>();
        dimensions.put(DataProxyMetricItem.KEY_CLUSTER_ID, this.getProxyClusterId());
        dimensions.put(DataProxyMetricItem.KEY_SINK_ID, this.getSinkName());
        long msgTime = System.currentTimeMillis();
        long auditFormatTime = msgTime - msgTime % CommonPropertiesHolder.getAuditFormatInterval();
        dimensions.put(DataProxyMetricItem.KEY_MESSAGE_TIME, String.valueOf(auditFormatTime));
        DataProxyMetricItem metricItem = this.getMetricItemSet().findMetricItem(dimensions);
        metricItem.sendFailCount.incrementAndGet();
    }

    /**
     * addSendMetric
     *
     * @param currentRecord
     * @param bid
     */
    public void addSendMetric(DispatchProfile currentRecord, String bid) {
        Map<String, String> dimensions = new HashMap<>();
        dimensions.put(DataProxyMetricItem.KEY_CLUSTER_ID, this.getProxyClusterId());
        // metric
        fillInlongId(currentRecord, dimensions);
        dimensions.put(DataProxyMetricItem.KEY_SINK_ID, this.getSinkName());
        dimensions.put(DataProxyMetricItem.KEY_SINK_DATA_ID, bid);
        long msgTime = currentRecord.getDispatchTime();
        long auditFormatTime = msgTime - msgTime % CommonPropertiesHolder.getAuditFormatInterval();
        dimensions.put(DataProxyMetricItem.KEY_MESSAGE_TIME, String.valueOf(auditFormatTime));
        DataProxyMetricItem metricItem = this.getMetricItemSet().findMetricItem(dimensions);
        long count = currentRecord.getCount();
        long size = currentRecord.getSize();
        metricItem.sendCount.addAndGet(count);
        metricItem.sendSize.addAndGet(size);
    }

}
