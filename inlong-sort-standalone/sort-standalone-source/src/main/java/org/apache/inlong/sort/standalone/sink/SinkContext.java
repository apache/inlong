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

package org.apache.inlong.sort.standalone.sink;

import org.apache.inlong.common.metric.MetricRegister;
import org.apache.inlong.common.pojo.sort.TaskConfig;
import org.apache.inlong.common.pojo.sortstandalone.SortTaskConfig;
import org.apache.inlong.sort.standalone.channel.ProfileEvent;
import org.apache.inlong.sort.standalone.config.holder.CommonPropertiesHolder;
import org.apache.inlong.sort.standalone.config.holder.SortClusterConfigHolder;
import org.apache.inlong.sort.standalone.config.holder.v2.SortConfigHolder;
import org.apache.inlong.sort.standalone.metrics.SortMetricItem;
import org.apache.inlong.sort.standalone.metrics.SortMetricItemSet;
import org.apache.inlong.sort.standalone.utils.BufferQueue;
import org.apache.inlong.sort.standalone.utils.InlongLoggerFactory;

import org.apache.commons.lang3.StringUtils;
import org.apache.flume.Channel;
import org.apache.flume.Context;
import org.slf4j.Logger;

import java.util.Date;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;

public class SinkContext {

    public static final Logger LOG = InlongLoggerFactory.getLogger(SinkContext.class);
    public static final String KEY_MAX_THREADS = "maxThreads";
    public static final String KEY_PROCESSINTERVAL = "processInterval";
    public static final String KEY_RELOADINTERVAL = "reloadInterval";
    public static final String KEY_TASK_NAME = "taskName";
    public static final String KEY_MAX_BUFFERQUEUE_SIZE_KB = "maxBufferQueueSizeKb";
    public static final int DEFAULT_MAX_BUFFERQUEUE_SIZE_KB = 128 * 1024;
    protected final String clusterId;
    protected final String taskName;
    protected final String sinkName;
    protected final Context sinkContext;
    protected TaskConfig taskConfig;
    @Deprecated
    protected SortTaskConfig sortTaskConfig;
    protected final Channel channel;
    protected final int maxThreads;
    protected final long processInterval;
    protected final long reloadInterval;
    protected final boolean unifiedConfiguration;
    protected final SortMetricItemSet metricItemSet;
    protected Timer reloadTimer;

    public SinkContext(String sinkName, Context context, Channel channel) {
        this.sinkName = sinkName;
        this.sinkContext = context;
        this.channel = channel;
        this.clusterId = sinkContext.getString(CommonPropertiesHolder.KEY_CLUSTER_ID);
        this.taskName = sinkContext.getString(KEY_TASK_NAME);
        this.maxThreads = sinkContext.getInteger(KEY_MAX_THREADS, 10);
        this.processInterval = sinkContext.getInteger(KEY_PROCESSINTERVAL, 100);
        this.reloadInterval = sinkContext.getLong(KEY_RELOADINTERVAL, 60000L);
        this.metricItemSet = new SortMetricItemSet(sinkName);
        this.unifiedConfiguration = CommonPropertiesHolder.useUnifiedConfiguration();
        MetricRegister.register(this.metricItemSet);
    }

    public void start() {
        try {
            this.reload();
            this.setReloadTimer();
        } catch (Exception e) {
            LOG.error("failed to start sink context", e);
        }
    }

    public void close() {
        try {
            this.reloadTimer.cancel();
        } catch (Exception e) {
            LOG.error("failed to close sink context", e);
        }
    }

    protected void setReloadTimer() {
        reloadTimer = new Timer(true);
        TimerTask task = new TimerTask() {

            public void run() {
                reload();
            }
        };
        reloadTimer.schedule(task, new Date(System.currentTimeMillis() + reloadInterval), reloadInterval);
    }

    public void reload() {
        try {
            this.sortTaskConfig = SortClusterConfigHolder.getTaskConfig(taskName);
            this.taskConfig = SortConfigHolder.getTaskConfig(taskName);
        } catch (Throwable e) {
            LOG.error("failed to stop sink context", e);
        }
    }

    public String getClusterId() {
        return clusterId;
    }

    public String getTaskName() {
        return taskName;
    }

    public String getSinkName() {
        return sinkName;
    }

    public Context getSinkContext() {
        return sinkContext;
    }

    public TaskConfig getTaskConfig() {
        return taskConfig;
    }

    public SortTaskConfig getSortTaskConfig() {
        return sortTaskConfig;
    }

    public boolean isUnifiedConfiguration() {
        return unifiedConfiguration;
    }

    public Channel getChannel() {
        return channel;
    }

    public int getMaxThreads() {
        return maxThreads;
    }

    public long getProcessInterval() {
        return processInterval;
    }

    public long getReloadInterval() {
        return reloadInterval;
    }

    public SortMetricItemSet getMetricItemSet() {
        return metricItemSet;
    }

    public static void fillInlongId(ProfileEvent currentRecord, Map<String, String> dimensions) {
        String inlongGroupId = currentRecord.getInlongGroupId();
        inlongGroupId = (StringUtils.isBlank(inlongGroupId)) ? "-" : inlongGroupId;
        String inlongStreamId = currentRecord.getInlongStreamId();
        inlongStreamId = (StringUtils.isBlank(inlongStreamId)) ? "-" : inlongStreamId;
        dimensions.put(SortMetricItem.KEY_INLONG_GROUP_ID, inlongGroupId);
        dimensions.put(SortMetricItem.KEY_INLONG_STREAM_ID, inlongStreamId);
    }

    public static <U> BufferQueue<U> createBufferQueue() {
        int maxBufferQueueSizeKb = CommonPropertiesHolder.getInteger(KEY_MAX_BUFFERQUEUE_SIZE_KB,
                DEFAULT_MAX_BUFFERQUEUE_SIZE_KB);
        BufferQueue<U> dispatchQueue = new BufferQueue<>(maxBufferQueueSizeKb);
        return dispatchQueue;
    }
}
