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

package org.apache.inlong.sort.standalone.sink.pulsar;

import org.apache.inlong.common.pojo.sort.ClusterTagConfig;
import org.apache.inlong.common.pojo.sort.TaskConfig;
import org.apache.inlong.common.pojo.sort.node.PulsarNodeConfig;
import org.apache.inlong.common.pojo.sortstandalone.SortTaskConfig;
import org.apache.inlong.sort.standalone.channel.ProfileEvent;
import org.apache.inlong.sort.standalone.config.holder.CommonPropertiesHolder;
import org.apache.inlong.sort.standalone.config.holder.SortClusterConfigHolder;
import org.apache.inlong.sort.standalone.config.holder.v2.SortConfigHolder;
import org.apache.inlong.sort.standalone.config.pojo.CacheClusterConfig;
import org.apache.inlong.sort.standalone.config.pojo.InlongId;
import org.apache.inlong.sort.standalone.metrics.SortConfigMetricReporter;
import org.apache.inlong.sort.standalone.metrics.SortMetricItem;
import org.apache.inlong.sort.standalone.metrics.audit.AuditUtils;
import org.apache.inlong.sort.standalone.sink.SinkContext;
import org.apache.inlong.sort.standalone.utils.InlongLoggerFactory;

import org.apache.commons.lang3.ClassUtils;
import org.apache.flume.Channel;
import org.apache.flume.Context;
import org.slf4j.Logger;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

public class PulsarFederationSinkContext extends SinkContext {

    public static final Logger LOG = InlongLoggerFactory.getLogger(PulsarFederationSinkContext.class);
    public static final String KEY_EVENT_HANDLER = "eventHandler";
    private Map<String, PulsarIdConfig> idConfigMap = new ConcurrentHashMap<>();
    private PulsarNodeConfig pulsarNodeConfig;
    private CacheClusterConfig cacheClusterConfig;

    public PulsarFederationSinkContext(String sinkName, Context context, Channel channel) {
        super(sinkName, context, channel);
    }

    public void reload() {
        try {
            TaskConfig newTaskConfig = SortConfigHolder.getTaskConfig(taskName);
            SortTaskConfig newSortTaskConfig = SortClusterConfigHolder.getTaskConfig(taskName);
            if (newTaskConfig == null && newSortTaskConfig == null) {
                LOG.error("newSortTaskConfig is null.");
                return;
            }
            if ((this.taskConfig != null && this.taskConfig.equals(newTaskConfig))
                    && (this.sortTaskConfig != null && this.sortTaskConfig.equals(newSortTaskConfig))) {
                LOG.info("Same sortTaskConfig, do nothing.");
                return;
            }

            if (newTaskConfig != null) {
                PulsarNodeConfig requestNodeConfig = (PulsarNodeConfig) newTaskConfig.getNodeConfig();
                if (pulsarNodeConfig == null || requestNodeConfig.getVersion() > pulsarNodeConfig.getVersion()) {
                    this.pulsarNodeConfig = requestNodeConfig;
                }
            }

            this.taskConfig = newTaskConfig;
            this.sortTaskConfig = newSortTaskConfig;

            CacheClusterConfig clusterConfig = new CacheClusterConfig();
            clusterConfig.setClusterName(this.taskName);
            clusterConfig.setParams(this.sortTaskConfig.getSinkParams());
            this.cacheClusterConfig = clusterConfig;

            Map<String, PulsarIdConfig> fromTaskConfig = fromTaskConfig(taskConfig);
            Map<String, PulsarIdConfig> fromSortTaskConfig = fromSortTaskConfig(sortTaskConfig);
            SortConfigMetricReporter.reportClusterDiff(clusterId, taskName, fromTaskConfig, fromSortTaskConfig);
            idConfigMap = unifiedConfiguration ? fromTaskConfig : fromSortTaskConfig;
        } catch (Throwable e) {
            LOG.error(e.getMessage(), e);
        }
    }

    public Map<String, PulsarIdConfig> fromTaskConfig(TaskConfig taskConfig) {
        if (taskConfig == null) {
            return new HashMap<>();
        }
        return taskConfig.getClusterTagConfigs()
                .stream()
                .map(ClusterTagConfig::getDataFlowConfigs)
                .flatMap(Collection::stream)
                .map(PulsarIdConfig::create)
                .collect(Collectors.toMap(
                        config -> InlongId.generateUid(config.getInlongGroupId(), config.getInlongStreamId()),
                        v -> v,
                        (v1, v2) -> v1));
    }

    public Map<String, PulsarIdConfig> fromSortTaskConfig(SortTaskConfig sortTaskConfig) {
        if (sortTaskConfig == null) {
            return new HashMap<>();
        }
        Map<String, PulsarIdConfig> newIdConfigMap = new ConcurrentHashMap<>();
        List<Map<String, String>> idList = sortTaskConfig.getIdParams();
        for (Map<String, String> idParam : idList) {
            try {
                PulsarIdConfig idConfig = new PulsarIdConfig(idParam);
                newIdConfigMap.put(idConfig.getUid(), idConfig);
            } catch (Exception e) {
                LOG.error("fail to parse pulsar id config", e);
            }
        }
        return newIdConfigMap;
    }

    public String getTopic(String uid) {
        PulsarIdConfig idConfig = this.idConfigMap.get(uid);
        if (idConfig == null) {
            throw new NullPointerException("uid " + uid + " got null id config");
        }
        return idConfig.getTopic();
    }

    public PulsarIdConfig getIdConfig(String uid) {
        PulsarIdConfig idConfig = this.idConfigMap.get(uid);
        if (idConfig == null) {
            throw new NullPointerException("uid " + uid + "got null PulsarIdConfig");
        }
        return idConfig;
    }

    public PulsarNodeConfig getNodeConfig() {
        return pulsarNodeConfig;
    }

    public CacheClusterConfig getCacheClusterConfig() {
        return cacheClusterConfig;
    }

    public void addSendMetric(ProfileEvent currentRecord, String topic) {
        Map<String, String> dimensions = new HashMap<>();
        dimensions.put(SortMetricItem.KEY_CLUSTER_ID, this.getClusterId());
        dimensions.put(SortMetricItem.KEY_TASK_NAME, this.getTaskName());
        // metric
        fillInlongId(currentRecord, dimensions);
        dimensions.put(SortMetricItem.KEY_SINK_ID, this.getSinkName());
        dimensions.put(SortMetricItem.KEY_SINK_DATA_ID, topic);
        long msgTime = currentRecord.getRawLogTime();
        long auditFormatTime = msgTime - msgTime % CommonPropertiesHolder.getAuditFormatInterval();
        dimensions.put(SortMetricItem.KEY_MESSAGE_TIME, String.valueOf(auditFormatTime));
        SortMetricItem metricItem = this.getMetricItemSet().findMetricItem(dimensions);
        long count = 1;
        long size = currentRecord.getBody().length;
        metricItem.sendCount.addAndGet(count);
        metricItem.sendSize.addAndGet(size);
    }

    /**
     * addReadFailMetric
     */
    public void addSendFailMetric() {
        Map<String, String> dimensions = new HashMap<>();
        dimensions.put(SortMetricItem.KEY_CLUSTER_ID, this.getClusterId());
        dimensions.put(SortMetricItem.KEY_SINK_ID, this.getSinkName());
        long msgTime = System.currentTimeMillis();
        long auditFormatTime = msgTime - msgTime % CommonPropertiesHolder.getAuditFormatInterval();
        dimensions.put(SortMetricItem.KEY_MESSAGE_TIME, String.valueOf(auditFormatTime));
        SortMetricItem metricItem = this.getMetricItemSet().findMetricItem(dimensions);
        metricItem.readFailCount.incrementAndGet();
    }

    /**
     * addSendResultMetric
     *
     * @param currentRecord
     * @param topic
     * @param result
     * @param sendTime
     */
    public void addSendResultMetric(ProfileEvent currentRecord, String topic, boolean result, long sendTime) {
        Map<String, String> dimensions = new HashMap<>();
        dimensions.put(SortMetricItem.KEY_CLUSTER_ID, this.getClusterId());
        dimensions.put(SortMetricItem.KEY_TASK_NAME, this.getTaskName());
        // metric
        fillInlongId(currentRecord, dimensions);
        dimensions.put(SortMetricItem.KEY_SINK_ID, this.getSinkName());
        dimensions.put(SortMetricItem.KEY_SINK_DATA_ID, topic);
        long msgTime = currentRecord.getRawLogTime();
        long auditFormatTime = msgTime - msgTime % CommonPropertiesHolder.getAuditFormatInterval();
        dimensions.put(SortMetricItem.KEY_MESSAGE_TIME, String.valueOf(auditFormatTime));
        SortMetricItem metricItem = this.getMetricItemSet().findMetricItem(dimensions);
        long count = 1;
        long size = currentRecord.getBody().length;
        if (result) {
            metricItem.sendSuccessCount.addAndGet(count);
            metricItem.sendSuccessSize.addAndGet(size);
            AuditUtils.add(AuditUtils.AUDIT_ID_SEND_SUCCESS, currentRecord);
            if (sendTime > 0) {
                long currentTime = System.currentTimeMillis();
                long sinkDuration = currentTime - sendTime;
                long nodeDuration = currentTime - currentRecord.getFetchTime();
                long wholeDuration = currentTime - currentRecord.getRawLogTime();
                metricItem.sinkDuration.addAndGet(sinkDuration * count);
                metricItem.nodeDuration.addAndGet(nodeDuration * count);
                metricItem.wholeDuration.addAndGet(wholeDuration * count);
            }
        } else {
            metricItem.sendFailCount.addAndGet(count);
            metricItem.sendFailSize.addAndGet(size);
        }
    }

    /**
     * create IEvent2PulsarRecordHandler
     *
     * @return IEvent2PulsarRecordHandler
     */
    public IEvent2PulsarRecordHandler createEventHandler() {
        // IEvent2ProducerRecordHandler
        String strHandlerClass = CommonPropertiesHolder.getString(KEY_EVENT_HANDLER,
                DefaultEvent2PulsarRecordHandler.class.getName());
        try {
            Class<?> handlerClass = ClassUtils.getClass(strHandlerClass);
            Object handlerObject = handlerClass.getDeclaredConstructor().newInstance();
            if (handlerObject instanceof IEvent2PulsarRecordHandler) {
                IEvent2PulsarRecordHandler handler = (IEvent2PulsarRecordHandler) handlerObject;
                return handler;
            }
        } catch (Throwable t) {
            LOG.error("Fail to init IEvent2PulsarRecordHandler,handlerClass:{},error:{}",
                    strHandlerClass, t.getMessage(), t);
        }
        return null;
    }
}
