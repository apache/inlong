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

package org.apache.inlong.sort.standalone.sink.kafka;

import org.apache.inlong.common.pojo.sort.ClusterTagConfig;
import org.apache.inlong.common.pojo.sort.TaskConfig;
import org.apache.inlong.common.pojo.sort.node.KafkaNodeConfig;
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
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/** Context of kafka sink. */
public class KafkaFederationSinkContext extends SinkContext {

    public static final Logger LOG = InlongLoggerFactory.getLogger(KafkaFederationSinkContext.class);
    public static final String KEY_EVENT_HANDLER = "eventHandler";

    private KafkaNodeConfig kafkaNodeConfig;
    private CacheClusterConfig cacheClusterConfig;
    private Map<String, KafkaIdConfig> idConfigMap = new ConcurrentHashMap<>();

    public KafkaFederationSinkContext(String sinkName, Context context, Channel channel) {
        super(sinkName, context, channel);
    }

    /** reload context */
    @Override
    public void reload() {
        LOG.info("reload KafkaFederationSinkContext.");
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
                KafkaNodeConfig requestNodeConfig = (KafkaNodeConfig) newTaskConfig.getNodeConfig();
                if (kafkaNodeConfig == null || requestNodeConfig.getVersion() > kafkaNodeConfig.getVersion()) {
                    this.kafkaNodeConfig = requestNodeConfig;
                }
            }

            this.taskConfig = newTaskConfig;
            this.sortTaskConfig = newSortTaskConfig;

            CacheClusterConfig clusterConfig = new CacheClusterConfig();
            clusterConfig.setClusterName(this.taskName);
            if (this.sortTaskConfig != null) {
                clusterConfig.setParams(this.sortTaskConfig.getSinkParams());
            }
            this.cacheClusterConfig = clusterConfig;

            Map<String, KafkaIdConfig> fromTaskConfig = fromTaskConfig(taskConfig);
            Map<String, KafkaIdConfig> fromSortTaskConfig = fromSortTaskConfig(sortTaskConfig);
            SortConfigMetricReporter.reportClusterDiff(clusterId, taskName, fromTaskConfig, fromSortTaskConfig);
            this.idConfigMap = unifiedConfiguration ? fromTaskConfig : fromSortTaskConfig;
        } catch (Throwable e) {
            LOG.error(e.getMessage(), e);
        }
    }

    public Map<String, KafkaIdConfig> fromTaskConfig(TaskConfig taskConfig) {
        if (taskConfig == null) {
            return new HashMap<>();
        }
        return taskConfig.getClusterTagConfigs()
                .stream()
                .map(ClusterTagConfig::getDataFlowConfigs)
                .flatMap(Collection::stream)
                .map(KafkaIdConfig::create)
                .collect(Collectors.toMap(
                        config -> InlongId.generateUid(config.getInlongGroupId(), config.getInlongStreamId()),
                        v -> v,
                        (flow1, flow2) -> flow1));
    }

    public Map<String, KafkaIdConfig> fromSortTaskConfig(SortTaskConfig sortTaskConfig) {
        if (sortTaskConfig == null) {
            return new HashMap<>();
        }

        List<Map<String, String>> idList = sortTaskConfig.getIdParams();
        Map<String, KafkaIdConfig> newIdConfigMap = new ConcurrentHashMap<>();
        for (Map<String, String> idParam : idList) {
            try {
                KafkaIdConfig idConfig = new KafkaIdConfig(idParam);
                newIdConfigMap.put(idConfig.getUid(), idConfig);
            } catch (Exception e) {
                LOG.error("fail to parse kafka id config", e);
            }
        }
        return newIdConfigMap;
    }

    public KafkaNodeConfig getNodeConfig() {
        return kafkaNodeConfig;
    }

    public CacheClusterConfig getCacheClusterConfig() {
        return cacheClusterConfig;
    }

    /**
     * get Topic by uid
     *
     * @param  uid uid
     * @return     topic
     */
    public String getTopic(String uid) {
        KafkaIdConfig idConfig = this.idConfigMap.get(uid);
        return Objects.isNull(idConfig) ? null : idConfig.getTopic();
    }

    /**
     * get KafkaIdConfig by uid
     *
     * @param  uid uid
     * @return     KafkaIdConfig
     */
    public KafkaIdConfig getIdConfig(String uid) {
        KafkaIdConfig idConfig = this.idConfigMap.get(uid);
        if (idConfig == null) {
            throw new NullPointerException("uid " + uid + "got null KafkaIdConfig");
        }
        return idConfig;
    }

    /**
     * addSendMetric
     *
     * @param currentRecord
     * @param topic
     */
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
     * create IEvent2ProducerRecordHandler
     *
     * @return IEvent2ProducerRecordHandler
     */
    public IEvent2KafkaRecordHandler createEventHandler() {
        // IEvent2ProducerRecordHandler
        String strHandlerClass = CommonPropertiesHolder.getString(KEY_EVENT_HANDLER,
                DefaultEvent2KafkaRecordHandler.class.getName());
        try {
            Class<?> handlerClass = ClassUtils.getClass(strHandlerClass);
            Object handlerObject = handlerClass.getDeclaredConstructor().newInstance();
            if (handlerObject instanceof IEvent2KafkaRecordHandler) {
                IEvent2KafkaRecordHandler handler = (IEvent2KafkaRecordHandler) handlerObject;
                return handler;
            }
        } catch (Throwable t) {
            LOG.error("Fail to init IEvent2KafkaRecordHandler,handlerClass:{},error:{}",
                    strHandlerClass, t.getMessage(), t);
        }
        return null;
    }
}
