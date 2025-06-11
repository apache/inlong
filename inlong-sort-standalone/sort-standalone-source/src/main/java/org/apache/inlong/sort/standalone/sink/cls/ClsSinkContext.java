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

package org.apache.inlong.sort.standalone.sink.cls;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import com.tencentcloudapi.cls.producer.AsyncProducerClient;
import com.tencentcloudapi.cls.producer.AsyncProducerConfig;
import com.tencentcloudapi.cls.producer.errors.ProducerException;
import com.tencentcloudapi.cls.producer.util.NetworkUtils;

import org.apache.commons.lang3.ClassUtils;
import org.apache.flume.Channel;
import org.apache.flume.Context;
import org.apache.inlong.common.pojo.sort.ClusterTagConfig;
import org.apache.inlong.common.pojo.sort.TaskConfig;
import org.apache.inlong.common.pojo.sort.dataflow.DataFlowConfig;
import org.apache.inlong.common.pojo.sort.dataflow.sink.ClsSinkConfig;
import org.apache.inlong.common.pojo.sort.dataflow.sink.SinkConfig;
import org.apache.inlong.common.pojo.sort.node.ClsNodeConfig;
import org.apache.inlong.common.pojo.sortstandalone.SortTaskConfig;
import org.apache.inlong.sdk.transform.encode.MapSinkEncoder;
import org.apache.inlong.sdk.transform.encode.SinkEncoderFactory;
import org.apache.inlong.sdk.transform.pojo.FieldInfo;
import org.apache.inlong.sdk.transform.pojo.MapSinkInfo;
import org.apache.inlong.sdk.transform.process.TransformProcessor;
import org.apache.inlong.sort.standalone.channel.ProfileEvent;
import org.apache.inlong.sort.standalone.config.holder.CommonPropertiesHolder;
import org.apache.inlong.sort.standalone.config.holder.SortClusterConfigHolder;
import org.apache.inlong.sort.standalone.config.holder.v2.SortConfigHolder;
import org.apache.inlong.sort.standalone.config.pojo.InlongId;
import org.apache.inlong.sort.standalone.metrics.SortConfigMetricReporter;
import org.apache.inlong.sort.standalone.metrics.SortMetricItem;
import org.apache.inlong.sort.standalone.metrics.audit.AuditUtils;
import org.apache.inlong.sort.standalone.sink.SinkContext;
import org.apache.inlong.sort.standalone.utils.Constants;
import org.apache.inlong.sort.standalone.utils.InlongLoggerFactory;
import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import lombok.Getter;

public class ClsSinkContext extends SinkContext {

    private static final Logger LOG = InlongLoggerFactory.getLogger(ClsSinkContext.class);
    // key of sink params
    private static final String KEY_TOTAL_SIZE_IN_BYTES = "totalSizeInBytes";
    private static final String KEY_MAX_SEND_THREAD_COUNT = "maxSendThreadCount";
    private static final String KEY_MAX_BLOCK_SEC = "maxBlockSec";
    private static final String KEY_MAX_BATCH_SIZE = "maxBatchSize";
    private static final String KEY_MAX_BATCH_COUNT = "maxBatchCount";
    private static final String KEY_LINGER_MS = "lingerMs";
    private static final String KEY_RETRIES = "retries";
    private static final String KEY_MAX_RESERVED_ATTEMPTS = "maxReservedAttempts";
    private static final String KEY_BASE_RETRY_BACKOFF_MS = "baseRetryBackoffMs";
    private static final String KEY_MAX_RETRY_BACKOFF_MS = "maxRetryBackoffMs";
    private static final String KEY_MAX_KEYWORD_LENGTH = "maxKeywordLength";
    private static final String KEY_EVENT_LOG_ITEM_HANDLER = "logItemHandler";
    public static final String KEY_TOPIC_ID = "topicId";

    private static final int DEFAULT_KEYWORD_MAX_LENGTH = 32 * 1024 - 1;
    private int keywordMaxLength = DEFAULT_KEYWORD_MAX_LENGTH;

    private final Map<String, AsyncProducerClient> clientMap;
    private List<AsyncProducerClient> deletingClients = new ArrayList<>();
    private Map<String, ClsIdConfig> idConfigMap = new ConcurrentHashMap<>();
    private IEvent2LogItemHandler event2LogItemHandler;
    private ClsNodeConfig clsNodeConfig;
    private ObjectMapper objectMapper;

    // Map<Uid, TransformProcessor<String sourceType, Map<String, Object> sinkType>
    @Getter
    protected Map<String, TransformProcessor<String, Map<String, Object>>> transformMap;

    public ClsSinkContext(String sinkName, Context context, Channel channel) {
        super(sinkName, context, channel);
        this.clientMap = new ConcurrentHashMap<>();
        this.objectMapper = new ObjectMapper();
        this.objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    }

    @Override
    public void reload() {
        try {
            // remove deleting clients.
            deletingClients.forEach(client -> {
                try {
                    client.close();
                } catch (InterruptedException e) {
                    LOG.error("close client failed, got InterruptedException" + e.getMessage(), e);
                } catch (ProducerException e) {
                    LOG.error("close client failed, got ProducerException" + e.getMessage(), e);
                }
            });

            TaskConfig newTaskConfig = SortConfigHolder.getTaskConfig(taskName);
            SortTaskConfig newSortTaskConfig = SortClusterConfigHolder.getTaskConfig(taskName);
            if ((newTaskConfig == null || newTaskConfig.equals(taskConfig))
                    && (newSortTaskConfig == null || newSortTaskConfig.equals(sortTaskConfig))) {
                return;
            }
            LOG.info("get new SortTaskConfig:taskName:{}", taskName);
            if (newTaskConfig != null) {
                ClsNodeConfig requestNodeConfig = (ClsNodeConfig) newTaskConfig.getNodeConfig();
                if (clsNodeConfig == null || requestNodeConfig.getVersion() > clsNodeConfig.getVersion()) {
                    this.clsNodeConfig = requestNodeConfig;
                }
            }

            this.taskConfig = newTaskConfig;
            this.sortTaskConfig = newSortTaskConfig;

            Map<String, ClsIdConfig> fromTaskConfig = reloadIdParamsFromTaskConfig(taskConfig, clsNodeConfig);
            Map<String, ClsIdConfig> fromSortTaskConfig = reloadIdParamsFromSortTaskConfig(sortTaskConfig);
            SortConfigMetricReporter.reportClusterDiff(clusterId, taskName, fromTaskConfig, fromSortTaskConfig);
            Map<String, TransformProcessor<String, Map<String, Object>>> transformProcessor =
                    reloadTransform(taskConfig);
            if (unifiedConfiguration) {
                idConfigMap = fromTaskConfig;
                transformMap = transformProcessor;
            } else {
                idConfigMap = fromSortTaskConfig;
            }
            this.reloadClients(idConfigMap);
            this.reloadHandler();
        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
        }
    }

    private void reloadHandler() {
        String logItemHandlerClass = CommonPropertiesHolder.getString(KEY_EVENT_LOG_ITEM_HANDLER,
                DefaultEvent2LogItemHandler.class.getName());
        try {
            Class<?> handlerClass = ClassUtils.getClass(logItemHandlerClass);
            Object handlerObject = handlerClass.getDeclaredConstructor().newInstance();
            if (handlerObject instanceof IEvent2LogItemHandler) {
                this.event2LogItemHandler = (IEvent2LogItemHandler) handlerObject;
            } else {
                LOG.error("{} is not the instance of IEvent2LogItemHandler", logItemHandlerClass);
            }
        } catch (Throwable t) {
            LOG.error("fail to init IEvent2LogItemHandler, handlerClass:{}, error:{}",
                    logItemHandlerClass, t.getMessage());
        }
    }

    private Map<String, ClsIdConfig> reloadIdParamsFromTaskConfig(TaskConfig taskConfig, ClsNodeConfig clsNodeConfig) {
        if (taskConfig == null) {
            return new HashMap<>();
        }
        return taskConfig.getClusterTagConfigs()
                .stream()
                .map(ClusterTagConfig::getDataFlowConfigs)
                .flatMap(Collection::stream)
                .map(dataFlowConfig -> ClsIdConfig.create(dataFlowConfig, clsNodeConfig))
                .collect(Collectors.toMap(
                        config -> InlongId.generateUid(config.getInlongGroupId(), config.getInlongStreamId()),
                        v -> v));
    }

    private Map<String, ClsIdConfig> reloadIdParamsFromSortTaskConfig(SortTaskConfig sortTaskConfig)
            throws JsonProcessingException {
        if (sortTaskConfig == null) {
            return new HashMap<>();
        }
        List<Map<String, String>> idList = this.sortTaskConfig.getIdParams();
        Map<String, ClsIdConfig> newIdConfigMap = new ConcurrentHashMap<>();
        for (Map<String, String> idParam : idList) {
            String inlongGroupId = idParam.get(Constants.INLONG_GROUP_ID);
            String inlongStreamId = idParam.get(Constants.INLONG_STREAM_ID);
            String uid = InlongId.generateUid(inlongGroupId, inlongStreamId);
            String jsonIdConfig = objectMapper.writeValueAsString(idParam);
            ClsIdConfig idConfig = objectMapper.readValue(jsonIdConfig, ClsIdConfig.class);
            newIdConfigMap.put(uid, idConfig);
        }
        return newIdConfigMap;
    }

    private void reloadClients(Map<String, ClsIdConfig> idConfigMap) {
        // get update secretIds
        Map<String, ClsIdConfig> updateConfigMap = idConfigMap.values()
                .stream()
                .collect(Collectors.toMap(ClsIdConfig::getSecretId, config -> config, (k1, k2) -> k1));

        // remove expire client
        clientMap.keySet()
                .stream()
                .filter(secretId -> !updateConfigMap.containsKey(secretId))
                .forEach(this::removeExpireClient);

        // start new client
        updateConfigMap.values()
                .stream()
                .filter(config -> !clientMap.containsKey(config.getSecretId()))
                .forEach(this::startNewClient);
    }

    private void startNewClient(ClsIdConfig idConfig) {
        AsyncProducerConfig producerConfig = new AsyncProducerConfig(
                idConfig.getEndpoint(),
                idConfig.getSecretId(),
                idConfig.getSecretKey(),
                NetworkUtils.getLocalMachineIP());
        AsyncProducerClient client = new AsyncProducerClient(producerConfig);
        clientMap.put(idConfig.getSecretId(), client);
    }

    private void removeExpireClient(String secretId) {
        AsyncProducerClient client = clientMap.get(secretId);
        if (client == null) {
            LOG.error("remove client failed, there is not client of {}", secretId);
            return;
        }
        deletingClients.add(clientMap.remove(secretId));
    }
    /**
     * addSendMetric
     *
     * @param currentRecord
     * @param topic
     */
    public void addSendMetric(ProfileEvent currentRecord, String topic) {
        Map<String, String> dimensions = this.getDimensions(currentRecord, topic);
        SortMetricItem metricItem = this.getMetricItemSet().findMetricItem(dimensions);
        metricItem.sendCount.incrementAndGet();
        metricItem.sendSize.addAndGet(currentRecord.getBody().length);
    }

    public void addSendResultMetric(ProfileEvent currentRecord, String bid, boolean result, long sendTime) {
        Map<String, String> dimensions = this.getDimensions(currentRecord, bid);
        SortMetricItem metricItem = this.getMetricItemSet().findMetricItem(dimensions);
        if (result) {
            metricItem.sendSuccessCount.incrementAndGet();
            metricItem.sendSuccessSize.addAndGet(currentRecord.getBody().length);
            AuditUtils.add(AuditUtils.AUDIT_ID_SEND_SUCCESS, currentRecord);
            if (sendTime > 0) {
                final long currentTime = System.currentTimeMillis();
                long sinkDuration = currentTime - sendTime;
                long nodeDuration = currentTime - currentRecord.getFetchTime();
                long wholeDuration = currentTime - currentRecord.getRawLogTime();
                metricItem.sinkDuration.addAndGet(sinkDuration);
                metricItem.nodeDuration.addAndGet(nodeDuration);
                metricItem.wholeDuration.addAndGet(wholeDuration);
            }
        } else {
            metricItem.sendFailCount.incrementAndGet();
            metricItem.sendFailSize.addAndGet(currentRecord.getBody().length);
        }
    }

    public void addSendFilterMetric(ProfileEvent currentRecord, String bid) {
        Map<String, String> dimensions = this.getDimensions(currentRecord, bid);
        SortMetricItem metricItem = this.getMetricItemSet().findMetricItem(dimensions);
        metricItem.sendFilterCount.incrementAndGet();
        metricItem.sendFilterSize.addAndGet(currentRecord.getBody().length);
    }

    private Map<String, String> getDimensions(ProfileEvent currentRecord, String bid) {
        Map<String, String> dimensions = new HashMap<>();
        dimensions.put(SortMetricItem.KEY_CLUSTER_ID, this.getClusterId());
        dimensions.put(SortMetricItem.KEY_TASK_NAME, this.getTaskName());
        // metric
        fillInlongId(currentRecord, dimensions);
        dimensions.put(SortMetricItem.KEY_SINK_ID, this.getSinkName());
        dimensions.put(SortMetricItem.KEY_SINK_DATA_ID, bid);
        long msgTime = currentRecord.getRawLogTime();
        long auditFormatTime = msgTime - msgTime % CommonPropertiesHolder.getAuditFormatInterval();
        dimensions.put(SortMetricItem.KEY_MESSAGE_TIME, String.valueOf(auditFormatTime));
        return dimensions;
    }

    public ClsIdConfig getIdConfig(String uid) {
        return idConfigMap.get(uid);
    }

    public int getKeywordMaxLength() {
        return keywordMaxLength;
    }

    public IEvent2LogItemHandler getLogItemHandler() {
        return event2LogItemHandler;
    }

    public AsyncProducerClient getClient(String secretId) {
        return clientMap.get(secretId);
    }

    public TransformProcessor<String, Map<String, Object>> getTransformProcessor(String uid) {
        return this.transformMap.get(uid);
    }

    private Map<String, TransformProcessor<String, Map<String, Object>>> reloadTransform(TaskConfig taskConfig) {
        ImmutableMap.Builder<String, TransformProcessor<String, Map<String, Object>>> builder =
                new ImmutableMap.Builder<>();

        taskConfig.getClusterTagConfigs()
                .stream()
                .map(ClusterTagConfig::getDataFlowConfigs)
                .flatMap(Collection::stream)
                .forEach(flow -> {
                    TransformProcessor<String, Map<String, Object>> transformProcessor =
                            createTransform(flow);
                    if (transformProcessor == null) {
                        return;
                    }
                    builder.put(InlongId.generateUid(flow.getInlongGroupId(), flow.getInlongStreamId()),
                            transformProcessor);
                });

        return builder.build();
    }

    private TransformProcessor<String, Map<String, Object>> createTransform(DataFlowConfig dataFlowConfig) {
        try {
            return TransformProcessor.create(
                    createTransformConfig(dataFlowConfig),
                    createSourceDecoder(dataFlowConfig.getSourceConfig()),
                    createClsSinkEncoder(dataFlowConfig.getSinkConfig()));
        } catch (Exception e) {
            LOG.error("failed to reload transform of dataflow={}, ex={}", dataFlowConfig.getDataflowId(),
                    e.getMessage());
            return null;
        }
    }

    private MapSinkEncoder createClsSinkEncoder(SinkConfig sinkConfig) {
        if (!(sinkConfig instanceof ClsSinkConfig)) {
            throw new IllegalArgumentException("sinkInfo must be an instance of ClsSinkConfig");
        }
        ClsSinkConfig clsSinkConfig = (ClsSinkConfig) sinkConfig;
        List<FieldInfo> fieldInfos = clsSinkConfig.getFieldConfigs()
                .stream()
                .map(config -> new FieldInfo(config.getName(), deriveTypeConverter(config.getFormatInfo())))
                .collect(Collectors.toList());

        MapSinkInfo sinkInfo = new MapSinkInfo(sinkConfig.getEncodingType(), fieldInfos);
        return SinkEncoderFactory.createMapEncoder(sinkInfo);
    }
}
