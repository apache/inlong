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

import org.apache.inlong.common.pojo.sort.SortClusterConfig;
import org.apache.inlong.common.pojo.sort.SortTaskConfig;
import org.apache.inlong.common.pojo.sort.node.ClsNodeConfig;
import org.apache.inlong.sort.standalone.channel.ProfileEvent;
import org.apache.inlong.sort.standalone.config.holder.CommonPropertiesHolder;
import org.apache.inlong.sort.standalone.config.holder.v2.SortConfigHolder;
import org.apache.inlong.sort.standalone.config.pojo.InlongId;
import org.apache.inlong.sort.standalone.metrics.SortMetricItem;
import org.apache.inlong.sort.standalone.metrics.audit.AuditUtils;
import org.apache.inlong.sort.standalone.sink.v2.SinkContext;
import org.apache.inlong.sort.standalone.utils.InlongLoggerFactory;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.tencentcloudapi.cls.producer.AsyncProducerClient;
import com.tencentcloudapi.cls.producer.AsyncProducerConfig;
import com.tencentcloudapi.cls.producer.errors.ProducerException;
import com.tencentcloudapi.cls.producer.util.NetworkUtils;
import org.apache.commons.lang3.ClassUtils;
import org.apache.flume.Channel;
import org.apache.flume.Context;
import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

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

            SortTaskConfig newSortTaskConfig = SortConfigHolder.getTaskConfig(taskName);
            if (newSortTaskConfig == null || newSortTaskConfig.equals(sortTaskConfig)) {
                return;
            }
            LOG.info("get new SortTaskConfig:taskName:{}:config:{}", taskName,
                    new ObjectMapper().writeValueAsString(newSortTaskConfig));
            this.sortTaskConfig = newSortTaskConfig;
            ClsNodeConfig requestNodeConfig = (ClsNodeConfig) sortTaskConfig.getNodeConfig();
            this.clsNodeConfig = requestNodeConfig.getVersion() >= clsNodeConfig.getVersion() ? requestNodeConfig : clsNodeConfig;
            this.keywordMaxLength = DEFAULT_KEYWORD_MAX_LENGTH;
            this.reloadIdParams();
            this.reloadClients();
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

    private void reloadIdParams() {
        this.idConfigMap = this.sortTaskConfig.getClusters()
                .stream()
                .map(SortClusterConfig::getDataFlowConfigs)
                .flatMap(Collection::stream)
                .map(dataFlowConfig -> ClsIdConfig.create(dataFlowConfig, clsNodeConfig))
                .collect(Collectors.toMap(
                        config -> InlongId.generateUid(config.getInlongGroupId(), config.getInlongStreamId()),
                        v -> v));
    }

    private void reloadClients() {
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
}
