/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
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

import com.alibaba.fastjson.JSON;
import com.tencentcloudapi.cls.producer.AsyncProducerClient;
import com.tencentcloudapi.cls.producer.AsyncProducerConfig;
import com.tencentcloudapi.cls.producer.errors.ProducerException;
import com.tencentcloudapi.cls.producer.util.NetworkUtils;
import org.apache.commons.lang.math.NumberUtils;
import org.apache.flume.Channel;
import org.apache.flume.Context;
import org.apache.inlong.sort.standalone.channel.ProfileEvent;
import org.apache.inlong.sort.standalone.config.holder.CommonPropertiesHolder;
import org.apache.inlong.sort.standalone.config.holder.SortClusterConfigHolder;
import org.apache.inlong.sort.standalone.config.pojo.InlongId;
import org.apache.inlong.sort.standalone.config.pojo.SortTaskConfig;
import org.apache.inlong.sort.standalone.metrics.SortMetricItem;
import org.apache.inlong.sort.standalone.metrics.audit.AuditUtils;
import org.apache.inlong.sort.standalone.sink.SinkContext;
import org.apache.inlong.sort.standalone.utils.Constants;
import org.apache.inlong.sort.standalone.utils.InlongLoggerFactory;
import org.slf4j.Logger;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

/**
 * Cls sink context.
 */
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

    private final Map<String, AsyncProducerClient> clientMap;
    private List<AsyncProducerClient> deletingClients;
    private Context sinkContext;
    private Map<String, ClsIdConfig> idConfigMap = new ConcurrentHashMap<>();
    private AtomicLong offerCounter = new AtomicLong(0);
    private AtomicLong takeCounter = new AtomicLong(0);
    private AtomicLong backCounter = new AtomicLong(0);
    private IEvent2LogItemHandler iEvent2LogItemHandler;

    // default sink params

    /**
     * Constructor
     *
     * @param sinkName Name of sink.
     * @param context Basic context.
     * @param channel Channel which worker acquire profile event from.
     */
    public ClsSinkContext(String sinkName, Context context, Channel channel) {
        super(sinkName, context, channel);
        this.clientMap = new ConcurrentHashMap<>();
    }

    @Override
    public void reload() {
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

        SortTaskConfig newSortTaskConfig = SortClusterConfigHolder.getTaskConfig(taskName);
        if (this.sortTaskConfig != null && this.sortTaskConfig.equals(newSortTaskConfig)) {
            return;
        }
        LOG.info("get new SortTaskConfig:taskName:{}:config:{}", taskName,
                JSON.toJSONString(newSortTaskConfig));
        this.sortTaskConfig = newSortTaskConfig;
        this.sinkContext = new Context(this.sortTaskConfig.getSinkParams());
        this.reloadIdParams();
        this.reloadClients();
        // todo get IEvent2LogItemHandler
    }

    /**
     * Reload id params.
     */
    private void reloadIdParams() {
        List<Map<String, String>> idList = this.sortTaskConfig.getIdParams();
        Map<String, ClsIdConfig> newIdConfigMap = new ConcurrentHashMap<>();
        for (Map<String, String> idParam : idList) {
            String inlongGroupId = idParam.get(Constants.INLONG_GROUP_ID);
            String inlongStreamId = idParam.get(Constants.INLONG_STREAM_ID);
            String uid = InlongId.generateUid(inlongGroupId, inlongStreamId);
            String jsonIdConfig = JSON.toJSONString(idParam);
            ClsIdConfig idConfig = JSON.parseObject(jsonIdConfig, ClsIdConfig.class);
            newIdConfigMap.put(uid, idConfig);
        }
        this.idConfigMap = newIdConfigMap;
    }

    /**
     * Close expire clients and start new clients.
     * 
     * <p>Each client response for data of one secretId.</p>
     * <p>First, find all secretId that are in the active clientMap
     * but not in the updated id config (or to say EXPIRE secretId), and put those clients into deletingClientsMap.
     * The real close process will be done at the beginning of next period of reloading.
     * Second, find all secretIds that in the updated id config
     * but not in the active clientMap(or to say NEW secretId),
     * and start new clients for these secretId and put them into the active clientMap.</p>
     */
    private void reloadClients() {
        // get update secretIds
        Set<String> updateSecretIdSet = idConfigMap
                .values()
                .stream()
                .map(ClsIdConfig::getSecretId)
                .collect(Collectors.toSet());

        // remove expire client
        clientMap.keySet()
                .stream()
                .filter(secretId -> !updateSecretIdSet.contains(secretId))
                .forEach(this::removeExpireClient);

        // start new client
        updateSecretIdSet.stream()
                .filter(secretId -> !clientMap.containsKey(secretId))
                .forEach(this::startNewClient);
    }

    /**
     * Start new cls client and put it to the active clientMap.
     *
     * @param secretId SecretId of new client.
     */
    private void startNewClient(String secretId) {
        ClsIdConfig idConfig = idConfigMap.get(secretId);
        if (idConfig == null) {
            LOG.error("Start client failed, there is not cls config of {}", secretId);
            return;
        }
        AsyncProducerConfig producerConfig = new AsyncProducerConfig(
                idConfig.getEndpoint(),
                idConfig.getSecretId(),
                idConfig.getSecretKey(),
                NetworkUtils.getLocalMachineIP());
        // todo set other configs
        AsyncProducerClient client = new AsyncProducerClient(producerConfig);
        clientMap.put(secretId, client);
    }

    /**
     * Remove expire client from active clientMap and into the deleting client list.
     * <P>The reason why not close client when it remove from clientMap is to avoid <b>Race Condition</b>.
     * Which will happen when worker thread get the client and ready to send msg,
     * while the reload thread try to close it.</P>
     *
     * @param secretId SecretId of expire client.
     */
    private void removeExpireClient(String secretId) {
        AsyncProducerClient client = clientMap.get(secretId);
        if (client == null) {
            LOG.error("Remove client failed, there is not client of {}", secretId);
            return;
        }
        deletingClients.add(clientMap.remove(secretId));
    }

    /**
     * Add send result.
     *
     * @param currentRecord Event to be sent.
     * @param bid Topic or dest ip of event.
     * @param result Result of send.
     * @param sendTime Time of sending.
     */
    public void addSendResultMetric(ProfileEvent currentRecord, String bid, boolean result, long sendTime) {
        Map<String, String> dimensions = this.getDimensions(currentRecord, bid);
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
                long nodeDuration = currentTime
                        - NumberUtils.toLong(Constants.HEADER_KEY_SOURCE_TIME, currentRecord.getRawLogTime());
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

    private Map<String, String> getDimensions (ProfileEvent currentRecord, String bid) {
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


    /**
     * Get {@link ClsIdConfig} by uid.
     * @param uid
     * @return
     */
    public ClsIdConfig getIdConfig(String uid) {
        return idConfigMap.get(uid);
    }



}
