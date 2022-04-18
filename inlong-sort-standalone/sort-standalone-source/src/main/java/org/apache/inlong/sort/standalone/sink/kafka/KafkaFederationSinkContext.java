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

package org.apache.inlong.sort.standalone.sink.kafka;

import org.apache.commons.lang.math.NumberUtils;
import org.apache.flume.Channel;
import org.apache.flume.Context;
import org.apache.inlong.common.pojo.sortstandalone.SortTaskConfig;
import org.apache.inlong.sort.standalone.channel.ProfileEvent;
import org.apache.inlong.sort.standalone.config.holder.CommonPropertiesHolder;
import org.apache.inlong.sort.standalone.config.holder.SortClusterConfigHolder;
import org.apache.inlong.sort.standalone.config.pojo.CacheClusterConfig;
import org.apache.inlong.sort.standalone.config.pojo.InlongId;
import org.apache.inlong.sort.standalone.metrics.SortMetricItem;
import org.apache.inlong.sort.standalone.metrics.audit.AuditUtils;
import org.apache.inlong.sort.standalone.sink.SinkContext;
import org.apache.inlong.sort.standalone.utils.Constants;
import org.apache.inlong.sort.standalone.utils.InlongLoggerFactory;
import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/** Context of kafka sink. */
public class KafkaFederationSinkContext extends SinkContext {

    public static final Logger LOG = InlongLoggerFactory.getLogger(KafkaFederationSinkContext.class);

    private Context producerContext;
    private Map<String, String> idTopicMap = new ConcurrentHashMap<>();
    private List<CacheClusterConfig> clusterConfigList = new ArrayList<>();

    public KafkaFederationSinkContext(String sinkName, Context context, Channel channel) {
        super(sinkName, context, channel);
    }

    /** reload context */
    @Override
    public void reload() {
        LOG.info("reload KafkaFederationSinkContext.");
        try {
            SortTaskConfig newSortTaskConfig = SortClusterConfigHolder.getTaskConfig(taskName);
            if (newSortTaskConfig == null) {
                LOG.error("newSortTaskConfig is null.");
                return;
            }
            if (this.sortTaskConfig != null && this.sortTaskConfig.equals(newSortTaskConfig)) {
                LOG.info("Same sortTaskConfig, do nothing.");
                return;
            }
            this.sortTaskConfig = newSortTaskConfig;
            this.producerContext = new Context(this.sortTaskConfig.getSinkParams());

            LOG.info("reload idTopicMap");
            Map<String, String> newIdTopicMap = new ConcurrentHashMap<>();
            List<Map<String, String>> idList = this.sortTaskConfig.getIdParams();
            for (Map<String, String> idParam : idList) {
                String inlongGroupId = idParam.get(Constants.INLONG_GROUP_ID);
                String inlongStreamId = idParam.get(Constants.INLONG_STREAM_ID);
                String uid = InlongId.generateUid(inlongGroupId, inlongStreamId);
                String topic = idParam.getOrDefault(Constants.TOPIC, uid);
                newIdTopicMap.put(uid, topic);
            }

            LOG.info("reload clusterConfig");
            CacheClusterConfig clusterConfig = new CacheClusterConfig();
            clusterConfig.setClusterName(this.taskName);
            clusterConfig.setParams(this.sortTaskConfig.getSinkParams());
            List<CacheClusterConfig> newClusterConfigList = new ArrayList<>();
            newClusterConfigList.add(clusterConfig);
            this.idTopicMap = newIdTopicMap;
            this.clusterConfigList = newClusterConfigList;
        } catch (Throwable e) {
            LOG.error(e.getMessage(), e);
        }
    }

    /**
     * get ProducerContext
     *
     * @return ProducerContext
     */
    public Context getProducerContext() {
        return producerContext;
    }

    /**
     * get ClusterConfigList
     *
     * @return ClusterConfigList
     */
    public List<CacheClusterConfig> getClusterConfigList() {
        return clusterConfigList;
    }

    /**
     * get Topic by uid
     *
     * @param  uid uid
     * @return     topic
     */
    public String getTopic(String uid) {
        String topic = this.idTopicMap.get(uid);
        if (topic == null) {
            throw new NullPointerException("uid " + uid + "got null topic");
        }
        return topic;
    }

    /**
     * addSendMetric
     * 
     * @param currentRecord
     * @param bid
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
                long nodeDuration = currentTime - NumberUtils.toLong(Constants.HEADER_KEY_SOURCE_TIME, msgTime);
                long wholeDuration = currentTime - msgTime;
                metricItem.sinkDuration.addAndGet(sinkDuration * count);
                metricItem.nodeDuration.addAndGet(nodeDuration * count);
                metricItem.wholeDuration.addAndGet(wholeDuration * count);
            }
        } else {
            metricItem.sendFailCount.addAndGet(count);
            metricItem.sendFailSize.addAndGet(size);
        }
    }
}
