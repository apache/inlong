/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * <p>http://www.apache.org/licenses/LICENSE-2.0
 *
 * <p>Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.inlong.sort.standalone.sink.kafka;

import org.apache.flume.Channel;
import org.apache.flume.Context;
import org.apache.inlong.sort.standalone.config.holder.SortClusterConfigHolder;
import org.apache.inlong.sort.standalone.config.pojo.CacheClusterConfig;
import org.apache.inlong.sort.standalone.config.pojo.InlongId;
import org.apache.inlong.sort.standalone.config.pojo.SortTaskConfig;
import org.apache.inlong.sort.standalone.sink.SinkContext;
import org.apache.inlong.sort.standalone.utils.Constants;
import org.apache.inlong.sort.standalone.utils.InlongLoggerFactory;
import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/** Context of kafka sink. */
public class KafkaFederationSinkContext extends SinkContext {
    public static final Logger LOG =
            InlongLoggerFactory.getLogger(KafkaFederationSinkContext.class);

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
     * @param uid uid
     * @return topic
     */
    public String getTopic(String uid) {
        String topic = this.idTopicMap.get(uid);
        if (topic == null) {
            throw new NullPointerException("uid " + uid + "got null topic");
        }
        return topic;
    }
}
