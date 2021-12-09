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

package org.apache.inlong.sort.standalone.sink.pulsar;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.flume.Channel;
import org.apache.flume.Context;
import org.apache.inlong.sort.standalone.config.holder.SortClusterConfigHolder;
import org.apache.inlong.sort.standalone.config.pojo.CacheClusterConfig;
import org.apache.inlong.sort.standalone.config.pojo.InlongId;
import org.apache.inlong.sort.standalone.config.pojo.SortTaskConfig;
import org.apache.inlong.sort.standalone.sink.SinkContext;
import org.apache.inlong.sort.standalone.utils.Constants;
import org.slf4j.Logger;
import org.apache.inlong.sort.standalone.utils.InlongLoggerFactory;

/**
 * 
 * PulsarFederationSinkContext
 */
public class PulsarFederationSinkContext extends SinkContext {

    public static final Logger LOG = InlongLoggerFactory.getLogger(PulsarFederationSinkContext.class);

    private Context producerContext;
    private Map<String, String> idTopicMap = new ConcurrentHashMap<>();
    private List<CacheClusterConfig> clusterConfigList = new ArrayList<>();

    /**
     * Constructor
     * 
     * @param sinkName
     * @param context
     * @param channel
     */
    public PulsarFederationSinkContext(String sinkName, Context context, Channel channel) {
        super(sinkName, context, channel);
    }

    /**
     * reload
     */
    public void reload() {
        super.reload();
        try {
            SortTaskConfig newSortTaskConfig = SortClusterConfigHolder.getTaskConfig(taskName);
            if (this.sortTaskConfig != null && this.sortTaskConfig.equals(newSortTaskConfig)) {
                return;
            }
            this.sortTaskConfig = newSortTaskConfig;
            this.producerContext = new Context(this.sortTaskConfig.getSinkParams());
            //
            Map<String, String> newIdTopicMap = new ConcurrentHashMap<>();
            List<Map<String, String>> idList = this.sortTaskConfig.getIdParams();
            for (Map<String, String> idParam : idList) {
                String inlongGroupId = idParam.get(Constants.INLONG_GROUP_ID);
                String inlongStreamId = idParam.get(Constants.INLONG_STREAM_ID);
                String uid = InlongId.generateUid(inlongGroupId, inlongStreamId);
                String topic = idParam.getOrDefault(Constants.TOPIC, uid);
                newIdTopicMap.put(uid, topic);
            }
            //
            CacheClusterConfig clusterConfig = new CacheClusterConfig();
            clusterConfig.setClusterName(this.taskName);
            clusterConfig.setParams(this.sortTaskConfig.getSinkParams());
            List<CacheClusterConfig> newClusterConfigList = new ArrayList<>();
            newClusterConfigList.add(clusterConfig);
            //
            this.idTopicMap = newIdTopicMap;
            this.clusterConfigList = newClusterConfigList;
        } catch (Throwable e) {
            LOG.error(e.getMessage(), e);
        }
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
     * getTopic
     * 
     * @param  uid
     * @return
     */
    public String getTopic(String uid) {
        return this.idTopicMap.get(uid);
    }

    /**
     * getCacheClusters
     * 
     * @return
     */
    public List<CacheClusterConfig> getCacheClusters() {
        return this.clusterConfigList;
    }
}
