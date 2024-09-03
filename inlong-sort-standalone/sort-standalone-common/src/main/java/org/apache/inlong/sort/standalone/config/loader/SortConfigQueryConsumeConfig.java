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

package org.apache.inlong.sort.standalone.config.loader;

import org.apache.inlong.common.pojo.sort.ClusterTagConfig;
import org.apache.inlong.common.pojo.sort.TaskConfig;
import org.apache.inlong.common.pojo.sort.dataflow.DataFlowConfig;
import org.apache.inlong.common.pojo.sort.mq.MqClusterConfig;
import org.apache.inlong.common.pojo.sort.mq.PulsarClusterConfig;
import org.apache.inlong.sdk.sort.api.ClientContext;
import org.apache.inlong.sdk.sort.api.InlongTopicTypeEnum;
import org.apache.inlong.sdk.sort.api.QueryConsumeConfig;
import org.apache.inlong.sdk.sort.entity.CacheZoneCluster;
import org.apache.inlong.sdk.sort.entity.ConsumeConfig;
import org.apache.inlong.sdk.sort.entity.InLongTopic;
import org.apache.inlong.sort.standalone.config.holder.v2.SortConfigHolder;

import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Collectors;

@Slf4j
public class SortConfigQueryConsumeConfig implements QueryConsumeConfig {

    @Override
    public ConsumeConfig queryCurrentConsumeConfig(String sortTaskId) {
        TaskConfig taskConfig = SortConfigHolder.getTaskConfig(sortTaskId);
        List<InLongTopic> topics = taskConfig.getClusterTagConfigs()
                .stream()
                .map(this::parseTopics)
                .flatMap(Collection::stream)
                .collect(Collectors.toList());

        return new ConsumeConfig(topics);
    }

    public List<InLongTopic> parseTopics(ClusterTagConfig clusterConfig) {
        List<InLongTopic> topics = new ArrayList<>();
        List<MqClusterConfig> mqClusterConfigs = clusterConfig.getMqClusterConfigs();
        List<DataFlowConfig> dataFlowConfigs = clusterConfig.getDataFlowConfigs();
        for (MqClusterConfig mq : mqClusterConfigs) {
            for (DataFlowConfig flow : dataFlowConfigs) {
                InLongTopic topic = new InLongTopic();
                topic.setInLongCluster(this.parseCacheZone(mq));
                topic.setTopic(flow.getSourceConfig().getTopic());
                // only supports pulsar now
                topic.setTopicType(InlongTopicTypeEnum.PULSAR.getName());
                topic.setStartConsumeTime(flow.getSourceConfig().getStartConsumeTime());
                topic.setStopConsumeTime(flow.getSourceConfig().getStopConsumeTime());
                topic.setProperties(flow.getProperties() != null ? flow.getProperties() : new HashMap<>());
                topics.add(topic);
            }
        }
        return topics;
    }

    public CacheZoneCluster parseCacheZone(MqClusterConfig mqClusterConfig) {
        PulsarClusterConfig pulsarClusterConfig = (PulsarClusterConfig) mqClusterConfig;
        return new CacheZoneCluster(pulsarClusterConfig.getClusterName(),
                pulsarClusterConfig.getServiceUrl(), pulsarClusterConfig.getToken());
    }

    @Override
    public void configure(ClientContext context) {

    }
}
