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

package org.apache.inlong.tubemq.server.master.metamanage.metastore.dao.mapper;

import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.inlong.tubemq.server.common.utils.ProcessResult;
import org.apache.inlong.tubemq.server.master.metamanage.metastore.dao.entity.TopicDeployEntity;



public interface TopicDeployMapper extends AbstractMapper {

    boolean addTopicConf(TopicDeployEntity entity, ProcessResult result);

    boolean updTopicConf(TopicDeployEntity entity, ProcessResult result);

    boolean delTopicConf(String recordKey, ProcessResult result);

    boolean delTopicConfByBrokerId(Integer brokerId, ProcessResult result);


    boolean hasConfiguredTopics(int brokerId);

    boolean isTopicDeployed(String topicName);

    List<TopicDeployEntity> getTopicConf(TopicDeployEntity qryEntity);

    TopicDeployEntity getTopicConf(int brokerId, String topicName);

    TopicDeployEntity getTopicConfByeRecKey(String recordKey);

    /**
     * Get broker topic entity, if query entity is null, return all topic entity
     *
     * @param topicNameSet need query topic name set
     * @param brokerIdSet  need query broker id set
     * @param qryEntity   must not null
     * @return  topic deploy info by topicName's key
     */
    Map<String, List<TopicDeployEntity>> getTopicConfMap(Set<String> topicNameSet,
                                                         Set<Integer> brokerIdSet,
                                                         TopicDeployEntity qryEntity);

    Map<Integer, List<TopicDeployEntity>> getTopicDeployInfoMap(Set<Integer> brokerIdSet,
                                                                Set<String> topicNameSet);

    Map<String/* topicName */, List<TopicDeployEntity>> getTopicConfMapByTopicAndBrokerIds(
            Set<String> topicSet, Set<Integer> brokerIdSet);

    Map<String, TopicDeployEntity> getConfiguredTopicInfo(int brokerId);

    Map<Integer/* brokerId */, Set<String>> getConfiguredTopicInfo(Set<Integer> brokerIdSet);

    Map<String/* topicName */, Map<Integer, String>> getTopicBrokerInfo(Set<String> topicNameSet);

    Set<String> getConfiguredTopicSet();

}
