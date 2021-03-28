/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.tubemq.server.master.metamanage.metastore;

import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.tubemq.server.Server;
import org.apache.tubemq.server.common.utils.ProcessResult;
import org.apache.tubemq.server.master.metamanage.keepalive.KeepAlive;
import org.apache.tubemq.server.master.metamanage.metastore.dao.entity.BrokerConfEntity;
import org.apache.tubemq.server.master.metamanage.metastore.dao.entity.ClusterSettingEntity;
import org.apache.tubemq.server.master.metamanage.metastore.dao.entity.GroupBaseCtrlEntity;
import org.apache.tubemq.server.master.metamanage.metastore.dao.entity.GroupBlackListEntity;
import org.apache.tubemq.server.master.metamanage.metastore.dao.entity.GroupConsumeCtrlEntity;
import org.apache.tubemq.server.master.metamanage.metastore.dao.entity.TopicConfEntity;
import org.apache.tubemq.server.master.metamanage.metastore.dao.entity.TopicCtrlEntity;


public interface MetaStoreService extends KeepAlive, Server {

    boolean checkStoreStatus(boolean checkIsMaster, ProcessResult result);

    // cluster default configure api
    boolean addClusterConfig(ClusterSettingEntity memEntity, ProcessResult result);

    boolean updClusterConfig(ClusterSettingEntity memEntity, ProcessResult result);

    ClusterSettingEntity getClusterConfig();

    boolean delClusterConfig(ProcessResult result);

    // broker configure api
    boolean addBrokerConf(BrokerConfEntity memEntity, ProcessResult result);

    boolean updBrokerConf(BrokerConfEntity memEntity, ProcessResult result);

    boolean delBrokerConf(int brokerId, ProcessResult result);

    Map<Integer, BrokerConfEntity> getBrokerConfInfo(BrokerConfEntity qryEntity);

    BrokerConfEntity getBrokerConfByBrokerId(int brokerId);

    BrokerConfEntity getBrokerConfByBrokerIp(String brokerIp);

    // topic configure api
    boolean addTopicConf(TopicConfEntity entity, ProcessResult result);

    boolean updTopicConf(TopicConfEntity entity, ProcessResult result);

    boolean delTopicConf(String recordKey, ProcessResult result);

    boolean hasConfiguredTopics(int brokerId);

    TopicConfEntity getTopicConfByeRecKey(String recordKey);

    List<TopicConfEntity> getTopicConf(TopicConfEntity qryEntity);

    Set<String> getConfiguredTopicSet();

    Map<Integer/* brokerId */, Set<String>> getConfiguredTopicInfo(Set<Integer> brokerIdSet);

    Map<String/* topicName */, Map<Integer, String>> getTopicBrokerInfo(Set<String> topicNameSet);

    Map<String/* topicName */, List<TopicConfEntity>> getTopicConfMap(TopicConfEntity qryEntity);

    Map<String, TopicConfEntity> getConfiguredTopicInfo(int brokerId);

    // topic control api
    boolean addTopicCtrlConf(TopicCtrlEntity entity, ProcessResult result);

    boolean updTopicCtrlConf(TopicCtrlEntity entity, ProcessResult result);

    boolean delTopicCtrlConf(String topicName, ProcessResult result);

    TopicCtrlEntity getTopicCtrlConf(String topicName);

    List<TopicCtrlEntity> getTopicCtrlConf(TopicCtrlEntity qryEntity);

    // group configure api
    boolean addGroupBaseCtrlConf(GroupBaseCtrlEntity entity, ProcessResult result);

    boolean updGroupBaseCtrlConf(GroupBaseCtrlEntity entity, ProcessResult result);

    boolean delGroupBaseCtrlConf(String groupName, ProcessResult result);

    GroupBaseCtrlEntity getGroupBaseCtrlConf(String groupName);

    Map<String, GroupBaseCtrlEntity> getGroupBaseCtrlConf(GroupBaseCtrlEntity qryEntity);

    // group blacklist api
    boolean addGroupBlackListConf(GroupBlackListEntity entity, ProcessResult result);

    boolean updGroupBlackListConf(GroupBlackListEntity entity, ProcessResult result);

    boolean delGroupBlackListConf(String groupName,
                                  String topicName,
                                  ProcessResult result);

    boolean delGroupBlackListConf(String recordKey, ProcessResult result);

    List<GroupBlackListEntity> getGrpBlkLstConfByGroupName(String groupName);

    List<GroupBlackListEntity> getGrpBlkLstConfByTopicName(String topicName);

    Set<String> getGrpBlkLstNATopicByGroupName(String groupName);

    List<GroupBlackListEntity> getGroupBlackListConf(GroupBlackListEntity qryEntity);

    // group consume control api
    boolean addGroupConsumeCtrlConf(GroupConsumeCtrlEntity entity, ProcessResult result);

    boolean updGroupConsumeCtrlConf(GroupConsumeCtrlEntity entity, ProcessResult result);

    boolean delGroupConsumeCtrlConf(String groupName, String topicName, ProcessResult result);

    boolean delGroupConsumeCtrlConf(String recordKey, ProcessResult result);

    GroupConsumeCtrlEntity getGroupConsumeCtrlConfByRecKey(String recordKey);

    GroupConsumeCtrlEntity getConsumeCtrlByGroupAndTopic(String groupName, String topicName);

    List<GroupConsumeCtrlEntity> getConsumeCtrlByTopicName(String topicName);

    List<GroupConsumeCtrlEntity> getGroupConsumeCtrlConf(GroupConsumeCtrlEntity qryEntity);
}
