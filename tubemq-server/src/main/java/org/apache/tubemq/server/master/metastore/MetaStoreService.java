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

package org.apache.tubemq.server.master.metastore;

import java.util.List;
import java.util.Map;
import org.apache.tubemq.server.Server;
import org.apache.tubemq.server.common.utils.ProcessResult;
import org.apache.tubemq.server.master.metastore.dao.entity.BrokerConfEntity;
import org.apache.tubemq.server.master.metastore.dao.entity.ClusterSettingEntity;
import org.apache.tubemq.server.master.metastore.dao.entity.GroupBlackListEntity;
import org.apache.tubemq.server.master.metastore.dao.entity.GroupConfigEntity;
import org.apache.tubemq.server.master.metastore.dao.entity.GroupFilterCtrlEntity;
import org.apache.tubemq.server.master.metastore.dao.entity.TopicConfEntity;
import org.apache.tubemq.server.master.metastore.dao.entity.TopicCtrlEntity;
import org.apache.tubemq.server.master.metastore.keepalive.KeepAlive;



public interface MetaStoreService extends KeepAlive, Server {

    // cluster default configure api
    boolean addClusterConfig(ClusterSettingEntity memEntity, ProcessResult result);

    boolean updClusterConfig(ClusterSettingEntity memEntity, ProcessResult result);

    ClusterSettingEntity getClusterConfig();

    boolean delClusterConfig(ProcessResult result);

    // broker configure api
    boolean addBrokerConf(BrokerConfEntity memEntity, ProcessResult result);

    boolean updBrokerConf(BrokerConfEntity memEntity, ProcessResult result);

    boolean delBrokerConf(int brokerId, ProcessResult result);

    Map<Integer, BrokerConfEntity> getBrokerConfByBrokerId(BrokerConfEntity qryEntity);

    BrokerConfEntity getBrokerConfByBrokerId(int brokerId);

    // topic configure api
    boolean addTopicConf(TopicConfEntity entity, ProcessResult result);

    boolean updTopicConf(TopicConfEntity entity, ProcessResult result);

    boolean delTopicConf(String recordKey, ProcessResult result);

    List<TopicConfEntity> getTopicConf(TopicConfEntity qryEntity);

    // topic control api
    boolean addTopicCtrlConf(TopicCtrlEntity entity, ProcessResult result);

    boolean updTopicCtrlConf(TopicCtrlEntity entity, ProcessResult result);

    boolean delTopicCtrlConf(String recordKey, ProcessResult result);

    TopicCtrlEntity getTopicCtrlConf(String topicName);

    List<TopicCtrlEntity> getTopicCtrlConf(TopicConfEntity qryEntity);

    // group configure api
    boolean addGroupConf(GroupConfigEntity entity, ProcessResult result);

    boolean updGroupConf(GroupConfigEntity entity, ProcessResult result);

    boolean delGroupConf(String groupName, ProcessResult result);

    GroupConfigEntity getGroupConf(String groupName);

    Map<String, GroupConfigEntity> getGroupConf(GroupConfigEntity qryEntity);

    // group blacklist api
    boolean addGroupBlackListConf(GroupBlackListEntity entity, ProcessResult result);

    boolean updGroupBlackListConf(GroupBlackListEntity entity, ProcessResult result);

    boolean delGroupBlackListConf(String recordKey, ProcessResult result);

    boolean delGroupBlackListConfByGroupName(String groupName, ProcessResult result);

    List<GroupBlackListEntity> getGrpBlkLstConfByGroupName(String groupName);

    List<GroupBlackListEntity> getGrpBlkLstConfByTopicName(String topicName);

    List<GroupBlackListEntity> getGroupBlackListConf(GroupBlackListEntity qryEntity);

    // group filter control api
    boolean addGroupFilterCtrlConf(GroupFilterCtrlEntity entity, ProcessResult result);

    boolean updGroupFilterCtrlConf(GroupFilterCtrlEntity entity, ProcessResult result);

    boolean delGroupFilterCtrlConf(String recordKey, ProcessResult result);

    List<GroupFilterCtrlEntity> getGroupFilterCtrlConf(String groupName);

    List<GroupFilterCtrlEntity> getGroupFilterCtrlConf(GroupFilterCtrlEntity qryEntity);
}
