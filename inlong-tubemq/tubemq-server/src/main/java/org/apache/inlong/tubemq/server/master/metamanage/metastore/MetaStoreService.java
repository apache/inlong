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

package org.apache.inlong.tubemq.server.master.metamanage.metastore;

import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.inlong.tubemq.corebase.rv.ProcessResult;
import org.apache.inlong.tubemq.server.Server;
import org.apache.inlong.tubemq.server.master.metamanage.keepalive.AliveObserver;
import org.apache.inlong.tubemq.server.master.metamanage.keepalive.KeepAlive;
import org.apache.inlong.tubemq.server.master.metamanage.metastore.dao.entity.BrokerConfEntity;
import org.apache.inlong.tubemq.server.master.metamanage.metastore.dao.entity.ClusterSettingEntity;
import org.apache.inlong.tubemq.server.master.metamanage.metastore.dao.entity.GroupConsumeCtrlEntity;
import org.apache.inlong.tubemq.server.master.metamanage.metastore.dao.entity.GroupResCtrlEntity;
import org.apache.inlong.tubemq.server.master.metamanage.metastore.dao.entity.TopicCtrlEntity;
import org.apache.inlong.tubemq.server.master.metamanage.metastore.dao.entity.TopicDeployEntity;

public interface MetaStoreService extends KeepAlive, Server {

    boolean checkStoreStatus(boolean checkIsMaster, ProcessResult result);

    // cluster default configure api

    /**
     * Add or update cluster default setting
     *
     * @param entity     the cluster default setting entity will be add
     * @param strBuff  the print info string buffer
     * @param result     the process result return
     * @return true if success otherwise false
     */
    boolean addClusterConfig(ClusterSettingEntity entity,
                             StringBuilder strBuff, ProcessResult result);

    /**
     * Update cluster default setting
     *
     * @param entity     the cluster default setting entity will be add
     * @param strBuff  the print info string buffer
     * @param result     the process result return
     * @return true if success otherwise false
     */
    boolean updClusterConfig(ClusterSettingEntity entity,
                             StringBuilder strBuff, ProcessResult result);

    ClusterSettingEntity getClusterConfig();

    /**
     * Delete cluster default setting
     *
     * @param operator   operator
     * @param strBuff  the print info string buffer
     * @param result     the process result return
     * @return true if success
     */
    boolean delClusterConfig(String operator,
                             StringBuilder strBuff, ProcessResult result);

    // broker configure api
    /**
     * Add broker configure information
     *
     * @param entity     the broker configure entity will be add
     * @param strBuff  the print information string buffer
     * @param result     the process result return
     * @return true if success otherwise false
     */
    boolean addBrokerConf(BrokerConfEntity entity,
                          StringBuilder strBuff, ProcessResult result);

    /**
     * Modify broker configure information
     *
     * @param entity     the broker configure entity will be update
     * @param strBuff  the print information string buffer
     * @param result     the process result return
     * @return true if success otherwise false
     */
    boolean updBrokerConf(BrokerConfEntity entity,
                          StringBuilder strBuff, ProcessResult result);

    /**
     * Delete broker configure information
     *
     * @param operator  operator
     * @param brokerId  need deleted broker id
     * @param strBuff  the print information string buffer
     * @param result     the process result return
     * @return true if success otherwise false
     */
    boolean delBrokerConf(String operator, int brokerId,
                          StringBuilder strBuff, ProcessResult result);

    Map<Integer, BrokerConfEntity> getBrokerConfInfo(BrokerConfEntity qryEntity);

    Map<Integer, BrokerConfEntity> getBrokerConfInfo(Set<Integer> brokerIdSet,
                                                     Set<String> brokerIpSet,
                                                     BrokerConfEntity qryEntity);

    BrokerConfEntity getBrokerConfByBrokerId(int brokerId);

    BrokerConfEntity getBrokerConfByBrokerIp(String brokerIp);

    // topic configure api
    boolean addTopicConf(TopicDeployEntity entity,
                         StringBuilder strBuff, ProcessResult result);

    boolean updTopicConf(TopicDeployEntity entity,
                         StringBuilder strBuff, ProcessResult result);

    boolean delTopicConf(String operator, String recordKey,
                         StringBuilder strBuff, ProcessResult result);

    boolean delTopicConfByBrokerId(String operator, int brokerId,
                                   StringBuilder strBuff, ProcessResult result);

    boolean hasConfiguredTopics(int brokerId);

    boolean isTopicDeployed(String topicName);

    TopicDeployEntity getTopicConfByeRecKey(String recordKey);

    List<TopicDeployEntity> getTopicConf(TopicDeployEntity qryEntity);

    TopicDeployEntity getTopicConf(int brokerId, String topicName);

    Set<String> getConfiguredTopicSet();

    Map<String, TopicDeployEntity> getConfiguredTopicInfo(int brokerId);

    Map<Integer/* brokerId */, Set<String>> getConfiguredTopicInfo(Set<Integer> brokerIdSet);

    Map<String/* topicName */, Map<Integer, String>> getTopicBrokerInfo(Set<String> topicNameSet);

    Map<String/* topicName */, List<TopicDeployEntity>> getTopicConfMap(
            Set<String> topicNameSet, Set<Integer> brokerIdSet, TopicDeployEntity qryEntity);

    Map<Integer/* brokerId */, List<TopicDeployEntity>> getTopicDeployInfoMap(
            Set<String> topicNameSet, Set<Integer> brokerIdSet);

    Map<String/* topicName */, List<TopicDeployEntity>> getTopicDepInfoByTopicBrokerId(
            Set<String> topicSet, Set<Integer> brokerIdSet);

    // topic control api
    /**
     * Add topic control configure info
     *
     * @param entity     the topic control info entity will be add
     * @param strBuff    the print info string buffer
     * @param result     the process result return
     * @return true if success otherwise false
     */
    boolean addTopicCtrlConf(TopicCtrlEntity entity,
                             StringBuilder strBuff, ProcessResult result);

    /**
     * Update topic control configure
     *
     * @param entity     the topic control info entity will be update
     * @param strBuff    the print info string buffer
     * @param result     the process result return
     * @return true if success otherwise false
     */
    boolean updTopicCtrlConf(TopicCtrlEntity entity,
                             StringBuilder strBuff, ProcessResult result);

    /**
     * Delete topic control configure
     *
     * @param operator   operator
     * @param topicName  the topicName will be deleted
     * @param strBuff  the print info string buffer
     * @param result     the process result return
     * @return true if success otherwise false
     */
    boolean delTopicCtrlConf(String operator, String topicName,
                             StringBuilder strBuff, ProcessResult result);

    TopicCtrlEntity getTopicCtrlConf(String topicName);

    List<TopicCtrlEntity> getTopicCtrlConf(TopicCtrlEntity qryEntity);

    Map<String, TopicCtrlEntity> getTopicCtrlConf(Set<String> topicNameSet,
                                                  TopicCtrlEntity qryEntity);

    // group resource configure api
    /**
     * Add group resource control configure info
     *
     * @param entity     the group resource control info entity will be add
     * @param strBuff  the print info string buffer
     * @param result     the process result return
     * @return true if success otherwise false
     */
    boolean addGroupResCtrlConf(GroupResCtrlEntity entity,
                                StringBuilder strBuff, ProcessResult result);

    /**
     * Update group reource control configure
     *
     * @param entity     the group resource control info entity will be update
     * @param strBuff  the print info string buffer
     * @param result     the process result return
     * @return true if success otherwise false
     */
    boolean updGroupResCtrlConf(GroupResCtrlEntity entity,
                                StringBuilder strBuff, ProcessResult result);

    /**
     * Delete group resource control configure
     *
     * @param operator   operator
     * @param groupName the group will be deleted
     * @param strBuff  the print info string buffer
     * @param result     the process result return
     * @return true if success otherwise false
     */
    boolean delGroupResCtrlConf(String operator, String groupName,
                                StringBuilder strBuff, ProcessResult result);

    GroupResCtrlEntity getGroupResCtrlConf(String groupName);

    Map<String, GroupResCtrlEntity> getGroupResCtrlConf(Set<String> groupSet,
                                                        GroupResCtrlEntity qryEntity);

    // group consume control api
    /**
     * Add group consume control configure
     *
     * @param entity     the group consume control info entity will be add
     * @param strBuff  the print info string buffer
     * @param result     the process result return
     * @return true if success otherwise false
     */
    boolean addGroupConsumeCtrlConf(GroupConsumeCtrlEntity entity,
                                    StringBuilder strBuff, ProcessResult result);

    /**
     * Modify group consume control configure
     *
     * @param entity     the group consume control info entity will be update
     * @param strBuff  the print info string buffer
     * @param result     the process result return
     * @return true if success otherwise false
     */
    boolean updGroupConsumeCtrlConf(GroupConsumeCtrlEntity entity,
                                    StringBuilder strBuff, ProcessResult result);

    /**
     * Delete group consume control configure
     *
     * @param operator  operator
     * @param groupName the blacklist record related to group
     * @param topicName the blacklist record related to topic
     *                  allow groupName or topicName is null,
     *                  but not all null
     * @param strBuff   the string buffer
     * @param result    the process result
     * @return true if success
     */
    boolean delGroupConsumeCtrlConf(String operator,
                                    String groupName, String topicName,
                                    StringBuilder strBuff, ProcessResult result);

    /**
     * Delete group consume control configure
     *
     * @param operator  operator
     * @param recordKey the record key to group consume control record
     * @param strBuff  the print info string buffer
     * @param result     the process result return
     * @return true if success
     */
    boolean delGroupConsumeCtrlConf(String operator, String recordKey,
                                    StringBuilder strBuff, ProcessResult result);

    void registerObserver(AliveObserver eventObserver);

    boolean isTopicNameInUsed(String topicName);

    boolean hasGroupConsumeCtrlConf(String groupName);

    GroupConsumeCtrlEntity getGroupConsumeCtrlConfByRecKey(String recordKey);

    GroupConsumeCtrlEntity getConsumeCtrlByGroupAndTopic(String groupName, String topicName);

    Map<String/* group */, List<GroupConsumeCtrlEntity>> getConsumeCtrlInfoMap(
            Set<String> groupSet, Set<String> topicSet, GroupConsumeCtrlEntity qryEntry);

    List<GroupConsumeCtrlEntity> getConsumeCtrlByTopicName(String topicName);

    List<GroupConsumeCtrlEntity> getConsumeCtrlByGroupName(String groupName);

    List<GroupConsumeCtrlEntity> getGroupConsumeCtrlConf(GroupConsumeCtrlEntity qryEntity);

    Set<String> getMatchedKeysByGroupAndTopicSet(Set<String> groupSet, Set<String> topicSet);
}
