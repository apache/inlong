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
import org.apache.tubemq.server.master.metamanage.metastore.dao.entity.GroupBlackListEntity;
import org.apache.tubemq.server.master.metamanage.metastore.dao.entity.GroupConsumeCtrlEntity;
import org.apache.tubemq.server.master.metamanage.metastore.dao.entity.GroupResCtrlEntity;
import org.apache.tubemq.server.master.metamanage.metastore.dao.entity.TopicCtrlEntity;
import org.apache.tubemq.server.master.metamanage.metastore.dao.entity.TopicDeployEntity;


public interface MetaStoreService extends KeepAlive, Server {

    boolean checkStoreStatus(boolean checkIsMaster, ProcessResult result);

    // cluster default configure api

    /**
     * Add or update cluster default setting
     *
     * @param entity     the cluster default setting entity will be add
     * @param strBuffer  the print info string buffer
     * @param result     the process result return
     * @return true if success otherwise false
     * @throws Exception
     */
    boolean addClusterConfig(ClusterSettingEntity entity,
                             StringBuilder strBuffer,
                             ProcessResult result);

    /**
     * Update cluster default setting
     *
     * @param entity     the cluster default setting entity will be add
     * @param strBuffer  the print info string buffer
     * @param result     the process result return
     * @return true if success otherwise false
     * @throws Exception
     */
    boolean updClusterConfig(ClusterSettingEntity entity,
                             StringBuilder strBuffer,
                             ProcessResult result);

    ClusterSettingEntity getClusterConfig();

    /**
     * Delete cluster default setting
     *
     * @param operator   operator
     * @param strBuffer  the print info string buffer
     * @param result     the process result return
     * @return true if success
     */
    boolean delClusterConfig(String operator,
                             StringBuilder strBuffer,
                             ProcessResult result);

    // broker configure api
    /**
     * Add broker configure information
     *
     * @param entity     the broker configure entity will be add
     * @param strBuffer  the print information string buffer
     * @param result     the process result return
     * @return true if success otherwise false
     */
    boolean addBrokerConf(BrokerConfEntity entity,
                          StringBuilder strBuffer,
                          ProcessResult result);

    /**
     * Modify broker configure information
     *
     * @param entity     the broker configure entity will be update
     * @param strBuffer  the print information string buffer
     * @param result     the process result return
     * @return true if success otherwise false
     */
    boolean updBrokerConf(BrokerConfEntity entity,
                          StringBuilder strBuffer,
                          ProcessResult result);

    /**
     * Delete broker configure information
     *
     * @param operator  operator
     * @param brokerId  need deleted broker id
     * @param strBuffer  the print information string buffer
     * @param result     the process result return
     * @return true if success otherwise false
     */
    boolean delBrokerConf(String operator,
                          int brokerId,
                          StringBuilder strBuffer,
                          ProcessResult result);

    Map<Integer, BrokerConfEntity> getBrokerConfInfo(BrokerConfEntity qryEntity);

    Map<Integer, BrokerConfEntity> getBrokerConfInfo(Set<Integer> brokerIdSet,
                                                     Set<String> brokerIpSet,
                                                     BrokerConfEntity qryEntity);

    BrokerConfEntity getBrokerConfByBrokerId(int brokerId);

    BrokerConfEntity getBrokerConfByBrokerIp(String brokerIp);

    // topic configure api
    boolean addTopicConf(TopicDeployEntity entity,
                         StringBuilder strBuffer,
                         ProcessResult result);

    boolean updTopicConf(TopicDeployEntity entity,
                         StringBuilder strBuffer,
                         ProcessResult result);

    boolean delTopicConf(String operator,
                         String recordKey,
                         StringBuilder strBuffer,
                         ProcessResult result);

    boolean delTopicConfByBrokerId(String operator,
                                   int brokerId,
                                   StringBuilder strBuffer,
                                   ProcessResult result);

    boolean hasConfiguredTopics(int brokerId);

    TopicDeployEntity getTopicConfByeRecKey(String recordKey);

    List<TopicDeployEntity> getTopicConf(TopicDeployEntity qryEntity);

    TopicDeployEntity getTopicConf(int brokerId, String topicName);

    Set<String> getConfiguredTopicSet();

    Map<Integer/* brokerId */, Set<String>> getConfiguredTopicInfo(Set<Integer> brokerIdSet);

    Map<String/* topicName */, Map<Integer, String>> getTopicBrokerInfo(Set<String> topicNameSet);

    Map<String/* topicName */, List<TopicDeployEntity>> getTopicConfMap(
            Set<String> topicNameSet, Set<Integer> brokerIdSet, TopicDeployEntity qryEntity);

    Map<Integer/* brokerId */, List<TopicDeployEntity>> getTopicDeployInfoMap(
            Set<String> topicNameSet, Set<Integer> brokerIdSet);

    Map<String/* topicName */, List<TopicDeployEntity>> getTopicDepInfoByTopicBrokerId(
            Set<String> topicSet, Set<Integer> brokerIdSet);

    Map<String, TopicDeployEntity> getConfiguredTopicInfo(int brokerId);

    // topic control api
    boolean addTopicCtrlConf(TopicCtrlEntity entity,
                             StringBuilder sBuffer,
                             ProcessResult result);

    boolean updTopicCtrlConf(TopicCtrlEntity entity, ProcessResult result);

    boolean delTopicCtrlConf(String topicName, ProcessResult result);

    TopicCtrlEntity getTopicCtrlConf(String topicName);

    List<TopicCtrlEntity> getTopicCtrlConf(TopicCtrlEntity qryEntity);

    // group resource configure api
    /**
     * Add group resource control configure info
     *
     * @param entity     the group resource control info entity will be add
     * @param strBuffer  the print info string buffer
     * @param result     the process result return
     * @return true if success otherwise false
     */
    boolean addGroupResCtrlConf(GroupResCtrlEntity entity,
                                StringBuilder strBuffer,
                                ProcessResult result);

    /**
     * Update group reource control configure
     *
     * @param entity     the group resource control info entity will be update
     * @param strBuffer  the print info string buffer
     * @param result     the process result return
     * @return true if success otherwise false
     */
    boolean updGroupResCtrlConf(GroupResCtrlEntity entity,
                                StringBuilder strBuffer,
                                ProcessResult result);

    /**
     * Delete group resource control configure
     *
     * @param operator   operator
     * @param groupName the group will be deleted
     * @param strBuffer  the print info string buffer
     * @param result     the process result return
     * @return true if success otherwise false
     */
    boolean delGroupResCtrlConf(String operator,
                                String groupName,
                                StringBuilder strBuffer,
                                ProcessResult result);

    GroupResCtrlEntity getGroupResCtrlConf(String groupName);

    Map<String, GroupResCtrlEntity> getGroupResCtrlConf(GroupResCtrlEntity qryEntity);

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
    /**
     * Add group consume control configure
     *
     * @param entity     the group consume control info entity will be add
     * @param strBuffer  the print info string buffer
     * @param result     the process result return
     * @return true if success otherwise false
     */
    boolean addGroupConsumeCtrlConf(GroupConsumeCtrlEntity entity,
                                    StringBuilder strBuffer,
                                    ProcessResult result);

    /**
     * Modify group consume control configure
     *
     * @param entity     the group consume control info entity will be update
     * @param strBuffer  the print info string buffer
     * @param result     the process result return
     * @return true if success otherwise false
     */
    boolean updGroupConsumeCtrlConf(GroupConsumeCtrlEntity entity,
                                    StringBuilder strBuffer,
                                    ProcessResult result);

    /**
     * Delete group consume control configure
     *
     * @param operator  operator
     * @param groupName the blacklist record related to group
     * @param topicName the blacklist record related to topic
     *                  allow groupName or topicName is null,
     *                  but not all null
     * @return true if success
     */
    boolean delGroupConsumeCtrlConf(String operator,
                                    String groupName,
                                    String topicName,
                                    StringBuilder strBuffer,
                                    ProcessResult result);
    /**
     * Delete group consume control configure
     *
     * @param operator  operator
     * @param recordKey the record key to group consume control record
     * @param strBuffer  the print info string buffer
     * @param result     the process result return
     * @return true if success
     */
    boolean delGroupConsumeCtrlConf(String operator,
                                    String recordKey,
                                    StringBuilder strBuffer,
                                    ProcessResult result);

    GroupConsumeCtrlEntity getGroupConsumeCtrlConfByRecKey(String recordKey);

    GroupConsumeCtrlEntity getConsumeCtrlByGroupAndTopic(String groupName, String topicName);

    Map<String/* group */, List<GroupConsumeCtrlEntity>> getConsumeCtrlByGroupAndTopic(
            Set<String> groupNameSet, Set<String> topicNameSet);

    List<GroupConsumeCtrlEntity> getConsumeCtrlByTopicName(String topicName);

    Set<String> getConsumeCtrlKeyByTopicName(Set<String> topicSet);

    Set<String> getConsumeCtrlKeyByGroupName(Set<String> groupSet);

    List<GroupConsumeCtrlEntity> getGroupConsumeCtrlConf(GroupConsumeCtrlEntity qryEntity);
}
