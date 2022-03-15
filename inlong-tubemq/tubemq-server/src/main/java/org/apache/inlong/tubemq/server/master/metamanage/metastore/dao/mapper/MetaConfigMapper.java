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
import org.apache.inlong.tubemq.corebase.rv.ProcessResult;
import org.apache.inlong.tubemq.server.common.statusdef.ManageStatus;
import org.apache.inlong.tubemq.server.common.statusdef.TopicStatus;
import org.apache.inlong.tubemq.server.master.metamanage.metastore.ConfigObserver;
import org.apache.inlong.tubemq.server.master.metamanage.metastore.KeepAliveService;
import org.apache.inlong.tubemq.server.master.metamanage.metastore.dao.entity.BaseEntity;
import org.apache.inlong.tubemq.server.master.metamanage.metastore.dao.entity.BrokerConfEntity;
import org.apache.inlong.tubemq.server.master.metamanage.metastore.dao.entity.ClusterSettingEntity;
import org.apache.inlong.tubemq.server.master.metamanage.metastore.dao.entity.GroupConsumeCtrlEntity;
import org.apache.inlong.tubemq.server.master.metamanage.metastore.dao.entity.GroupResCtrlEntity;
import org.apache.inlong.tubemq.server.master.metamanage.metastore.dao.entity.TopicCtrlEntity;
import org.apache.inlong.tubemq.server.master.metamanage.metastore.dao.entity.TopicDeployEntity;
import org.apache.inlong.tubemq.server.master.metamanage.metastore.dao.entity.TopicPropGroup;

public interface MetaConfigMapper extends KeepAliveService {

    /**
     * Register meta configure change observer
     *
     * @param eventObserver  the event observer
     */
    void regMetaConfigObserver(ConfigObserver eventObserver);

    boolean checkStoreStatus(boolean checkIsMaster, ProcessResult result);

    /**
     * Add or update cluster default setting
     * @param opEntity       operator information
     * @param brokerPort     broker port
     * @param brokerTlsPort  broker tls port
     * @param brokerWebPort  broker web port
     * @param maxMsgSizeMB   max cluster message size in MB
     * @param qryPriorityId  the default query priority id
     * @param flowCtrlEnable enable or disable flow control function
     * @param flowRuleCnt    the default flow rule count
     * @param flowCtrlInfo   the default flow control information
     * @param topicProps     default topic property information
     * @param strBuff        the print information string buffer
     * @param result         the process result return
     * @return true if success otherwise false
     */
    boolean addOrUpdClusterDefSetting(BaseEntity opEntity, int brokerPort,
                                      int brokerTlsPort, int brokerWebPort,
                                      int maxMsgSizeMB, int qryPriorityId,
                                      Boolean flowCtrlEnable, int flowRuleCnt,
                                      String flowCtrlInfo, TopicPropGroup topicProps,
                                      StringBuilder strBuff, ProcessResult result);

    /**
     * Get cluster configure information
     *
     * @param isMustConf  whether must be configured data.
     * @return the cluster configure
     */
    ClusterSettingEntity getClusterDefSetting(boolean isMustConf);

    // ////////////////////////////////////////////////////////////
    /**
     * Add or update broker configure information
     *
     * @param isAddOp        whether add operation
     * @param opEntity       operator information
     * @param brokerId       broker id
     * @param brokerIp       broker ip
     * @param brokerPort     broker port
     * @param brokerTlsPort  broker tls port
     * @param brokerWebPort  broker web port
     * @param regionId       region id
     * @param groupId        group id
     * @param mngStatus      manage status
     * @param topicProps     default topic property information
     * @param strBuff        the print information string buffer
     * @param result         the process result return
     * @return true if success otherwise false
     */
    boolean addOrUpdBrokerConfig(boolean isAddOp, BaseEntity opEntity,
                                 int brokerId, String brokerIp, int brokerPort,
                                 int brokerTlsPort, int brokerWebPort,
                                 int regionId, int groupId, ManageStatus mngStatus,
                                 TopicPropGroup topicProps, StringBuilder strBuff,
                                 ProcessResult result);

    /**
     * Add or update broker configure information
     *
     * @param isAddOp   whether add operation
     * @param entity    need add or update configure information
     * @param strBuff   the print information string buffer
     * @param result    the process result return
     * @return true if success otherwise false
     */
    boolean addOrUpdBrokerConfig(boolean isAddOp, BrokerConfEntity entity,
                                 StringBuilder strBuff, ProcessResult result);

    /**
     * Change broker configure status
     *
     * @param opEntity      operator
     * @param brokerId      need deleted broker id
     * @param newMngStatus  manage status
     * @param strBuff       the print information string buffer
     * @param result        the process result return
     * @return true if success otherwise false
     */
    boolean changeBrokerConfStatus(BaseEntity opEntity,
                                   int brokerId, ManageStatus newMngStatus,
                                   StringBuilder strBuff, ProcessResult result);

    /**
     * Delete broker configure information
     *
     * @param operator  operator
     * @param brokerId  need deleted broker id
     * @param rsvData   reserve broker data
     * @param strBuff   the print information string buffer
     * @param result     the process result return
     * @return true if success otherwise false
     */
    boolean delBrokerConfInfo(String operator, int brokerId, boolean rsvData,
                              StringBuilder strBuff, ProcessResult result);

    /**
     * Query broker configure information
     *
     * @param qryEntity  the query condition, must not null
     * @return  the query result
     */
    Map<Integer, BrokerConfEntity> getBrokerConfInfo(BrokerConfEntity qryEntity);

    /**
     * Get broker configure information
     *
     * @param brokerIdSet  the broker id set need to query
     * @param brokerIpSet  the broker ip set need to query
     * @param qryEntity    the query condition
     * @return broker configure information
     */
    Map<Integer, BrokerConfEntity> getBrokerConfInfo(Set<Integer> brokerIdSet,
                                                     Set<String> brokerIpSet,
                                                     BrokerConfEntity qryEntity);

    /**
     * Get broker configure information
     *
     * @param brokerId  need queried broker id
     * @return  the broker configure record
     */
    BrokerConfEntity getBrokerConfByBrokerId(int brokerId);

    /**
     * Get broker configure information
     *
     * @param brokerIp  need queried broker ip
     * @return  the broker configure record
     */
    BrokerConfEntity getBrokerConfByBrokerIp(String brokerIp);

    // ////////////////////////////////////////////////////////////

    /**
     * Add or Update topic control configure info
     *
     * @param isAddOp        whether add operation
     * @param opEntity       operator information
     * @param topicName      topic name
     * @param topicNameId    the topic name id
     * @param enableTopicAuth  whether enable topic authentication
     * @param maxMsgSizeInMB   the max message size in MB
     * @param strBuff          the print info string buffer
     * @param result           the process result return
     * @return true if success otherwise false
     */
    boolean addOrUpdTopicCtrlConf(boolean isAddOp, BaseEntity opEntity,
                                  String topicName, int topicNameId,
                                  Boolean enableTopicAuth, int maxMsgSizeInMB,
                                  StringBuilder strBuff, ProcessResult result);

    /**
     * Add or Update topic control configure info
     *
     * @param isAddOp  whether add operation
     * @param entity   the topic control info entity will be add
     * @param strBuff  the print info string buffer
     * @param result   the process result return
     * @return true if success otherwise false
     */
    boolean addOrUpdTopicCtrlConf(boolean isAddOp, TopicCtrlEntity entity,
                                  StringBuilder strBuff, ProcessResult result);

    /**
     * Add topic control record, or update records if data exists
     *
     * @param opEntity operator information
     * @param topicName topic info
     * @param enableTopicAuth if authenticate check
     * @param strBuff  the print info string buffer
     * @param result     the process result return
     * @return true if success otherwise false
     */
    boolean insertTopicCtrlConf(BaseEntity opEntity,
                                String topicName, Boolean enableTopicAuth,
                                StringBuilder strBuff, ProcessResult result);

    /**
     * Add topic control record, or update records if data exists
     *
     * @param entity  operator information
     * @param strBuff  the print info string buffer
     * @param result     the process result return
     * @return true if success otherwise false
     */
    boolean insertTopicCtrlConf(TopicCtrlEntity entity,
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

    /**
     * Get topic control record by topic name
     *
     * @param topicName  the topic name
     * @return    the topic control record
     */
    TopicCtrlEntity getTopicCtrlByTopicName(String topicName);

    /**
     * Get topic max message size in MB configure
     *
     * @param topicName   the topic name
     * @return   the max message size
     */
    int getTopicMaxMsgSizeInMB(String topicName);

    /**
     * Get topic control entity list
     *
     * @param qryEntity   the query condition
     * @return   the query result list
     */
    List<TopicCtrlEntity> queryTopicCtrlConf(TopicCtrlEntity qryEntity);

    /**
     * Get topic control entity list
     *
     * @param topicNameSet  the topic name set
     * @param qryEntity     the query condition
     * @return   the query result list
     */
    Map<String, TopicCtrlEntity> getTopicCtrlConf(Set<String> topicNameSet,
                                                  TopicCtrlEntity qryEntity);

    // ////////////////////////////////////////////////////////////

    /**
     * Add or update topic deploy information
     *
     * @param isAddOp         whether add operation
     * @param opEntity        the operation information
     * @param brokerId        the broker id
     * @param topicName       the topic name
     * @param deployStatus    the deploy status
     * @param topicPropInfo   the topic property set
     * @param strBuff         the string buffer
     * @param result          the process result
     * @return                true if success otherwise false
     */
    boolean addOrUpdTopicDeployInfo(boolean isAddOp, BaseEntity opEntity,
                                    int brokerId, String topicName,
                                    TopicStatus deployStatus,
                                    TopicPropGroup topicPropInfo,
                                    StringBuilder strBuff,
                                    ProcessResult result);

    /**
     * Add or update topic deploy configure info
     *
     * @param isAddOp  whether add operation
     * @param deployEntity   the topic deploy info entity
     * @param strBuff  the print info string buffer
     * @param result   the process result return
     * @return true if success otherwise false
     */
    boolean addOrUpdTopicDeployInfo(boolean isAddOp, TopicDeployEntity deployEntity,
                                    StringBuilder strBuff, ProcessResult result);

    /**
     * Update topic deploy status info
     *
     * @param opEntity       the operation information
     * @param brokerId       the broker id
     * @param topicName      the topic name
     * @param topicStatus    the topic deploy status
     * @param strBuff  the print info string buffer
     * @param result   the process result return
     * @return true if success otherwise false
     */
    boolean updTopicDeployStatusInfo(BaseEntity opEntity, int brokerId,
                                     String topicName, TopicStatus topicStatus,
                                     StringBuilder strBuff, ProcessResult result);

    /**
     * Get broker topic entity, if query entity is null, return all topic entity
     *
     * @param topicNameSet  query by topicNameSet
     * @param brokerIdSet   query by brokerIdSet
     * @param qryEntity     query conditions
     * @return topic deploy entity map
     */
    Map<String, List<TopicDeployEntity>> getTopicDeployInfoMap(Set<String> topicNameSet,
                                                               Set<Integer> brokerIdSet,
                                                               TopicDeployEntity qryEntity);

    /**
     * Get broker topic entity, if query entity is null, return all topic entity
     *
     * @param topicNameSet   the query topic set
     * @param brokerIdSet    the query broker id set
     * @return topic deploy entity map
     */
    Map<Integer, List<TopicDeployEntity>> getTopicDeployInfoMap(Set<String> topicNameSet,
                                                                Set<Integer> brokerIdSet);

    /**
     * Get broker topic entity by topic name and broker id set
     *
     * @param topicNameSet   the query topic set
     * @param brokerIdSet    the query broker id set
     * @return topic entity map
     */
    Map<String, List<TopicDeployEntity>> getTopicConfInfoByTopicAndBrokerIds(
            Set<String> topicNameSet, Set<Integer> brokerIdSet);

    // ////////////////////////////////////////////////////////////

    /**
     * Add or update group resource configure information
     *
     * @param isAddOp        whether add operation
     * @param opEntity       operator information
     * @param groupName      the group name
     * @param resCheckEnable  whether check resource rate
     * @param allowedBClientRate  the allowed broker-client rate
     * @param qryPriorityId  the query priority id
     * @param flowCtrlEnable enable or disable flow control
     * @param flowRuleCnt    the flow control rule count
     * @param flowCtrlInfo   the flow control information
     * @param strBuff        the print information string buffer
     * @param result         the process result return
     * @return true if success otherwise false
     */
    boolean addOrUpdGroupResCtrlConf(boolean isAddOp, BaseEntity opEntity,
                                     String groupName, Boolean resCheckEnable,
                                     int allowedBClientRate, int qryPriorityId,
                                     Boolean flowCtrlEnable, int flowRuleCnt,
                                     String flowCtrlInfo, StringBuilder strBuff,
                                     ProcessResult result);

    /**
     * Add group resource control configure info
     *
     * @param entity     the group resource control info entity will be add
     * @param strBuff  the print info string buffer
     * @param result     the process result return
     * @return true if success otherwise false
     */
    boolean addOrUpdGroupResCtrlConf(boolean isAddOp, GroupResCtrlEntity entity,
                                     StringBuilder strBuff, ProcessResult result);

    /**
     * Add group control configure, or update records if data exists
     *
     * @param opEntity       operator information
     * @param groupName      the group name
     * @param qryPriorityId  the query priority id
     * @param flowCtrlEnable enable or disable flow control
     * @param flowRuleCnt    the flow control rule count
     * @param flowCtrlRuleInfo   the flow control information
     * @param strBuff        the print information string buffer
     * @param result         the process result return
     * @return true if success otherwise false
     */
    boolean insertGroupCtrlConf(BaseEntity opEntity, String groupName,
                                int qryPriorityId, Boolean flowCtrlEnable,
                                int flowRuleCnt, String flowCtrlRuleInfo,
                                StringBuilder strBuff, ProcessResult result);

    /**
     * Add group control configure, or update records if data exists
     *
     * @param opEntity         the group resource control info entity will be add
     * @param groupName        operate target
     * @param resChkEnable     resource check status
     * @param allowedB2CRate   allowed B2C rate
     * @param strBuff          the print info string buffer
     * @param result           the process result return
     * @return true if success otherwise false
     */
    boolean insertGroupCtrlConf(BaseEntity opEntity, String groupName,
                                Boolean resChkEnable, int allowedB2CRate,
                                StringBuilder strBuff, ProcessResult result);

    /**
     * Add group control configure, or update records if data exists
     *
     * @param entity  the group resource control info entity will be add
     * @param strBuff   the print info string buffer
     * @param result    the process result return
     * @return true if success otherwise false
     */
    boolean insertGroupCtrlConf(GroupResCtrlEntity entity,
                                StringBuilder strBuff, ProcessResult result);

    /**
     * Delete group control information
     *
     * @param operator       operator
     * @param groupName      need deleted group
     * @param strBuff        the print info string buffer
     * @param result         the process result return
     * @return    delete result
     */
    boolean delGroupCtrlConf(String operator, String groupName,
                             StringBuilder strBuff, ProcessResult result);

    /**
     * Get group control information by group and query condition
     *
     * @param groupSet       need queried group set
     * @param qryEntity      query condition
     * @return    query result
     */
    Map<String, GroupResCtrlEntity> getGroupCtrlConf(Set<String> groupSet,
                                                     GroupResCtrlEntity qryEntity);

    /**
     * Get group control information by group name
     *
     * @param groupName       need queried group name
     * @return    query result
     */
    GroupResCtrlEntity getGroupCtrlConf(String groupName);

    // //////////////////////////////////////////////////////////////////

    /**
     * Add or update group resource configure information
     *
     * @param isAddOp        whether add operation
     * @param opEntity       operator information
     * @param groupName      the group name
     * @param topicName      the topic name
     * @param enableCsm      enable or disable consume
     * @param disableRsn     the disable reason
     * @param enableFlt      enable or disable filter
     * @param fltCondStr     the filter conditions
     * @param strBuff        the print information string buffer
     * @param result         the process result return
     * @return true if success otherwise false
     */
    boolean addOrUpdConsumeCtrlInfo(boolean isAddOp, BaseEntity opEntity,
                                    String groupName, String topicName,
                                    Boolean enableCsm, String disableRsn,
                                    Boolean enableFlt, String fltCondStr,
                                    StringBuilder strBuff, ProcessResult result);

    /**
     * add or update group's consume control information
     *
     * @param isAddOp   whether add operation
     * @param entity    need add or update group configure info
     * @param strBuff   the print info string buffer
     * @param result    the process result return
     * @return true if success otherwise false
     */
    boolean addOrUpdConsumeCtrlInfo(boolean isAddOp, GroupConsumeCtrlEntity entity,
                                    StringBuilder strBuff, ProcessResult result);

    /**
     * Add consume control information, or update records if data exists
     *
     * @param opEntity   add or update base information, include creator, create time, etc.
     * @param groupName  add or update groupName information
     * @param topicName  add or update topicName information
     * @param enableCsm  add or update consume enable status information
     * @param disReason  add or update disable consume reason
     * @param enableFlt  add or update filter enable status information
     * @param fltCondStr add or update filter configure information
     * @param strBuff    the print info string buffer
     * @param result     the process result return
     * @return    process result
     */
    boolean insertConsumeCtrlInfo(BaseEntity opEntity, String groupName,
                                  String topicName, Boolean enableCsm,
                                  String disReason, Boolean enableFlt,
                                  String fltCondStr, StringBuilder strBuff,
                                  ProcessResult result);

    /**
     * Add consume control information, or update records if data exists
     *
     * @param entity     add or update group consume control info
     * @param strBuff    the print info string buffer
     * @param result     the process result return
     * @return    process result
     */
    boolean insertConsumeCtrlInfo(GroupConsumeCtrlEntity entity,
                                  StringBuilder strBuff, ProcessResult result);

    /**
     * Delete consume control configure
     *
     * @param operator   the operator
     * @param groupName  the group name
     * @param topicName  the topic name
     * @param strBuff    the print info string buffer
     * @param result     the process result return
     * @return    process result
     */
    boolean delConsumeCtrlConf(String operator,
                               String groupName, String topicName,
                               StringBuilder strBuff, ProcessResult result);

    /**
     * Get all group consume control record for a specific topic
     *
     * @param topicName  the queried topic name
     * @return group consume control list
     */
    List<GroupConsumeCtrlEntity> getConsumeCtrlByTopic(String topicName);

    /**
     * Get all disable consumed topic for a specific group
     *
     * @param groupName  the queried group name
     * @return  the disable consumed topic list
     */
    Set<String> getDisableTopicByGroupName(String groupName);

    /**
     * Get group consume control configure for topic & group set
     *
     * @param groupSet the topic name set
     * @param topicSet the group name set
     * @param qryEntry the query conditions
     * @return  the queried consume control record
     */
    Map<String, List<GroupConsumeCtrlEntity>> getGroupConsumeCtrlConf(
            Set<String> groupSet, Set<String> topicSet, GroupConsumeCtrlEntity qryEntry);

}
