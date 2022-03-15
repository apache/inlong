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

package org.apache.inlong.tubemq.server.master.metamanage.metastore.impl.bdbimpl;

import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.Durability;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.EnvironmentFailureException;
import com.sleepycat.je.rep.InsufficientLogException;
import com.sleepycat.je.rep.NetworkRestore;
import com.sleepycat.je.rep.NetworkRestoreConfig;
import com.sleepycat.je.rep.NodeState;
import com.sleepycat.je.rep.ReplicatedEnvironment;
import com.sleepycat.je.rep.ReplicationConfig;
import com.sleepycat.je.rep.ReplicationGroup;
import com.sleepycat.je.rep.ReplicationMutableConfig;
import com.sleepycat.je.rep.ReplicationNode;
import com.sleepycat.je.rep.StateChangeEvent;
import com.sleepycat.je.rep.StateChangeListener;
import com.sleepycat.je.rep.TimeConsistencyPolicy;
import com.sleepycat.je.rep.UnknownMasterException;
import com.sleepycat.je.rep.util.ReplicationGroupAdmin;
import com.sleepycat.je.rep.utilint.ServiceDispatcher;
import com.sleepycat.persist.StoreConfig;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.inlong.tubemq.corebase.TBaseConstants;
import org.apache.inlong.tubemq.corebase.TokenConstants;
import org.apache.inlong.tubemq.corebase.rv.ProcessResult;
import org.apache.inlong.tubemq.corebase.utils.TStringUtils;
import org.apache.inlong.tubemq.corebase.utils.Tuple2;
import org.apache.inlong.tubemq.server.common.fileconfig.MasterReplicationConfig;
import org.apache.inlong.tubemq.server.master.MasterConfig;
import org.apache.inlong.tubemq.server.master.bdbstore.MasterGroupStatus;
import org.apache.inlong.tubemq.server.master.bdbstore.MasterNodeInfo;
import org.apache.inlong.tubemq.server.master.metamanage.DataOpErrCode;
import org.apache.inlong.tubemq.server.master.metamanage.metastore.MetaConfigObserver;
import org.apache.inlong.tubemq.server.master.metamanage.metastore.MetaStoreService;
import org.apache.inlong.tubemq.server.master.metamanage.metastore.dao.entity.BrokerConfEntity;
import org.apache.inlong.tubemq.server.master.metamanage.metastore.dao.entity.ClusterSettingEntity;
import org.apache.inlong.tubemq.server.master.metamanage.metastore.dao.entity.GroupConsumeCtrlEntity;
import org.apache.inlong.tubemq.server.master.metamanage.metastore.dao.entity.GroupResCtrlEntity;
import org.apache.inlong.tubemq.server.master.metamanage.metastore.dao.entity.TopicCtrlEntity;
import org.apache.inlong.tubemq.server.master.metamanage.metastore.dao.entity.TopicDeployEntity;
import org.apache.inlong.tubemq.server.master.metamanage.metastore.dao.mapper.BrokerConfigMapper;
import org.apache.inlong.tubemq.server.master.metamanage.metastore.dao.mapper.ClusterConfigMapper;
import org.apache.inlong.tubemq.server.master.metamanage.metastore.dao.mapper.ConsumeCtrlMapper;
import org.apache.inlong.tubemq.server.master.metamanage.metastore.dao.mapper.GroupResCtrlMapper;
import org.apache.inlong.tubemq.server.master.metamanage.metastore.dao.mapper.TopicCtrlMapper;
import org.apache.inlong.tubemq.server.master.metamanage.metastore.dao.mapper.TopicDeployMapper;
import org.apache.inlong.tubemq.server.master.utils.MetaConfigSamplePrint;
import org.apache.inlong.tubemq.server.master.web.model.ClusterGroupVO;
import org.apache.inlong.tubemq.server.master.web.model.ClusterNodeVO;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BdbMetaStoreServiceImpl implements MetaStoreService {

    private static final int REP_HANDLE_RETRY_MAX = 1;

    private static final Logger logger =
            LoggerFactory.getLogger(BdbMetaStoreServiceImpl.class);
    private final MetaConfigSamplePrint metaConfigSamplePrint =
            new MetaConfigSamplePrint(logger);
    // parameters need input
    // local host name
    private final String nodeHost;
    // meta data store path
    private final String metaDataPath;
    // bdb replication configure
    private final MasterReplicationConfig replicationConfig;
    private final Listener listener = new Listener();
    private ExecutorService executorService = null;
    private final List<MetaConfigObserver> eventObservers = new ArrayList<>();
    // service status
    // 0 stopped, 1 starting, 2 started, 3 stopping
    private final AtomicInteger srvStatus = new AtomicInteger(0);
    // master role flag
    private volatile boolean isMaster = false;
    // time since node become active
    private final AtomicLong masterSinceTime = new AtomicLong(Long.MAX_VALUE);
    // master node name
    private String masterNodeName;
    // node connect failure count
    private int connectNodeFailCount = 0;
    // replication nodes
    private Set<String> replicas4Transfer = new HashSet<>();

    // meta data store file
    private File envHome;
    // bdb replication configure
    private ReplicationConfig repConfig;
    // bdb environment configure
    private EnvironmentConfig envConfig;
    // bdb replicated environment
    private ReplicatedEnvironment repEnv;
    // bdb data store configure
    private final StoreConfig storeConfig = new StoreConfig();
    // bdb replication group admin info
    private final ReplicationGroupAdmin replicationGroupAdmin;

    // cluster default setting
    private ClusterConfigMapper clusterConfigMapper;
    // broker configure
    private BrokerConfigMapper brokerConfigMapper;
    // topic deployment configure
    private TopicDeployMapper topicDeployMapper;
    // topic control configure
    private TopicCtrlMapper topicCtrlMapper;
    // group resource control configure
    private GroupResCtrlMapper groupResCtrlMapper;
    // group consume control configure
    private ConsumeCtrlMapper consumeCtrlMapper;

    public BdbMetaStoreServiceImpl(MasterConfig masterConfig) {
        this.nodeHost = masterConfig.getHostName();
        this.metaDataPath = masterConfig.getMetaDataPath();
        this.replicationConfig = masterConfig.getReplicationConfig();
        // build replicationGroupAdmin info
        Set<InetSocketAddress> helpers = new HashSet<>();
        for (int i = 1; i <= 3; i++) {
            InetSocketAddress helper = new InetSocketAddress(
                    this.nodeHost, replicationConfig.getRepNodePort() + i);
            helpers.add(helper);
        }
        this.replicationGroupAdmin =
                new ReplicationGroupAdmin(this.replicationConfig.getRepGroupName(), helpers);
    }

    @Override
    public void start() throws Exception {
        if (!srvStatus.compareAndSet(0, 1)) {
            return;
        }
        try {
            if (executorService != null) {
                executorService.shutdownNow();
                executorService = null;
            }
            executorService = Executors.newSingleThreadExecutor();
            initEnvConfig();
            repEnv = getEnvironment();
            initMetaStore();
            repEnv.setStateChangeListener(listener);
            srvStatus.compareAndSet(1, 2);
        } catch (Throwable ee) {
            srvStatus.compareAndSet(1, 0);
            logger.error("[BDB Impl] start StoreManagerService failure, error", ee);
            return;
        }
        logger.info("[BDB Impl] start StoreManagerService success");
    }

    @Override
    public void stop() throws Exception {
        if (!srvStatus.compareAndSet(2, 3)) {
            return;
        }
        logger.info("[BDB Impl] Stopping StoreManagerService...");
        // close bdb configure
        brokerConfigMapper.close();
        topicDeployMapper.close();
        groupResCtrlMapper.close();
        topicCtrlMapper.close();
        consumeCtrlMapper.close();
        clusterConfigMapper.close();
        /* evn close */
        if (repEnv != null) {
            try {
                repEnv.close();
                repEnv = null;
            } catch (Throwable ee) {
                logger.error("[BDB Impl] Close repEnv throw error ", ee);
            }
        }
        if (executorService != null) {
            executorService.shutdownNow();
            executorService = null;
        }
        srvStatus.set(0);
        logger.info("[BDB Impl] stopping StoreManagerService successfully...");
    }

    // cluster default configure api
    @Override
    public boolean addUpdClusterConfig(ClusterSettingEntity entity,
                                       StringBuilder strBuff, ProcessResult result) {
        // check current status
        if (!checkStoreStatus(true, result)) {
            return result.isSuccess();
        }
        if (clusterConfigMapper.addUpdClusterConfig(entity, strBuff, result)) {
            ClusterSettingEntity newEntity = clusterConfigMapper.getClusterConfig();
            strBuff.append("[insertClusterConfig], ").append(entity.getCreateUser())
                    .append(" insert cluster setting record :").append(newEntity);
            logger.info(strBuff.toString());
        } else {
            strBuff.append("[addUpdClusterConfig], ")
                    .append("failure to insert cluster setting record : ")
                    .append(result.getErrMsg());
            logger.warn(strBuff.toString());
        }
        strBuff.delete(0, strBuff.length());
        return result.isSuccess();
    }

    @Override
    public ClusterSettingEntity getClusterConfig() {
        return clusterConfigMapper.getClusterConfig();
    }

    @Override
    public boolean delClusterConfig(String operator,
                                    StringBuilder strBuff, ProcessResult result) {
        if (!checkStoreStatus(true, result)) {
            return false;
        }
        if (clusterConfigMapper.delClusterConfig(strBuff, result)) {
            ClusterSettingEntity entity =
                    (ClusterSettingEntity) result.getRetData();
            if (entity != null) {
                strBuff.append("[delClusterConfig], ").append(operator)
                        .append(" deleted cluster setting record :").append(entity);
                logger.info(strBuff.toString());
            }
        } else {
            strBuff.append("[delClusterConfig], ")
                    .append("failure to delete cluster setting record : ")
                    .append(result.getErrMsg());
            logger.warn(strBuff.toString());
        }
        strBuff.delete(0, strBuff.length());
        return result.isSuccess();
    }

    // broker configure api
    @Override
    public boolean addBrokerConf(BrokerConfEntity entity,
                                 StringBuilder strBuff, ProcessResult result) {
        // check current status
        if (!checkStoreStatus(true, result)) {
            return result.isSuccess();
        }
        if (brokerConfigMapper.addBrokerConf(entity, strBuff, result)) {
            strBuff.append("[addBrokerConf], ").append(entity.getCreateUser())
                    .append(" added broker configure record :").append(entity);
            logger.info(strBuff.toString());
        } else {
            strBuff.append("[addBrokerConf], ")
                    .append("failure to add broker configure record : ")
                    .append(result.getErrMsg());
            logger.warn(strBuff.toString());
        }
        strBuff.delete(0, strBuff.length());
        return result.isSuccess();
    }

    @Override
    public boolean updBrokerConf(BrokerConfEntity entity,
                                 StringBuilder strBuff, ProcessResult result) {
        // check current status
        if (!checkStoreStatus(true, result)) {
            return result.isSuccess();
        }
        BrokerConfEntity curEntity =
                brokerConfigMapper.getBrokerConfByBrokerId(entity.getBrokerId());
        if (brokerConfigMapper.updBrokerConf(entity, strBuff, result)) {
            BrokerConfEntity newEntity =
                    brokerConfigMapper.getBrokerConfByBrokerId(entity.getBrokerId());
            strBuff.append("[updBrokerConf], ")
                    .append(entity.getModifyUser())
                    .append(" updated broker configure record from :")
                    .append(curEntity).append(" to ").append(newEntity);
            logger.info(strBuff.toString());
        } else {
            strBuff.append("[updBrokerConf], ")
                    .append("failure to update broker configure record : ")
                    .append(result.getErrMsg());
            logger.warn(strBuff.toString());
        }
        strBuff.delete(0, strBuff.length());
        return result.isSuccess();
    }

    @Override
    public boolean delBrokerConf(String operator, int brokerId,
                                 StringBuilder strBuffer, ProcessResult result) {
        // check current status
        if (!checkStoreStatus(true, result)) {
            return result.isSuccess();
        }
        if (brokerConfigMapper.delBrokerConf(brokerId, strBuffer, result)) {
            BrokerConfEntity entity = (BrokerConfEntity) result.getRetData();
            if (entity != null) {
                strBuffer.append("[delBrokerConf], ").append(operator)
                        .append(" deleted broker configure record :").append(entity);
                logger.info(strBuffer.toString());
            }
        } else {
            strBuffer.append("[delBrokerConf], ")
                    .append("failure to delete broker configure record : ")
                    .append(result.getErrMsg());
            logger.warn(strBuffer.toString());
        }
        strBuffer.delete(0, strBuffer.length());
        return result.isSuccess();
    }

    @Override
    public Map<Integer, BrokerConfEntity> getBrokerConfInfo(BrokerConfEntity qryEntity) {
        return brokerConfigMapper.getBrokerConfInfo(qryEntity);
    }

    @Override
    public Map<Integer, BrokerConfEntity> getBrokerConfInfo(Set<Integer> brokerIdSet,
                                                            Set<String> brokerIpSet,
                                                            BrokerConfEntity qryEntity) {
        return brokerConfigMapper.getBrokerConfInfo(brokerIdSet, brokerIpSet, qryEntity);
    }

    @Override
    public BrokerConfEntity getBrokerConfByBrokerId(int brokerId) {
        return brokerConfigMapper.getBrokerConfByBrokerId(brokerId);
    }

    @Override
    public BrokerConfEntity getBrokerConfByBrokerIp(String brokerIp) {
        return brokerConfigMapper.getBrokerConfByBrokerIp(brokerIp);
    }

    // topic configure api
    @Override
    public boolean addTopicConf(TopicDeployEntity entity,
                                StringBuilder strBuff, ProcessResult result) {
        // check current status
        if (!checkStoreStatus(true, result)) {
            return result.isSuccess();
        }
        if (topicDeployMapper.addTopicDeployConf(entity, strBuff, result)) {
            strBuff.append("[addTopicConf], ").append(entity.getCreateUser())
                    .append(" added topic configure record :").append(entity);
            logger.info(strBuff.toString());
        } else {
            strBuff.append("[addTopicConf], ")
                    .append("failure to add topic configure record : ")
                    .append(result.getErrMsg());
            logger.warn(strBuff.toString());
        }
        strBuff.delete(0, strBuff.length());
        return result.isSuccess();
    }

    @Override
    public boolean updTopicConf(TopicDeployEntity entity,
                                StringBuilder strBuff, ProcessResult result) {
        // check current status
        if (!checkStoreStatus(true, result)) {
            return result.isSuccess();
        }
        TopicDeployEntity curEntity =
                topicDeployMapper.getTopicConfByeRecKey(entity.getRecordKey());
        if (topicDeployMapper.updTopicDeployConf(entity, strBuff, result)) {
            TopicDeployEntity newEntity =
                    topicDeployMapper.getTopicConfByeRecKey(entity.getRecordKey());
            strBuff.append("[updTopicConf], ").append(entity.getModifyUser())
                    .append(" updated record from :").append(curEntity)
                    .append(" to ").append(newEntity);
            logger.info(strBuff.toString());
        } else {
            strBuff.append("[updTopicConf], ")
                    .append("failure to update topic configure record : ")
                    .append(result.getErrMsg());
            logger.warn(strBuff.toString());
        }
        strBuff.delete(0, strBuff.length());
        return result.isSuccess();
    }

    @Override
    public boolean delTopicConf(String operator, String recordKey,
                                StringBuilder strBuff, ProcessResult result) {
        // check current status
        if (!checkStoreStatus(true, result)) {
            return result.isSuccess();
        }
        if (topicDeployMapper.delTopicDeployConf(recordKey, strBuff, result)) {
            GroupResCtrlEntity entity =
                    (GroupResCtrlEntity) result.getRetData();
            if (entity != null) {
                strBuff.append("[delTopicConf], ").append(operator)
                        .append(" deleted topic configure record :").append(entity);
                logger.info(strBuff.toString());
            }
        } else {
            strBuff.append("[delTopicConf], ")
                    .append("failure to delete topic configure record : ")
                    .append(result.getErrMsg());
            logger.warn(strBuff.toString());
        }
        strBuff.delete(0, strBuff.length());
        return result.isSuccess();
    }

    @Override
    public boolean delTopicConfByBrokerId(String operator, int brokerId,
                                          StringBuilder strBuff, ProcessResult result) {
        // check current status
        if (!checkStoreStatus(true, result)) {
            return result.isSuccess();
        }
        if (topicDeployMapper.delTopicConfByBrokerId(brokerId, strBuff, result)) {
            strBuff.append("[delTopicConfByBrokerId], ")
                    .append(operator)
                    .append(" deleted topic deploy record :")
                    .append(brokerId);
            logger.info(strBuff.toString());
        } else {
            strBuff.append("[delTopicConfByBrokerId], ")
                    .append("failure to delete topic deploy record : ")
                    .append(brokerId).append(result.getErrMsg());
            logger.warn(strBuff.toString());
        }
        strBuff.delete(0, strBuff.length());
        return result.isSuccess();
    }

    @Override
    public boolean hasConfiguredTopics(int brokerId) {
        return topicDeployMapper.hasConfiguredTopics(brokerId);
    }

    @Override
    public boolean isTopicDeployed(String topicName) {
        return topicDeployMapper.isTopicDeployed(topicName);
    }

    @Override
    public TopicDeployEntity getTopicConfByeRecKey(String recordKey) {
        return topicDeployMapper.getTopicConfByeRecKey(recordKey);
    }

    @Override
    public List<TopicDeployEntity> getTopicConf(TopicDeployEntity qryEntity) {
        return topicDeployMapper.getTopicConf(qryEntity);
    }

    @Override
    public TopicDeployEntity getTopicConf(int brokerId, String topicName) {
        return topicDeployMapper.getTopicConf(brokerId, topicName);
    }

    @Override
    public Map<Integer, Set<String>> getConfiguredTopicInfo(Set<Integer> brokerIdSet) {
        return topicDeployMapper.getConfiguredTopicInfo(brokerIdSet);
    }

    @Override
    public Map<String, TopicDeployEntity> getConfiguredTopicInfo(int brokerId) {
        return topicDeployMapper.getConfiguredTopicInfo(brokerId);
    }

    @Override
    public Map<String, Map<Integer, String>> getTopicBrokerInfo(Set<String> topicNameSet) {
        return topicDeployMapper.getTopicBrokerInfo(topicNameSet);
    }

    @Override
    public Set<String> getConfiguredTopicSet() {
        return topicDeployMapper.getConfiguredTopicSet();
    }

    @Override
    public Map<String/* topicName */, List<TopicDeployEntity>> getTopicConfMap(
            Set<String> topicNameSet, Set<Integer> brokerIdSet, TopicDeployEntity qryEntity) {
        return topicDeployMapper.getTopicConfMap(topicNameSet, brokerIdSet, qryEntity);
    }

    @Override
    public Map<Integer/* brokerId */, List<TopicDeployEntity>> getTopicDeployInfoMap(
            Set<String> topicNameSet, Set<Integer> brokerIdSet) {
        return topicDeployMapper.getTopicDeployInfoMap(topicNameSet, brokerIdSet);
    }

    @Override
    public Map<String, List<TopicDeployEntity>> getTopicDepInfoByTopicBrokerId(
            Set<String> topicSet, Set<Integer> brokerIdSet) {
        return topicDeployMapper.getTopicConfMapByTopicAndBrokerIds(topicSet, brokerIdSet);
    }

    // topic control api
    @Override
    public boolean addTopicCtrlConf(TopicCtrlEntity entity,
                                    StringBuilder strBuff, ProcessResult result) {
        // check current status
        if (!checkStoreStatus(true, result)) {
            return result.isSuccess();
        }
        if (topicCtrlMapper.addTopicCtrlConf(entity, strBuff, result)) {
            strBuff.append("[addTopicCtrlConf], ").append(entity.getCreateUser())
                    .append(" added topic control record :").append(entity);
            logger.info(strBuff.toString());
        } else {
            strBuff.append("[addTopicCtrlConf], ")
                    .append("failure to add topic control record : ")
                    .append(result.getErrMsg());
            logger.warn(strBuff.toString());
        }
        strBuff.delete(0, strBuff.length());
        return result.isSuccess();
    }

    @Override
    public boolean updTopicCtrlConf(TopicCtrlEntity entity,
                                    StringBuilder strBuff, ProcessResult result) {
        // check current status
        if (!checkStoreStatus(true, result)) {
            return result.isSuccess();
        }
        TopicCtrlEntity curEntity =
                topicCtrlMapper.getTopicCtrlConf(entity.getTopicName());
        if (topicCtrlMapper.updTopicCtrlConf(entity, strBuff, result)) {
            TopicCtrlEntity newEntity =
                    topicCtrlMapper.getTopicCtrlConf(entity.getTopicName());
            strBuff.append("[updTopicCtrlConf], ").append(entity.getModifyUser())
                    .append(" updated record from :").append(curEntity)
                    .append(" to ").append(newEntity);
            logger.info(strBuff.toString());
        } else {
            strBuff.append("[updTopicCtrlConf], ")
                    .append("failure to update topic control record : ")
                    .append(result.getErrMsg());
            logger.warn(strBuff.toString());
        }
        strBuff.delete(0, strBuff.length());
        return result.isSuccess();
    }

    @Override
    public boolean delTopicCtrlConf(String operator, String topicName,
                                    StringBuilder strBuff, ProcessResult result) {
        // check current status
        if (!checkStoreStatus(true, result)) {
            return result.isSuccess();
        }
        if (topicCtrlMapper.delTopicCtrlConf(topicName, strBuff, result)) {
            TopicCtrlEntity entity =
                    (TopicCtrlEntity) result.getRetData();
            if (entity != null) {
                strBuff.append("[delTopicCtrlConf], ").append(operator)
                        .append(" deleted topic control record :").append(entity);
                logger.info(strBuff.toString());
            }
        } else {
            strBuff.append("[delTopicCtrlConf], ")
                    .append("failure to delete topic control record : ")
                    .append(result.getErrMsg());
            logger.warn(strBuff.toString());
        }
        strBuff.delete(0, strBuff.length());
        return result.isSuccess();
    }

    @Override
    public TopicCtrlEntity getTopicCtrlConf(String topicName) {
        return topicCtrlMapper.getTopicCtrlConf(topicName);
    }

    @Override
    public List<TopicCtrlEntity> getTopicCtrlConf(TopicCtrlEntity qryEntity) {
        return topicCtrlMapper.getTopicCtrlConf(qryEntity);
    }

    @Override
    public Map<String, TopicCtrlEntity> getTopicCtrlConf(Set<String> topicNameSet,
                                                         TopicCtrlEntity qryEntity) {
        return topicCtrlMapper.getTopicCtrlConf(topicNameSet, qryEntity);
    }

    // group configure api
    @Override
    public boolean addGroupResCtrlConf(GroupResCtrlEntity entity,
                                       StringBuilder strBuff, ProcessResult result) {
        // check current status
        if (!checkStoreStatus(true, result)) {
            return result.isSuccess();
        }
        if (groupResCtrlMapper.addGroupResCtrlConf(entity, strBuff, result)) {
            strBuff.append("[addGroupResCtrlConf], ").append(entity.getCreateUser())
                    .append(" added group resource control record :").append(entity);
            logger.info(strBuff.toString());
        } else {
            strBuff.append("[addGroupResCtrlConf], ")
                    .append("failure to add group resource control record : ")
                    .append(result.getErrMsg());
            logger.warn(strBuff.toString());
        }
        strBuff.delete(0, strBuff.length());
        return result.isSuccess();
    }

    @Override
    public boolean updGroupResCtrlConf(GroupResCtrlEntity entity,
                                       StringBuilder strBuff, ProcessResult result) {
        // check current status
        if (!checkStoreStatus(true, result)) {
            return result.isSuccess();
        }
        GroupResCtrlEntity curEntity =
                groupResCtrlMapper.getGroupResCtrlConf(entity.getGroupName());
        if (groupResCtrlMapper.updGroupResCtrlConf(entity, strBuff, result)) {
            GroupResCtrlEntity newEntity =
                    groupResCtrlMapper.getGroupResCtrlConf(entity.getGroupName());
            strBuff.append("[updGroupResCtrlConf], ").append(entity.getModifyUser())
                    .append(" updated record from :").append(curEntity)
                    .append(" to ").append(newEntity);
            logger.info(strBuff.toString());
        } else {
            strBuff.append("[updGroupResCtrlConf], ")
                    .append("failure to update group resource control record : ")
                    .append(result.getErrMsg());
            logger.warn(strBuff.toString());
        }
        strBuff.delete(0, strBuff.length());
        return result.isSuccess();
    }

    @Override
    public boolean delGroupResCtrlConf(String operator, String groupName,
                                       StringBuilder strBuff, ProcessResult result) {
        // check current status
        if (!checkStoreStatus(true, result)) {
            return result.isSuccess();
        }
        if (groupResCtrlMapper.delGroupResCtrlConf(groupName, strBuff, result)) {
            GroupResCtrlEntity entity =
                    (GroupResCtrlEntity) result.getRetData();
            if (entity != null) {
                strBuff.append("[delGroupResCtrlConf], ").append(operator)
                        .append(" deleted group resource control record :").append(entity);
                logger.info(strBuff.toString());
            }
        } else {
            strBuff.append("[delGroupResCtrlConf], ")
                    .append("failure to delete group resource control record : ")
                    .append(result.getErrMsg());
            logger.warn(strBuff.toString());
        }
        strBuff.delete(0, strBuff.length());
        return result.isSuccess();
    }

    @Override
    public GroupResCtrlEntity getGroupResCtrlConf(String groupName) {
        return groupResCtrlMapper.getGroupResCtrlConf(groupName);
    }

    @Override
    public Map<String, GroupResCtrlEntity> getGroupResCtrlConf(Set<String> groupSet,
                                                               GroupResCtrlEntity qryEntity) {
        return groupResCtrlMapper.getGroupResCtrlConf(groupSet, qryEntity);
    }

    @Override
    public boolean addGroupConsumeCtrlConf(GroupConsumeCtrlEntity entity,
                                           StringBuilder strBuff, ProcessResult result) {
        // check current status
        if (!checkStoreStatus(true, result)) {
            return result.isSuccess();
        }
        if (consumeCtrlMapper.addGroupConsumeCtrlConf(entity, strBuff, result)) {
            strBuff.append("[addGroupConsumeCtrlConf], ").append(entity.getCreateUser())
                    .append(" added group consume control record :").append(entity);
            logger.info(strBuff.toString());
        } else {
            strBuff.append("[addGroupConsumeCtrlConf], ")
                    .append("failure to add group consume control record : ")
                    .append(result.getErrMsg());
            logger.warn(strBuff.toString());
        }
        strBuff.delete(0, strBuff.length());
        return result.isSuccess();
    }

    @Override
    public boolean updGroupConsumeCtrlConf(GroupConsumeCtrlEntity entity,
                                           StringBuilder strBuff, ProcessResult result) {
        // check current status
        if (!checkStoreStatus(true, result)) {
            return result.isSuccess();
        }
        GroupConsumeCtrlEntity curEntity =
                consumeCtrlMapper.getGroupConsumeCtrlConfByRecKey(entity.getRecordKey());
        if (consumeCtrlMapper.updGroupConsumeCtrlConf(entity, strBuff, result)) {
            GroupConsumeCtrlEntity newEntity =
                    consumeCtrlMapper.getGroupConsumeCtrlConfByRecKey(entity.getRecordKey());
            strBuff.append("[updGroupConsumeCtrlConf], ").append(entity.getModifyUser())
                    .append(" updated record from :").append(curEntity)
                    .append(" to ").append(newEntity);
            logger.info(strBuff.toString());
        } else {
            strBuff.append("[updGroupConsumeCtrlConf], ")
                    .append("failure to update group consume control record : ")
                    .append(result.getErrMsg());
            logger.warn(strBuff.toString());
        }
        strBuff.delete(0, strBuff.length());
        return result.isSuccess();
    }

    @Override
    public boolean delGroupConsumeCtrlConf(String operator, String groupName,
                                           String topicName, StringBuilder strBuff,
                                           ProcessResult result) {
        // check current status
        if (groupName == null && topicName == null) {
            result.setSuccResult(null);
            return result.isSuccess();
        }
        if (!checkStoreStatus(true, result)) {
            return result.isSuccess();
        }
        if (consumeCtrlMapper.delGroupConsumeCtrlConf(groupName,
                topicName, strBuff, result)) {
            strBuff.append("[delGroupConsumeCtrlConf], ").append(operator)
                    .append(" deleted group consume control record by index : ")
                    .append("groupName=").append(groupName)
                    .append(", topicName=").append(topicName);
            logger.info(strBuff.toString());
        } else {
            strBuff.append("[delGroupConsumeCtrlConf], ")
                    .append("failure to delete group consume control record : ")
                    .append(result.getErrMsg());
            logger.warn(strBuff.toString());
        }
        strBuff.delete(0, strBuff.length());
        return result.isSuccess();
    }

    @Override
    public boolean delGroupConsumeCtrlConf(String operator, String recordKey,
                                           StringBuilder strBuff, ProcessResult result) {
        if (recordKey == null) {
            result.setSuccResult(null);
            return result.isSuccess();
        }
        // check current status
        if (!checkStoreStatus(true, result)) {
            return result.isSuccess();
        }
        if (consumeCtrlMapper.delGroupConsumeCtrlConf(recordKey, strBuff, result)) {
            strBuff.append("[delGroupConsumeCtrlConf], ").append(operator)
                    .append(" deleted group consume control record by index : ")
                    .append("recordKey=").append(recordKey);
            logger.info(strBuff.toString());
        } else {
            strBuff.append("[delGroupConsumeCtrlConf], ")
                    .append("failure to delete group consume control record : ")
                    .append(result.getErrMsg());
            logger.warn(strBuff.toString());
        }
        strBuff.delete(0, strBuff.length());
        return result.isSuccess();
    }

    @Override
    public boolean isTopicNameInUsed(String topicName) {
        return consumeCtrlMapper.isTopicNameInUse(topicName);
    }

    @Override
    public boolean hasGroupConsumeCtrlConf(String groupName) {
        return consumeCtrlMapper.isGroupNameInUse(groupName);
    }

    @Override
    public GroupConsumeCtrlEntity getGroupConsumeCtrlConfByRecKey(String recordKey) {
        return consumeCtrlMapper.getGroupConsumeCtrlConfByRecKey(recordKey);
    }

    @Override
    public List<GroupConsumeCtrlEntity> getConsumeCtrlByTopicName(String topicName) {
        return consumeCtrlMapper.getConsumeCtrlByTopicName(topicName);
    }

    @Override
    public List<GroupConsumeCtrlEntity> getConsumeCtrlByGroupName(String groupName) {
        return consumeCtrlMapper.getConsumeCtrlByGroupName(groupName);
    }

    @Override
    public GroupConsumeCtrlEntity getConsumeCtrlByGroupAndTopic(
            String groupName, String topicName) {
        return consumeCtrlMapper.getConsumeCtrlByGroupAndTopic(groupName, topicName);
    }

    @Override
    public Map<String/* group */, List<GroupConsumeCtrlEntity>> getConsumeCtrlInfoMap(
            Set<String> groupSet, Set<String> topicSet, GroupConsumeCtrlEntity qryEntry) {
        return consumeCtrlMapper.getConsumeCtrlInfoMap(groupSet, topicSet, qryEntry);
    }

    @Override
    public List<GroupConsumeCtrlEntity> getGroupConsumeCtrlConf(GroupConsumeCtrlEntity qryEntity) {
        return consumeCtrlMapper.getGroupConsumeCtrlConf(qryEntity);
    }

    @Override
    public Set<String> getMatchedKeysByGroupAndTopicSet(Set<String> groupSet,
                                                        Set<String> topicSet) {
        return consumeCtrlMapper.getMatchedRecords(groupSet, topicSet);
    }

    @Override
    public void registerObserver(MetaConfigObserver eventObserver) {
        if (eventObserver != null) {
            eventObservers.add(eventObserver);
        }
    }

    @Override
    public boolean isMasterNow() {
        return isMaster;
    }

    /**
     * Get master start time
     *
     * @return the since time
     */
    @Override
    public long getMasterSinceTime() {
        return this.masterSinceTime.get();
    }

    /**
     * Check if primary node is active
     *
     * @return whether is active
     */
    @Override
    public boolean isPrimaryNodeActive() {
        if (repEnv == null) {
            return false;
        }
        ReplicationMutableConfig tmpConfig = repEnv.getRepMutableConfig();
        return tmpConfig != null && tmpConfig.getDesignatedPrimary();
    }

    /**
     * Transfer master role to other replica node
     *
     * @throws Exception the exception information
     */
    @Override
    public void transferMaster() throws Exception {
        if (!isStarted()) {
            throw new Exception("The BDB store StoreService is reboot now!");
        }
        if (isMasterNow()) {
            if (!isPrimaryNodeActive()) {
                if ((replicas4Transfer != null) && (!replicas4Transfer.isEmpty())) {
                    logger.info(new StringBuilder(TBaseConstants.BUILDER_DEFAULT_SIZE)
                            .append("[BDB Impl] start transferMaster to replicas: ")
                            .append(replicas4Transfer).toString());
                    repEnv.transferMaster(replicas4Transfer, 5, TimeUnit.MINUTES);
                    logger.info("[BDB Impl] transferMaster end...");
                } else {
                    throw new Exception("The replicate nodes is empty!");
                }
            } else {
                throw new Exception("DesignatedPrimary happened...please check if the other member is down!");
            }
        } else {
            throw new Exception("Please send your request to the master Node!");
        }
    }

    /**
     * Get current master address
     *
     * @return  the current master address
     */
    @Override
    public String getMasterAddress() {
        ReplicationGroup replicationGroup = getCurrReplicationGroup();
        if (replicationGroup == null) {
            logger.info("[BDB Impl] ReplicationGroup is null...please check the group status!");
            return null;
        }
        for (ReplicationNode node : replicationGroup.getNodes()) {
            try {
                NodeState nodeState =
                        replicationGroupAdmin.getNodeState(node, 2000);
                if (nodeState != null) {
                    if (nodeState.getNodeState().isMaster()) {
                        return node.getSocketAddress().getAddress().getHostAddress();
                    }
                }
            } catch (Throwable e) {
                logger.error("[BDB Impl] Get nodeState Throwable error", e);
            }
        }
        return null;
    }

    /**
     * Get group address info
     *
     * @return  the group address information
     */
    @Override
    public ClusterGroupVO getGroupAddressStrInfo() {
        ClusterGroupVO clusterGroupVO = new ClusterGroupVO();
        clusterGroupVO.setGroupStatus("Abnormal");
        clusterGroupVO.setGroupName(replicationGroupAdmin.getGroupName());
        // query current replication group info
        ReplicationGroup replicationGroup = getCurrReplicationGroup();
        if (replicationGroup == null) {
            return clusterGroupVO;
        }
        // translate replication group info to ClusterGroupVO structure
        Tuple2<Boolean, List<ClusterNodeVO>>  transResult =
                transReplicateNodes(replicationGroup);
        clusterGroupVO.setNodeData(transResult.getF1());
        clusterGroupVO.setPrimaryNodeActive(isPrimaryNodeActive());
        if (transResult.getF0()) {
            if (isPrimaryNodeActive()) {
                clusterGroupVO.setGroupStatus("Running-ReadOnly");
            } else {
                clusterGroupVO.setGroupStatus("Running-ReadWrite");
            }
        }
        return clusterGroupVO;
    }

    /**
     * Get master group status
     *
     * @param isFromHeartbeat   whether called by hb thread
     * @return    the master group status
     */
    @Override
    public MasterGroupStatus getMasterGroupStatus(boolean isFromHeartbeat) {
        // #lizard forgives
        if (repEnv == null) {
            return null;
        }
        ReplicationGroup replicationGroup = null;
        try {
            replicationGroup = repEnv.getGroup();
        } catch (DatabaseException e) {
            if (e instanceof EnvironmentFailureException) {
                if (isFromHeartbeat) {
                    logger.error("[BDB Error] Check found EnvironmentFailureException", e);
                    try {
                        stop();
                        start();
                        replicationGroup = repEnv.getGroup();
                    } catch (Throwable e1) {
                        logger.error("[BDB Error] close and reopen storeManager error", e1);
                    }
                } else {
                    logger.error(
                            "[BDB Error] Get EnvironmentFailureException error while non heartBeat request", e);
                }
            } else {
                logger.error("[BDB Error] Get replication group info error", e);
            }
        } catch (Throwable ee) {
            logger.error("[BDB Error] Get replication group throw error", ee);
        }
        if (replicationGroup == null) {
            logger.error(
                    "[BDB Error] ReplicationGroup is null...please check the status of the group!");
            return null;
        }
        int activeNodes = 0;
        boolean isMasterActive = false;
        Set<String> tmp = new HashSet<>();
        for (ReplicationNode node : replicationGroup.getNodes()) {
            MasterNodeInfo masterNodeInfo =
                    new MasterNodeInfo(replicationGroup.getName(),
                            node.getName(), node.getHostName(), node.getPort());
            try {
                NodeState nodeState = replicationGroupAdmin.getNodeState(node, 2000);
                if (nodeState != null) {
                    if (nodeState.getNodeState().isActive()) {
                        activeNodes++;
                        if (nodeState.getNodeName().equals(masterNodeName)) {
                            isMasterActive = true;
                            masterNodeInfo.setNodeStatus(1);
                        }
                    }
                    if (nodeState.getNodeState().isReplica()) {
                        tmp.add(nodeState.getNodeName());
                        replicas4Transfer = tmp;
                        masterNodeInfo.setNodeStatus(0);
                    }
                }
            } catch (IOException e) {
                connectNodeFailCount++;
                masterNodeInfo.setNodeStatus(-1);
                metaConfigSamplePrint.printExceptionCaught(e, node.getHostName(), node.getName());
                continue;
            } catch (ServiceDispatcher.ServiceConnectFailedException e) {
                masterNodeInfo.setNodeStatus(-2);
                metaConfigSamplePrint.printExceptionCaught(e, node.getHostName(), node.getName());
                continue;
            } catch (Throwable ee) {
                masterNodeInfo.setNodeStatus(-3);
                metaConfigSamplePrint.printExceptionCaught(ee, node.getHostName(), node.getName());
                continue;
            }
        }
        MasterGroupStatus masterGroupStatus = new MasterGroupStatus(isMasterActive);
        int groupSize = replicationGroup.getElectableNodes().size();
        int majoritySize = groupSize / 2 + 1;
        if ((activeNodes >= majoritySize) && isMasterActive) {
            masterGroupStatus.setMasterGroupStatus(true, true, true);
            connectNodeFailCount = 0;
            if (isPrimaryNodeActive()) {
                repEnv.setRepMutableConfig(repEnv.getRepMutableConfig().setDesignatedPrimary(false));
            }
        }
        if (groupSize == 2 && connectNodeFailCount >= 3) {
            masterGroupStatus.setMasterGroupStatus(true, false, true);
            if (connectNodeFailCount > 1000) {
                connectNodeFailCount = 3;
            }
            if (!isPrimaryNodeActive()) {
                logger.error("[BDB Error] DesignatedPrimary happened...please check if the other member is down");
                repEnv.setRepMutableConfig(repEnv.getRepMutableConfig().setDesignatedPrimary(true));
            }
        }
        return masterGroupStatus;
    }

    /**
     * State Change Listener,
     * through this object, it complete the metadata cache cleaning
     * and loading of the latest data.
     *
     * */
    public class Listener implements StateChangeListener {
        @Override
        public void stateChange(StateChangeEvent stateChangeEvent) throws RuntimeException {
            if (repConfig != null) {
                logger.warn(new StringBuilder(TBaseConstants.BUILDER_DEFAULT_SIZE)
                        .append("[BDB Impl][").append(repConfig.getGroupName())
                        .append("Receive a group status changed event]... stateChangeEventTime: ")
                        .append(stateChangeEvent.getEventTime()).toString());
            }
            doWork(stateChangeEvent);
        }

        /**
         * process replicate nodes status event
         *
         * @param stateChangeEvent status change event
         */
        public void doWork(final StateChangeEvent stateChangeEvent) {

            final String currentNode = new StringBuilder(TBaseConstants.BUILDER_DEFAULT_SIZE)
                    .append("GroupName:").append(repConfig.getGroupName())
                    .append(",nodeName:").append(repConfig.getNodeName())
                    .append(",hostName:").append(repConfig.getNodeHostPort()).toString();
            if (executorService == null) {
                logger.error("[BDB Impl] found  executorService is null while doWork!");
                return;
            }
            executorService.submit(() -> {
                StringBuilder sBuilder =
                        new StringBuilder(TBaseConstants.BUILDER_DEFAULT_SIZE);
                switch (stateChangeEvent.getState()) {
                    case MASTER:
                        if (!isMaster) {
                            try {
                                reloadMetaStore(sBuilder);
                                isMaster = true;
                                masterSinceTime.set(System.currentTimeMillis());
                                masterNodeName = stateChangeEvent.getMasterNodeName();
                                logger.info(sBuilder.append("[BDB Impl] ")
                                        .append(currentNode)
                                        .append(" is a master.").toString());
                            } catch (Throwable e) {
                                isMaster = false;
                                logger.error("[BDB Impl] fatal error when Reloading Info ", e);
                            }
                        }
                        break;
                    case REPLICA:
                        isMaster = false;
                        masterNodeName = stateChangeEvent.getMasterNodeName();
                        logger.info(sBuilder.append("[BDB Impl] ")
                                .append(currentNode).append(" is a slave.").toString());
                        break;
                    default:
                        isMaster = false;
                        logger.info(sBuilder.append("[BDB Impl] ")
                                .append(currentNode).append(" is Unknown state ")
                                .append(stateChangeEvent.getState().name()).toString());
                        break;
                }
                sBuilder.delete(0, sBuilder.length());
            });
        }
    }

    private boolean isStarted() {
        return (this.srvStatus.get() == 2);
    }

    /**
     * clear cached data in alive event observers.
     *
     * */
    private void clearCachedRunData() {
        for (MetaConfigObserver observer : eventObservers) {
            observer.clearCacheData();
        }
    }

    /**
     * clear cached data in alive event observers.
     *
     * */
    private void reloadRunData() {
        for (MetaConfigObserver observer : eventObservers) {
            observer.reloadCacheData();
        }
    }

    /**
     * Initialize configuration for BDB-JE replication environment.
     *
     * */
    private void initEnvConfig() {

        // Set envHome and generate a ReplicationConfig. Note that ReplicationConfig and
        // EnvironmentConfig values could all be specified in the je.properties file,
        // as is shown in the properties file included in the example.
        repConfig = new ReplicationConfig();
        // Set consistency policy for replica.
        TimeConsistencyPolicy consistencyPolicy =
                new TimeConsistencyPolicy(3,
                        TimeUnit.SECONDS, 3, TimeUnit.SECONDS);
        repConfig.setConsistencyPolicy(consistencyPolicy);
        // Wait up to 3 seconds for commitConsumed acknowledgments.
        repConfig.setReplicaAckTimeout(3, TimeUnit.SECONDS);
        repConfig.setConfigParam(ReplicationConfig.TXN_ROLLBACK_LIMIT, "1000");
        repConfig.setGroupName(replicationConfig.getRepGroupName());
        repConfig.setNodeName(replicationConfig.getRepNodeName());
        repConfig.setNodeHostPort(this.nodeHost + TokenConstants.ATTR_SEP
                + replicationConfig.getRepNodePort());
        if (TStringUtils.isNotEmpty(replicationConfig.getRepHelperHost())) {
            logger.info("[BDB Impl] ADD HELP HOST");
            repConfig.setHelperHosts(replicationConfig.getRepHelperHost());
        }

        // A replicated environment must be opened with transactions enabled.
        // Environments on a master must be read/write, while environments
        // on a client can be read/write or read/only. Since the master's
        // identity may change, it's most convenient to open the environment in the default
        // read/write mode. All write operations will be refused on the client though.
        envConfig = new EnvironmentConfig();
        envConfig.setTransactional(true);
        Durability durability =
                new Durability(replicationConfig.getMetaLocalSyncPolicy(),
                        replicationConfig.getMetaReplicaSyncPolicy(),
                        replicationConfig.getRepReplicaAckPolicy());
        envConfig.setDurability(durability);
        envConfig.setAllowCreate(true);
        // build envHome file
        this.envHome = new File(metaDataPath);
        // An Entity Store in a replicated environment must be transactional.
        storeConfig.setTransactional(true);
        // Note that both Master and Replica open the store for write.
        storeConfig.setReadOnly(false);
        storeConfig.setAllowCreate(true);
    }

    /**
     * Creates the replicated environment handle and returns it. It will retry indefinitely if a
     * master could not be established because a sufficient number of nodes were not available, or
     * there were networking issues, etc.
     *
     * @return the newly created replicated environment handle
     * @throws InterruptedException if the operation was interrupted
     */
    private ReplicatedEnvironment getEnvironment() throws InterruptedException {
        DatabaseException exception = null;
        //In this example we retry REP_HANDLE_RETRY_MAX times, but a production HA application may
        //retry indefinitely.
        for (int i = 0; i < REP_HANDLE_RETRY_MAX; i++) {
            try {
                return new ReplicatedEnvironment(envHome, repConfig, envConfig);
            } catch (UnknownMasterException unknownMaster) {
                exception = unknownMaster;
                // Indicates there is a group level problem: insufficient nodes for an election,
                // network connectivity issues, etc. Wait and retry to allow the problem
                // to be resolved.
                logger.error(new StringBuilder(TBaseConstants.BUILDER_DEFAULT_SIZE)
                        .append("[BDB Impl] master could not be established. ")
                        .append("Exception message:").append(unknownMaster.getMessage())
                        .append(" Will retry after 5 seconds.").toString());
                Thread.sleep(5 * 1000);
                continue;
            } catch (InsufficientLogException insufficientLogEx) {
                logger.error(new StringBuilder(TBaseConstants.BUILDER_DEFAULT_SIZE)
                        .append("[BDB Impl] [Restoring data please wait....] ")
                        .append("Obtains logger files for a Replica from other members of ")
                        .append("the replication group. A Replica may need to do so if it ")
                        .append("has been offline for some time, and has fallen behind in ")
                        .append("its execution of the replication stream.").toString());
                NetworkRestore restore = new NetworkRestore();
                NetworkRestoreConfig config = new NetworkRestoreConfig();
                // delete obsolete logger files.
                config.setRetainLogFiles(false);
                restore.execute(insufficientLogEx, config);
                // retry
                return new ReplicatedEnvironment(envHome, repConfig, envConfig);
            }
        }
        // Failed despite retries.
        throw exception;
    }

    /* initial metadata */
    private void initMetaStore() {
        clusterConfigMapper = new BdbClusterConfigMapperImpl(repEnv, storeConfig);
        brokerConfigMapper = new BdbBrokerConfigMapperImpl(repEnv, storeConfig);
        topicDeployMapper =  new BdbTopicDeployMapperImpl(repEnv, storeConfig);
        groupResCtrlMapper = new BdbGroupResCtrlMapperImpl(repEnv, storeConfig);
        topicCtrlMapper = new BdbTopicCtrlMapperImpl(repEnv, storeConfig);
        consumeCtrlMapper = new BdbConsumeCtrlMapperImpl(repEnv, storeConfig);
    }

    /* reload metadata */
    private void reloadMetaStore(StringBuilder strBuff) {
        clearCachedRunData();
        clusterConfigMapper.loadConfig(strBuff);
        brokerConfigMapper.loadConfig(strBuff);
        topicDeployMapper.loadConfig(strBuff);
        topicCtrlMapper.loadConfig(strBuff);
        groupResCtrlMapper.loadConfig(strBuff);
        consumeCtrlMapper.loadConfig(strBuff);
        reloadRunData();
    }

    private ReplicationGroup getCurrReplicationGroup() {
        ReplicationGroup replicationGroup;
        try {
            replicationGroup = repEnv.getGroup();
        } catch (Throwable e) {
            logger.error("[BDB Impl] get current master group info error", e);
            return null;
        }
        return replicationGroup;
    }

    /**
     * Query replication group nodes status and translate to ClusterNodeVO type
     *
     * @param replicationGroup  the replication group
     * @return if has master, replication nodes info
     */
    private Tuple2<Boolean, List<ClusterNodeVO>> transReplicateNodes(
            ReplicationGroup replicationGroup) {
        boolean hasMaster = false;
        List<ClusterNodeVO> clusterNodeVOList = new ArrayList<>();
        for (ReplicationNode node : replicationGroup.getNodes()) {
            ClusterNodeVO clusterNodeVO = new ClusterNodeVO();
            clusterNodeVO.setHostName(node.getHostName());
            clusterNodeVO.setNodeName(node.getName());
            clusterNodeVO.setPort(node.getPort());
            try {
                NodeState nodeState =
                        replicationGroupAdmin.getNodeState(node, 2000);
                if (nodeState != null) {
                    if (nodeState.getNodeState() == ReplicatedEnvironment.State.MASTER) {
                        hasMaster = true;
                    }
                    clusterNodeVO.setNodeStatus(nodeState.getNodeState().toString());
                    clusterNodeVO.setJoinTime(nodeState.getJoinTime());
                } else {
                    clusterNodeVO.setNodeStatus("Not-found");
                    clusterNodeVO.setJoinTime(0);
                }
            } catch (IOException e) {
                clusterNodeVO.setNodeStatus("Error");
                clusterNodeVO.setJoinTime(0);
            } catch (ServiceDispatcher.ServiceConnectFailedException e) {
                clusterNodeVO.setNodeStatus("Unconnected");
                clusterNodeVO.setJoinTime(0);
            }
            clusterNodeVOList.add(clusterNodeVO);
        }
        return new Tuple2<>(hasMaster, clusterNodeVOList);
    }

    @Override
    public boolean checkStoreStatus(boolean checkIsMaster, ProcessResult result) {
        if (!isStarted()) {
            result.setFailResult(DataOpErrCode.DERR_STORE_STOPPED.getCode(),
                    "Meta store service stopped!");
            return result.isSuccess();
        }
        if (checkIsMaster && !isMasterNow()) {
            result.setFailResult(DataOpErrCode.DERR_STORE_NOT_MASTER.getCode(),
                    "Current node not active, please send your request to the active Node!");
            return result.isSuccess();
        }
        result.setSuccResult(null);
        return true;
    }
}
