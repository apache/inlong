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

package org.apache.tubemq.server.master.metastore.impl.bdbimpl;

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

import org.apache.tubemq.corebase.TBaseConstants;
import org.apache.tubemq.corebase.TokenConstants;
import org.apache.tubemq.corebase.utils.TStringUtils;
import org.apache.tubemq.corebase.utils.Tuple2;
import org.apache.tubemq.server.common.fileconfig.MasterReplicationConfig;
import org.apache.tubemq.server.common.utils.ProcessResult;
import org.apache.tubemq.server.master.bdbstore.MasterGroupStatus;
import org.apache.tubemq.server.master.bdbstore.MasterNodeInfo;
import org.apache.tubemq.server.master.metastore.DataOpErrCode;
import org.apache.tubemq.server.master.metastore.MetaStoreService;
import org.apache.tubemq.server.master.metastore.dao.entity.BrokerConfEntity;
import org.apache.tubemq.server.master.metastore.dao.entity.ClusterSettingEntity;
import org.apache.tubemq.server.master.metastore.dao.entity.GroupBlackListEntity;
import org.apache.tubemq.server.master.metastore.dao.entity.GroupConfigEntity;
import org.apache.tubemq.server.master.metastore.dao.entity.GroupFilterCtrlEntity;
import org.apache.tubemq.server.master.metastore.dao.entity.TopicConfEntity;
import org.apache.tubemq.server.master.metastore.dao.entity.TopicCtrlEntity;
import org.apache.tubemq.server.master.metastore.dao.mapper.BrokerConfigMapper;
import org.apache.tubemq.server.master.metastore.dao.mapper.ClusterConfigMapper;
import org.apache.tubemq.server.master.metastore.dao.mapper.GroupBlackListMapper;
import org.apache.tubemq.server.master.metastore.dao.mapper.GroupConfigMapper;
import org.apache.tubemq.server.master.metastore.dao.mapper.GroupFilterCtrlMapper;
import org.apache.tubemq.server.master.metastore.dao.mapper.TopicConfigMapper;
import org.apache.tubemq.server.master.metastore.dao.mapper.TopicCtrlMapper;
import org.apache.tubemq.server.master.metastore.keepalive.AliveObserver;
import org.apache.tubemq.server.master.utils.BdbStoreSamplePrint;
import org.apache.tubemq.server.master.web.model.ClusterGroupVO;
import org.apache.tubemq.server.master.web.model.ClusterNodeVO;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class BdbMetaStoreServiceImpl implements MetaStoreService {

    private static final int REP_HANDLE_RETRY_MAX = 1;

    private static final Logger logger =
            LoggerFactory.getLogger(BdbMetaStoreServiceImpl.class);
    private final BdbStoreSamplePrint bdbStoreSamplePrint =
            new BdbStoreSamplePrint(logger);
    // parameters need input
    // local host name
    private final String nodeHost;
    // meta data store path
    private final String metaDataPath;
    // bdb replication configure
    private final MasterReplicationConfig replicationConfig;
    private Listener listener = new Listener();
    private ExecutorService executorService = null;
    private List<AliveObserver> eventObservers = new ArrayList<>();
    // service status
    // 0 stopped, 1 starting, 2 started, 3 stopping
    private AtomicInteger srvStatus = new AtomicInteger(0);
    // master role flag
    private volatile boolean isMaster = false;
    // time since node become active
    private AtomicLong masterSinceTime = new AtomicLong(Long.MAX_VALUE);
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
    private StoreConfig storeConfig = new StoreConfig();
    // bdb replication group admin info
    private ReplicationGroupAdmin replicationGroupAdmin;

    // cluster default setting
    private ClusterConfigMapper clusterConfigMapper;
    // broker configure
    private BrokerConfigMapper brokerConfigMapper;
    // topic configure
    private TopicConfigMapper topicConfigMapper;
    // topic control configure
    private TopicCtrlMapper topicCtrlMapper;
    // group configure
    private GroupConfigMapper groupConfigMapper;
    // group filter configure
    private GroupFilterCtrlMapper groupFilterCtrlMapper;
    // group blackList configure
    private GroupBlackListMapper groupBlackListMapper;




    public BdbMetaStoreServiceImpl(String nodeHost, String metaDataPath,
                                   MasterReplicationConfig replicationConfig) {
        this.nodeHost = nodeHost;
        this.metaDataPath = metaDataPath;
        this.replicationConfig = replicationConfig;
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
        topicConfigMapper.close();
        groupConfigMapper.close();
        topicCtrlMapper.close();
        groupBlackListMapper.close();
        groupFilterCtrlMapper.close();
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
    public boolean addClusterConfig(ClusterSettingEntity memEntity, ProcessResult result) {
        // check current status
        if (!checkStoreStatus(true, result)) {
            return result.isSuccess();
        }
        return clusterConfigMapper.addClusterConfig(memEntity, result);
    }

    @Override
    public boolean updClusterConfig(ClusterSettingEntity memEntity, ProcessResult result) {
        // check current status
        if (!checkStoreStatus(true, result)) {
            return result.isSuccess();
        }
        return clusterConfigMapper.updClusterConfig(memEntity, result);
    }

    @Override
    public ClusterSettingEntity getClusterConfig() {
        return clusterConfigMapper.getClusterConfig();
    }

    @Override
    public boolean delClusterConfig(ProcessResult result) {
        if (!checkStoreStatus(true, result)) {
            return result.isSuccess();
        }
        return clusterConfigMapper.delClusterConfig();
    }

    // broker configure api
    @Override
    public boolean addBrokerConf(BrokerConfEntity memEntity, ProcessResult result) {
        // check current status
        if (!checkStoreStatus(true, result)) {
            return result.isSuccess();
        }
        return brokerConfigMapper.addBrokerConf(memEntity, result);
    }

    @Override
    public boolean updBrokerConf(BrokerConfEntity memEntity, ProcessResult result) {
        // check current status
        if (!checkStoreStatus(true, result)) {
            return result.isSuccess();
        }
        return brokerConfigMapper.updBrokerConf(memEntity, result);
    }

    @Override
    public boolean delBrokerConf(int brokerId, ProcessResult result) {
        // check current status
        if (!checkStoreStatus(true, result)) {
            return result.isSuccess();
        }
        return brokerConfigMapper.delBrokerConf(brokerId);
    }

    @Override
    public Map<Integer, BrokerConfEntity> getBrokerConfByBrokerId(BrokerConfEntity qryEntity) {
        return brokerConfigMapper.getBrokerConfByBrokerId(qryEntity);
    }

    @Override
    public BrokerConfEntity getBrokerConfByBrokerId(int brokerId) {
        return brokerConfigMapper.getBrokerConfByBrokerId(brokerId);
    }

    // topic configure api
    @Override
    public boolean addTopicConf(TopicConfEntity entity, ProcessResult result) {
        // check current status
        if (!checkStoreStatus(true, result)) {
            return result.isSuccess();
        }
        return topicConfigMapper.addTopicConf(entity, result);
    }

    @Override
    public boolean updTopicConf(TopicConfEntity entity, ProcessResult result) {
        // check current status
        if (!checkStoreStatus(true, result)) {
            return result.isSuccess();
        }
        return topicConfigMapper.updTopicConf(entity, result);
    }

    @Override
    public boolean delTopicConf(String recordKey, ProcessResult result) {
        // check current status
        if (!checkStoreStatus(true, result)) {
            return result.isSuccess();
        }
        return topicConfigMapper.delTopicConf(recordKey);

    }

    @Override
    public List<TopicConfEntity> getTopicConf(TopicConfEntity qryEntity) {
        return topicConfigMapper.getTopicConf(qryEntity);
    }

    // topic control api
    @Override
    public boolean addTopicCtrlConf(TopicCtrlEntity entity, ProcessResult result) {
        // check current status
        if (!checkStoreStatus(true, result)) {
            return result.isSuccess();
        }
        return topicCtrlMapper.addTopicCtrlConf(entity, result);
    }

    @Override
    public boolean updTopicCtrlConf(TopicCtrlEntity entity, ProcessResult result) {
        // check current status
        if (!checkStoreStatus(true, result)) {
            return result.isSuccess();
        }
        return topicCtrlMapper.updTopicCtrlConf(entity, result);
    }

    @Override
    public boolean delTopicCtrlConf(String recordKey, ProcessResult result) {
        // check current status
        if (!checkStoreStatus(true, result)) {
            return result.isSuccess();
        }
        return topicCtrlMapper.delTopicCtrlConf(recordKey);
    }

    @Override
    public TopicCtrlEntity getTopicCtrlConf(String topicName) {
        return topicCtrlMapper.getTopicCtrlConf(topicName);
    }

    @Override
    public List<TopicCtrlEntity> getTopicCtrlConf(TopicConfEntity qryEntity) {
        return topicCtrlMapper.getTopicCtrlConf(qryEntity);
    }

    // group configure api
    @Override
    public boolean addGroupConf(GroupConfigEntity entity, ProcessResult result) {
        // check current status
        if (!checkStoreStatus(true, result)) {
            return result.isSuccess();
        }
        return groupConfigMapper.addGroupConf(entity, result);
    }

    @Override
    public boolean updGroupConf(GroupConfigEntity entity, ProcessResult result) {
        // check current status
        if (!checkStoreStatus(true, result)) {
            return result.isSuccess();
        }
        return groupConfigMapper.updGroupConf(entity, result);
    }

    @Override
    public boolean delGroupConf(String groupName, ProcessResult result) {
        // check current status
        if (!checkStoreStatus(true, result)) {
            return result.isSuccess();
        }
        return groupConfigMapper.delGroupConf(groupName);
    }

    @Override
    public GroupConfigEntity getGroupConf(String groupName) {
        return groupConfigMapper.getGroupConf(groupName);
    }

    @Override
    public Map<String, GroupConfigEntity> getGroupConf(GroupConfigEntity qryEntity) {
        return groupConfigMapper.getGroupConf(qryEntity);
    }

    // group blacklist api
    @Override
    public boolean addGroupBlackListConf(GroupBlackListEntity entity, ProcessResult result) {
        // check current status
        if (!checkStoreStatus(true, result)) {
            return result.isSuccess();
        }
        return groupBlackListMapper.addGroupBlackListConf(entity, result);
    }

    @Override
    public boolean updGroupBlackListConf(GroupBlackListEntity entity, ProcessResult result) {
        // check current status
        if (!checkStoreStatus(true, result)) {
            return result.isSuccess();
        }
        return groupBlackListMapper.updGroupBlackListConf(entity, result);
    }

    @Override
    public boolean delGroupBlackListConf(String recordKey, ProcessResult result) {
        // check current status
        if (!checkStoreStatus(true, result)) {
            return result.isSuccess();
        }
        return groupBlackListMapper.delGroupBlackListConf(recordKey);
    }

    @Override
    public boolean delGroupBlackListConfByGroupName(String groupName, ProcessResult result) {
        // check current status
        if (!checkStoreStatus(true, result)) {
            return result.isSuccess();
        }
        return groupBlackListMapper.delGroupBlackListConf(groupName);
    }

    @Override
    public  List<GroupBlackListEntity> getGrpBlkLstConfByGroupName(String groupName) {
        return groupBlackListMapper.getGrpBlkLstConfByGroupName(groupName);
    }

    @Override
    public List<GroupBlackListEntity> getGrpBlkLstConfByTopicName(String topicName) {
        return groupBlackListMapper.getGrpBlkLstConfByTopicName(topicName);
    }

    @Override
    public List<GroupBlackListEntity> getGroupBlackListConf(GroupBlackListEntity qryEntity) {
        return groupBlackListMapper.getGroupBlackListConf(qryEntity);
    }

    // group filter control api
    @Override
    public boolean addGroupFilterCtrlConf(GroupFilterCtrlEntity entity, ProcessResult result) {
        // check current status
        if (!checkStoreStatus(true, result)) {
            return result.isSuccess();
        }
        return groupFilterCtrlMapper.addGroupFilterCtrlConf(entity, result);
    }

    @Override
    public boolean updGroupFilterCtrlConf(GroupFilterCtrlEntity entity, ProcessResult result) {
        // check current status
        if (!checkStoreStatus(true, result)) {
            return result.isSuccess();
        }
        return groupFilterCtrlMapper.updGroupFilterCtrlConf(entity, result);
    }

    @Override
    public boolean delGroupFilterCtrlConf(String recordKey, ProcessResult result) {
        // check current status
        if (!checkStoreStatus(true, result)) {
            return result.isSuccess();
        }
        return groupFilterCtrlMapper.delGroupFilterCtrlConf(recordKey);
    }

    @Override
    public List<GroupFilterCtrlEntity> getGroupFilterCtrlConf(String groupName) {
        return groupFilterCtrlMapper.getGroupFilterCtrlConf(groupName);
    }

    @Override
    public List<GroupFilterCtrlEntity> getGroupFilterCtrlConf(GroupFilterCtrlEntity qryEntity) {
        return groupFilterCtrlMapper.getGroupFilterCtrlConf(qryEntity);
    }

    @Override
    public void registerObserver(AliveObserver eventObserver) {
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
     * @return
     */
    @Override
    public long getMasterSinceTime() {
        return this.masterSinceTime.get();
    }

    /**
     * Check if primary node is active
     *
     * @return
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
     * @throws Exception
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
     * @return
     */
    @Override
    public InetSocketAddress getMasterAddress() {
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
                        return node.getSocketAddress();
                    }
                }
            } catch (Throwable e) {
                logger.error("[BDB Impl] Get nodeState Throwable error", e);
                continue;
            }
        }
        return null;
    }

    /**
     * Get group address info
     *
     * @return
     */
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
        Tuple2<Boolean, List<ClusterNodeVO>>  transReult =
                transReplicateNodes(replicationGroup);
        clusterGroupVO.setNodeData(transReult.getF1());
        clusterGroupVO.setPrimaryNodeActive(isPrimaryNodeActive());
        if (transReult.getF0()) {
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
     * @param isFromHeartbeat
     * @return
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
                bdbStoreSamplePrint.printExceptionCaught(e, node.getHostName(), node.getName());
                continue;
            } catch (ServiceDispatcher.ServiceConnectFailedException e) {
                masterNodeInfo.setNodeStatus(-2);
                bdbStoreSamplePrint.printExceptionCaught(e, node.getHostName(), node.getName());
                continue;
            } catch (Throwable ee) {
                masterNodeInfo.setNodeStatus(-3);
                bdbStoreSamplePrint.printExceptionCaught(ee, node.getHostName(), node.getName());
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

        public void doWork(final StateChangeEvent stateChangeEvent) {

            final String currentNode = new StringBuilder(TBaseConstants.BUILDER_DEFAULT_SIZE)
                    .append("GroupName:").append(repConfig.getGroupName())
                    .append(",nodeName:").append(repConfig.getNodeName())
                    .append(",hostName:").append(repConfig.getNodeHostPort()).toString();
            if (executorService == null) {
                logger.error("[BDB Impl] found  executorService is null while doWork!");
                return;
            }
            executorService.submit(new Runnable() {
                @Override
                public void run() {
                    ProcessResult result = new ProcessResult();
                    StringBuilder sBuilder =
                            new StringBuilder(TBaseConstants.BUILDER_DEFAULT_SIZE);
                    switch (stateChangeEvent.getState()) {
                        case MASTER:
                            if (!isMaster) {
                                try {
                                    clearCachedRunData();
                                    clusterConfigMapper.loadConfig();
                                    brokerConfigMapper.loadConfig();
                                    topicConfigMapper.loadConfig();
                                    topicCtrlMapper.loadConfig();
                                    groupConfigMapper.loadConfig();
                                    groupBlackListMapper.loadConfig();
                                    groupFilterCtrlMapper.loadConfig();
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
                }
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
        for (AliveObserver observer : eventObservers) {
            observer.clearCacheData();
        }
    }

    /**
     * Initialize configuration for BDB-JE replication environment.
     *
     * */
    private void initEnvConfig() throws InterruptedException {

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
        if (exception != null) {
            throw exception;
        }
        // Don't expect to get here.
        throw new IllegalStateException("Failed despite retries");
    }

    /* initial metadata */
    private void initMetaStore() {
        clusterConfigMapper = new BdbClusterConfigMapperImpl(repEnv, storeConfig);
        brokerConfigMapper = new BdbBrokerConfigMapperImpl(repEnv, storeConfig);
        topicConfigMapper =  new BdbTopicConfigMapperImpl(repEnv, storeConfig);
        groupConfigMapper = new BdbGroupConfigMapperImpl(repEnv, storeConfig);
        topicCtrlMapper = new BdbTopicCtrlMapperImpl(repEnv, storeConfig);
        groupFilterCtrlMapper = new BdbGroupFilterCtrlMapperImpl(repEnv, storeConfig);
        groupBlackListMapper = new BdbGroupBlackListMapperImpl(repEnv, storeConfig);
    }

    private ReplicationGroup getCurrReplicationGroup() {
        ReplicationGroup replicationGroup = null;
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
     * @return if has master, replication nodes info
     * @throws InterruptedException if the operation was interrupted
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

    private boolean checkStoreStatus(boolean checkIsMaster, ProcessResult result) {
        if (!isStarted()) {
            result.setFailResult(DataOpErrCode.DERR_STORE_STOPPED.getCode(),
                    "Meta store service stopped!");
            return result.isSuccess();
        }
        if (checkIsMaster && !isMasterNow()) {
            result.setFailResult(DataOpErrCode.DERR_STORE_NOT_MASTER.getCode(),
                    "Current node not active!");
            return result.isSuccess();
        }
        result.setSuccResult(null);
        return true;
    }
}
