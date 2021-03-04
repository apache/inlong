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

package org.apache.tubemq.server.broker;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.log4j.LogManager;
import org.apache.tubemq.corebase.TErrCodeConstants;
import org.apache.tubemq.corebase.TokenConstants;
import org.apache.tubemq.corebase.aaaclient.ClientAuthenticateHandler;
import org.apache.tubemq.corebase.aaaclient.SimpleClientAuthenticateHandler;
import org.apache.tubemq.corebase.cluster.MasterInfo;
import org.apache.tubemq.corebase.policies.FlowCtrlRuleHandler;
import org.apache.tubemq.corebase.protobuf.generated.ClientMaster;
import org.apache.tubemq.corebase.protobuf.generated.ClientMaster.CloseRequestB2M;
import org.apache.tubemq.corebase.protobuf.generated.ClientMaster.HeartRequestB2M;
import org.apache.tubemq.corebase.protobuf.generated.ClientMaster.HeartResponseM2B;
import org.apache.tubemq.corebase.protobuf.generated.ClientMaster.RegisterRequestB2M;
import org.apache.tubemq.corebase.protobuf.generated.ClientMaster.RegisterResponseM2B;
import org.apache.tubemq.corebase.utils.ServiceStatusHolder;
import org.apache.tubemq.corebase.utils.ThreadUtils;
import org.apache.tubemq.corebase.utils.Tuple2;
import org.apache.tubemq.corerpc.RpcConfig;
import org.apache.tubemq.corerpc.RpcConstants;
import org.apache.tubemq.corerpc.RpcServiceFactory;
import org.apache.tubemq.corerpc.netty.NettyClientFactory;
import org.apache.tubemq.corerpc.service.MasterService;
import org.apache.tubemq.server.Stoppable;
import org.apache.tubemq.server.broker.exception.StartupException;
import org.apache.tubemq.server.broker.metadata.BrokerMetadataManager;
import org.apache.tubemq.server.broker.metadata.ClusterConfigHolder;
import org.apache.tubemq.server.broker.metadata.MetadataManager;
import org.apache.tubemq.server.broker.metadata.TopicMetadata;
import org.apache.tubemq.server.broker.msgstore.MessageStoreManager;
import org.apache.tubemq.server.broker.nodeinfo.ConsumerNodeInfo;
import org.apache.tubemq.server.broker.offset.DefaultOffsetManager;
import org.apache.tubemq.server.broker.offset.OffsetService;
import org.apache.tubemq.server.broker.offset.OffsetTimeManager;
import org.apache.tubemq.server.broker.utils.BrokerSamplePrint;
import org.apache.tubemq.server.broker.utils.GroupOffsetInfo;
import org.apache.tubemq.server.broker.utils.TopicPubStoreInfo;
import org.apache.tubemq.server.broker.web.WebServer;
import org.apache.tubemq.server.common.TubeServerVersion;
import org.apache.tubemq.server.common.aaaserver.SimpleCertificateBrokerHandler;
import org.apache.tubemq.server.common.fielddef.WebFieldDef;
import org.apache.tubemq.server.common.utils.ProcessResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/***
 * Tube broker server. In charge of init each components, and communicating to master.
 */
public class TubeBroker implements Stoppable {
    private static final Logger logger =
            LoggerFactory.getLogger(TubeBroker.class);
    // tube broker config
    private final BrokerConfig tubeConfig;
    // broker id
    private final String brokerId;
    private final NettyClientFactory clientFactory = new NettyClientFactory();
    private final RpcServiceFactory rpcServiceFactory;
    // tube web server
    private final WebServer webServer;
    // tube broker's metadata manager
    private final MetadataManager metadataManager;
    // tube broker's store manager
    private final MessageStoreManager storeManager;
    // tube broker's offset manager
    private final OffsetService offsetManager;
    private final BrokerServiceServer brokerServiceServer;
    private final OffsetTimeManager offsetTimeManager;
    private final BrokerSamplePrint samplePrintCtrl =
            new BrokerSamplePrint(logger);
    private final ScheduledExecutorService scheduledExecutorService;
    // shutdown hook.
    private final ShutdownHook shutdownHook = new ShutdownHook();
    // certificate handler.
    private final SimpleCertificateBrokerHandler serverAuthHandler;
    private final ClientAuthenticateHandler clientAuthHandler =
            new SimpleClientAuthenticateHandler();
    private MasterService masterService;
    private boolean requireReportConf = false;
    private boolean isOnline = false;
    private AtomicBoolean shutdown = new AtomicBoolean(true);
    private final AtomicBoolean isKeepAlive = new AtomicBoolean(false);
    private final AtomicLong lastRegTime = new AtomicLong(0);
    private AtomicBoolean shutdownHooked = new AtomicBoolean(false);
    private AtomicLong heartbeatErrors = new AtomicLong(0);
    private int maxReleaseTryCnt = 10;


    public TubeBroker(final BrokerConfig tubeConfig) throws Exception {
        java.security.Security.setProperty("networkaddress.cache.ttl", "3");
        java.security.Security.setProperty("networkaddress.cache.negative.ttl", "1");
        this.tubeConfig = tubeConfig;
        this.brokerId = generateBrokerClientId();
        this.metadataManager = new BrokerMetadataManager();
        this.offsetManager = new DefaultOffsetManager(tubeConfig);
        this.storeManager = new MessageStoreManager(this, tubeConfig);

        this.serverAuthHandler = new SimpleCertificateBrokerHandler(this);
        // rpc config.
        RpcConfig rpcConfig = new RpcConfig();
        rpcConfig.put(RpcConstants.CONNECT_TIMEOUT, 3000);
        rpcConfig.put(RpcConstants.REQUEST_TIMEOUT, this.tubeConfig.getRpcReadTimeoutMs());
        clientFactory.configure(rpcConfig);
        this.rpcServiceFactory =
                new RpcServiceFactory(clientFactory);
        // broker service.
        this.brokerServiceServer =
                new BrokerServiceServer(this, tubeConfig);
        logger.info("create offset time service");
        this.offsetTimeManager = new OffsetTimeManager(tubeConfig, this);
        // web server.
        this.webServer = new WebServer(tubeConfig.getHostName(), tubeConfig.getWebPort(), this);
        this.webServer.start();
        // used for communicate to master.
        this.masterService =
                rpcServiceFactory.getFailoverService(MasterService.class,
                        new MasterInfo(tubeConfig.getMasterAddressList()), rpcConfig);
        // used for heartbeat.
        this.scheduledExecutorService =
                Executors.newSingleThreadScheduledExecutor(new ThreadFactory() {
                    @Override
                    public Thread newThread(Runnable r) {
                        Thread t = new Thread(r, "Broker Heartbeat Thread");
                        t.setPriority(Thread.MAX_PRIORITY);
                        return t;
                    }
                });
        Runtime.getRuntime().addShutdownHook(this.shutdownHook);
    }

    public ConsumerNodeInfo getConsumerNodeInfo(String storeKey) {
        return this.brokerServiceServer.getConsumerNodeInfo(storeKey);
    }

    public OffsetService getOffsetManager() {
        return this.offsetManager;
    }

    public BrokerConfig getTubeConfig() {
        return tubeConfig;
    }

    public boolean isKeepAlive() {
        return  this.isKeepAlive.get();
    }

    public long getLastRegTime() {
        return this.lastRegTime.get();
    }

    public RpcServiceFactory getRpcServiceFactory() {
        return this.rpcServiceFactory;
    }

    public MetadataManager getMetadataManager() {
        return metadataManager;
    }

    public SimpleCertificateBrokerHandler getServerAuthHandler() {
        return serverAuthHandler;
    }

    @Override
    public boolean isStopped() {
        return this.shutdown.get();
    }

    public MessageStoreManager getStoreManager() {
        return this.storeManager;
    }

    public BrokerServiceServer getBrokerServiceServer() {
        return brokerServiceServer;
    }

    /***
     * Start broker service.
     *
     * @throws Exception
     */
    public void start() throws Exception {
        logger.info("Starting tube server...");
        if (!this.shutdown.get()) {
            return;
        }
        this.shutdown.set(false);
        // register to master, and heartbeat to master.
        this.register2Master();
        this.scheduledExecutorService.scheduleAtFixedRate(
            new Runnable() {
                @Override
                public void run() {
                    if (!shutdown.get()) {
                        long currErrCnt = heartbeatErrors.get();
                        if (currErrCnt > maxReleaseTryCnt) {
                            if (currErrCnt % 2 == 0) {
                                heartbeatErrors.incrementAndGet();
                                return;
                            }
                        }
                        try {
                            HeartResponseM2B response =
                                masterService.brokerHeartbeatB2M(createBrokerHeartBeatRequest(),
                                    tubeConfig.getHostName(), false);
                            if (!response.getSuccess()) {
                                isKeepAlive.set(false);
                                if (response.getErrCode() == TErrCodeConstants.HB_NO_NODE) {
                                    register2Master();
                                    heartbeatErrors.set(0);
                                    logger.info("Re-register to master successfully!");
                                }
                                return;
                            }
                            isKeepAlive.set(true);
                            heartbeatErrors.set(0);
                            StringBuilder sBuilder = new StringBuilder(512);
                            procConfigFromHeartBeat(sBuilder, response);
                        } catch (Throwable t) {
                            isKeepAlive.set(false);
                            heartbeatErrors.incrementAndGet();
                            samplePrintCtrl.printExceptionCaught(t);
                        }
                    }
                }
            }, tubeConfig.getHeartbeatPeriodMs(), tubeConfig.getHeartbeatPeriodMs(),
            TimeUnit.MILLISECONDS);
        this.storeManager.start();
        this.brokerServiceServer.start();
        isOnline = true;
        logger.info(new StringBuilder(512)
                .append("Start tube server successfully, broker version=")
                .append(TubeServerVersion.BROKER_VERSION).toString());
    }

    public synchronized void reloadConfig() {
        this.tubeConfig.reload();
    }

    public boolean isOnline() {
        return this.isOnline;
    }

    @Override
    public void stop(String why) {
        if (!shutdown.compareAndSet(false, true)) {
            return;
        }
        logger.info(why + ".Stopping Tube server...");
        try {
            TubeBroker.this.webServer.stop();
            logger.info("Tube WebService stopped.......");
            masterService.brokerCloseClientB2M(createMasterCloseRequest(),
                    tubeConfig.getHostName(), false);
            logger.info("Tube Closing to Master.....");
        } catch (Throwable e) {
            logger.warn("CloseBroker throw exception : ", e);
        }
        ThreadUtils.sleep(2000);
        logger.info("Tube Client StoreService stopping.....");
        TubeBroker.this.brokerServiceServer.stop();
        logger.info("Tube Client StoreService stopped.....");
        TubeBroker.this.storeManager.close();
        logger.info("Tube message store stopped.....");
        TubeBroker.this.offsetManager.close(-1);
        logger.info("Tube offset store stopped.....");
        scheduledExecutorService.shutdownNow();
        if (!shutdownHooked.get()) {
            Runtime.getRuntime().removeShutdownHook(TubeBroker.this.shutdownHook);
        }
        try {
            TubeBroker.this.rpcServiceFactory.destroy();
            TubeBroker.this.clientFactory.shutdown();
        } catch (Throwable e2) {
            logger.error("Stop rpcService failure", e2);
        }
        logger.info("Stop tube server successfully.");
        LogManager.shutdown();
        ThreadUtils.sleep(2000);
    }

    private String generateBrokerClientId() {
        return new StringBuilder(512).append(tubeConfig.getBrokerId()).append(":")
                .append(tubeConfig.getHostName()).append(":")
                .append(tubeConfig.getPort()).append(":")
                .append(TubeServerVersion.BROKER_VERSION).toString();
    }

    private void procConfigFromHeartBeat(StringBuilder sBuilder,
                                         HeartResponseM2B response) {
        // process service status
        ServiceStatusHolder
                .setReadWriteServiceStatus(response.getStopRead(),
                        response.getStopWrite(), "Master");
        // process flow controller rules
        FlowCtrlRuleHandler flowCtrlRuleHandler =
                metadataManager.getFlowCtrlRuleHandler();
        long flowCheckId = flowCtrlRuleHandler.getFlowCtrlId();
        int qryPriorityId = flowCtrlRuleHandler.getQryPriorityId();
        if (response.hasFlowCheckId()) {
            qryPriorityId = response.hasQryPriorityId()
                    ? response.getQryPriorityId() : qryPriorityId;
            if (response.getFlowCheckId() != flowCheckId) {
                flowCheckId = response.getFlowCheckId();
                try {
                    flowCtrlRuleHandler
                            .updateDefFlowCtrlInfo(qryPriorityId,
                                    flowCheckId, response.getFlowControlInfo());
                } catch (Exception e1) {
                    logger.warn(
                            "[HeartBeat response] found parse flowCtrl rules failure", e1);
                }
            }
            if (qryPriorityId != flowCtrlRuleHandler.getQryPriorityId()) {
                flowCtrlRuleHandler.setQryPriorityId(qryPriorityId);
            }
        }
        // update configure report requirement
        requireReportConf = response.getNeedReportData();
        // update cluster setting
        if (response.hasClsConfig()) {
            long configId = response.getClsConfig().getConfigId();
            if (configId != ClusterConfigHolder.getConfigId()) {
                ClusterConfigHolder.updClusterSetting(response.getClsConfig());
                logger.info(sBuilder
                        .append("[HeartBeat response] received cluster configure changed,")
                        .append(",hasClsConfig=").append(response.hasClsConfig())
                        .append(",curClusterConfigId=").append(ClusterConfigHolder.getConfigId())
                        .append(",curMaxMsgSize=").append(ClusterConfigHolder.getMaxMsgSize())
                        .append(",minMemCacheSize=")
                        .append(ClusterConfigHolder.getMinMemCacheSize())
                        .toString());
                sBuilder.delete(0, sBuilder.length());
            }
        }
        if (response.getTakeConfInfo()) {
            logger.info(sBuilder
                    .append("[HeartBeat response] received broker metadata info: brokerConfId=")
                    .append(response.getCurBrokerConfId())
                    .append(",stopWrite=").append(response.getStopWrite())
                    .append(",stopRead=").append(response.getStopRead())
                    .append(",configCheckSumId=").append(response.getConfCheckSumId())
                    .append(",hasFlowCtrl=").append(response.hasFlowCheckId())
                    .append(",curFlowCtrlId=").append(flowCheckId)
                    .append(",curQryPriorityId=").append(qryPriorityId)
                    .append(",brokerDefaultConfInfo=")
                    .append(response.getBrokerDefaultConfInfo())
                    .append(",brokerTopicSetConfList=")
                    .append(response.getBrokerTopicSetConfInfoList().toString()).toString());
            sBuilder.delete(0, sBuilder.length());
            metadataManager
                    .updateBrokerTopicConfigMap(response.getCurBrokerConfId(),
                            response.getConfCheckSumId(), response.getBrokerDefaultConfInfo(),
                            response.getBrokerTopicSetConfInfoList(), false, sBuilder);
        }
        // update auth info
        if (response.hasBrokerAuthorizedInfo()) {
            serverAuthHandler.appendVisitToken(response.getBrokerAuthorizedInfo());
        }
        // process topic deletion
        boolean needProcess =
                metadataManager.updateBrokerRemoveTopicMap(
                        response.getTakeRemoveTopicInfo(),
                        response.getRemoveTopicConfInfoList(), sBuilder);
        if (needProcess) {
            new Thread() {
                @Override
                public void run() {
                    storeManager.removeTopicStore();
                }
            }.start();
        }
    }

    /***
     * Register to master. Try multi times if failed.
     *
     * @throws StartupException
     */
    private void register2Master() throws StartupException {
        int remainingRetry = 5;
        StringBuilder sBuilder = new StringBuilder(512);
        while (true) {
            try {
                final RegisterResponseM2B response =
                        masterService.brokerRegisterB2M(createMasterRegisterRequest(),
                                tubeConfig.getHostName(), false);
                if (!response.getSuccess()) {
                    logger.warn("Register to master failure, errInfo is " + response.getErrMsg());
                    throw new StartupException(sBuilder
                            .append("Register to master failed! The error message is ")
                            .append(response.getErrMsg()).toString());
                }
                procConfigFromRegister(sBuilder, response);
                isKeepAlive.set(true);
                lastRegTime.set(System.currentTimeMillis());
                break;
            } catch (Throwable e) {
                sBuilder.delete(0, sBuilder.length());
                remainingRetry--;
                if (remainingRetry == 0) {
                    throw new StartupException("Register to master failed!", e);
                }
                ThreadUtils.sleep(200);
            }
        }
    }


    private void procConfigFromRegister(StringBuilder sBuilder,
                                        final RegisterResponseM2B response) {
        // process service status
        ServiceStatusHolder
                .setReadWriteServiceStatus(response.getStopRead(),
                        response.getStopWrite(), "Master");
        // process flow controller rules
        FlowCtrlRuleHandler flowCtrlRuleHandler =
                metadataManager.getFlowCtrlRuleHandler();
        if (response.hasFlowCheckId()) {
            int qryPriorityId = response.hasQryPriorityId()
                    ? response.getQryPriorityId() : flowCtrlRuleHandler.getQryPriorityId();
            if (response.getFlowCheckId() != flowCtrlRuleHandler.getFlowCtrlId()) {
                try {
                    flowCtrlRuleHandler
                            .updateDefFlowCtrlInfo(response.getQryPriorityId(),
                                    response.getFlowCheckId(), response.getFlowControlInfo());
                } catch (Exception e1) {
                    logger.warn("[Register response] found parse flowCtrl rules failure", e1);
                }
            }
            if (qryPriorityId != flowCtrlRuleHandler.getQryPriorityId()) {
                flowCtrlRuleHandler.setQryPriorityId(qryPriorityId);
            }
        }
        // update auth info
        serverAuthHandler.configure(response.getEnableBrokerInfo());
        if (response.hasBrokerAuthorizedInfo()) {
            serverAuthHandler.appendVisitToken(response.getBrokerAuthorizedInfo());
        }
        // update cluster setting
        if (response.hasClsConfig()) {
            long configId = response.getClsConfig().getConfigId();
            if (configId != ClusterConfigHolder.getConfigId()) {
                ClusterConfigHolder.updClusterSetting(response.getClsConfig());
            }
        }
        sBuilder.append("[Register response] received broker metadata info: brokerConfId=")
                .append(response.getCurBrokerConfId())
                .append(",stopWrite=").append(response.getStopWrite())
                .append(",stopRead=").append(response.getStopRead())
                .append(",configCheckSumId=").append(response.getConfCheckSumId())
                .append(",hasFlowCtrl=").append(response.hasFlowCheckId())
                .append(",curFlowCtrlId=").append(flowCtrlRuleHandler.getFlowCtrlId())
                .append(",curQryPriorityId=").append(flowCtrlRuleHandler.getQryPriorityId())
                .append(",hasClsConfig=").append(response.hasClsConfig())
                .append(",curClusterConfigId=").append(ClusterConfigHolder.getConfigId())
                .append(",curMaxMsgSize=").append(ClusterConfigHolder.getMaxMsgSize())
                .append(",minMemCacheSize=").append(ClusterConfigHolder.getMinMemCacheSize())
                .append(",enableVisitTokenCheck=")
                .append(serverAuthHandler.isEnableVisitTokenCheck())
                .append(",enableProduceAuthenticate=")
                .append(serverAuthHandler.isEnableProduceAuthenticate())
                .append(",enableProduceAuthorize=").append(serverAuthHandler.isEnableProduceAuthorize())
                .append(",enableConsumeAuthenticate=")
                .append(serverAuthHandler.isEnableConsumeAuthenticate())
                .append(",enableConsumeAuthorize=")
                .append(serverAuthHandler.isEnableConsumeAuthorize())
                .append(",brokerDefaultConfInfo=").append(response.getBrokerDefaultConfInfo())
                .append(",brokerTopicSetConfList=")
                .append(response.getBrokerTopicSetConfInfoList().toString()).toString();
        sBuilder.delete(0, sBuilder.length());
        metadataManager.updateBrokerTopicConfigMap(response.getCurBrokerConfId(),
                response.getConfCheckSumId(), response.getBrokerDefaultConfInfo(),
                response.getBrokerTopicSetConfInfoList(), true, sBuilder);
    }

    // build cluster configure info
    private ClientMaster.ClusterConfig.Builder buildClusterConfig() {
        ClientMaster.ClusterConfig.Builder defSetting =
                ClientMaster.ClusterConfig.newBuilder();
        defSetting.setConfigId(ClusterConfigHolder.getConfigId());
        return defSetting;
    }

    /***
     * Build register request to master.
     *
     * @return
     * @throws Exception
     */
    private RegisterRequestB2M createMasterRegisterRequest() throws Exception {
        RegisterRequestB2M.Builder builder = RegisterRequestB2M.newBuilder();
        builder.setClientId(this.brokerId);
        builder.setBrokerOnline(isOnline);
        builder.setEnableTls(this.tubeConfig.isTlsEnable());
        builder.setTlsPort(this.tubeConfig.getTlsPort());
        builder.setReadStatusRpt(ServiceStatusHolder.getReadServiceReportStatus());
        builder.setWriteStatusRpt(ServiceStatusHolder.getWriteServiceReportStatus());
        builder.setCurBrokerConfId(metadataManager.getBrokerMetadataConfId());
        builder.setConfCheckSumId(metadataManager.getBrokerConfCheckSumId());
        FlowCtrlRuleHandler flowCtrlRuleHandler =
                metadataManager.getFlowCtrlRuleHandler();
        builder.setFlowCheckId(flowCtrlRuleHandler.getFlowCtrlId());
        builder.setQryPriorityId(flowCtrlRuleHandler.getQryPriorityId());
        String brokerDefaultConfInfo = metadataManager.getBrokerDefMetaConfInfo();
        if (brokerDefaultConfInfo != null) {
            builder.setBrokerDefaultConfInfo(brokerDefaultConfInfo);
        }
        List<String> topicConfInfoList = metadataManager.getTopicMetaConfInfoLst();
        if (topicConfInfoList != null) {
            builder.addAllBrokerTopicSetConfInfo(topicConfInfoList);
        }
        ClientMaster.MasterCertificateInfo.Builder authInfoBuilder = genMasterCertificateInfo();
        if (authInfoBuilder != null) {
            builder.setAuthInfo(authInfoBuilder.build());
        }
        builder.setClsConfig(buildClusterConfig());
        logger.info(new StringBuilder(512)
            .append("[Register request] current broker report info: brokerConfId=")
            .append(metadataManager.getBrokerMetadataConfId())
            .append(",readStatusRpt=").append(builder.getReadStatusRpt())
            .append(",writeStatusRpt=").append(builder.getWriteStatusRpt())
            .append(",isTlsEnable=").append(tubeConfig.isTlsEnable())
            .append(",TlsPort=").append(tubeConfig.getTlsPort())
            .append(",flowCtrlId=").append(flowCtrlRuleHandler.getFlowCtrlId())
            .append(",QryPriorityId=").append(flowCtrlRuleHandler.getQryPriorityId())
            .append(",configCheckSumId=").append(metadataManager.getBrokerConfCheckSumId())
            .append(",brokerDefaultConfInfo=").append(brokerDefaultConfInfo)
            .append(",brokerTopicSetConfList=").append(topicConfInfoList).toString());
        return builder.build();
    }

    /***
     * Build heartbeat request to master.
     *
     * @return
     */
    private HeartRequestB2M createBrokerHeartBeatRequest() {
        HeartRequestB2M.Builder builder = HeartRequestB2M.newBuilder();
        builder.setBrokerId(String.valueOf(tubeConfig.getBrokerId()));
        builder.setBrokerOnline(isOnline);
        builder.setReadStatusRpt(ServiceStatusHolder.getReadServiceReportStatus());
        builder.setWriteStatusRpt(ServiceStatusHolder.getWriteServiceReportStatus());
        builder.setCurBrokerConfId(metadataManager.getBrokerMetadataConfId());
        builder.setConfCheckSumId(metadataManager.getBrokerConfCheckSumId());
        FlowCtrlRuleHandler flowCtrlRuleHandler =
                metadataManager.getFlowCtrlRuleHandler();
        builder.setFlowCheckId(flowCtrlRuleHandler.getFlowCtrlId());
        builder.setQryPriorityId(flowCtrlRuleHandler.getQryPriorityId());
        builder.setTakeConfInfo(false);
        builder.setTakeRemovedTopicInfo(false);
        List<String> removedTopics = this.metadataManager.getHardRemovedTopics();
        if (!removedTopics.isEmpty()) {
            builder.setTakeRemovedTopicInfo(true);
            builder.addAllRemovedTopicsInfo(removedTopics);
        }
        ClientMaster.MasterCertificateInfo.Builder authInfoBuilder = genMasterCertificateInfo();
        if (authInfoBuilder != null) {
            builder.setAuthInfo(authInfoBuilder.build());
        }
        builder.setClsConfig(buildClusterConfig());
        if (metadataManager.isBrokerMetadataChanged() || requireReportConf) {
            builder.setTakeConfInfo(true);
            builder.setBrokerDefaultConfInfo(metadataManager.getBrokerDefMetaConfInfo());
            builder.addAllBrokerTopicSetConfInfo(metadataManager.getTopicMetaConfInfoLst());
            logger.info(new StringBuilder(512)
                .append("[HeartBeat request] current broker report info: brokerConfId=")
                .append(metadataManager.getBrokerMetadataConfId())
                .append(",readStatusRpt=").append(builder.getReadStatusRpt())
                .append(",writeStatusRpt=").append(builder.getWriteStatusRpt())
                .append(",flowCtrlId=").append(flowCtrlRuleHandler.getFlowCtrlId())
                .append(",QryPriorityId=").append(flowCtrlRuleHandler.getQryPriorityId())
                .append(",ReadStatusRpt=").append(builder.getReadStatusRpt())
                .append(",WriteStatusRpt=").append(builder.getWriteStatusRpt())
                .append(",lastReportedConfigId=").append(metadataManager.getLastRptBrokerMetaConfId())
                .append(",configCheckSumId=").append(metadataManager.getBrokerConfCheckSumId())
                .append(",brokerDefaultConfInfo=").append(metadataManager.getBrokerDefMetaConfInfo())
                .append(",brokerTopicSetConfList=").append(metadataManager.getTopicMetaConfInfoLst()).toString());
            metadataManager.setLastRptBrokerMetaConfId(metadataManager.getBrokerMetadataConfId());
            requireReportConf = false;
        }
        return builder.build();
    }

    /***
     * Build close request to master.
     *
     * @return
     */
    private CloseRequestB2M createMasterCloseRequest() {
        CloseRequestB2M.Builder builder = CloseRequestB2M.newBuilder();
        builder.setBrokerId(String.valueOf(tubeConfig.getBrokerId()));
        ClientMaster.MasterCertificateInfo.Builder authInfoBuilder = genMasterCertificateInfo();
        if (authInfoBuilder != null) {
            builder.setAuthInfo(authInfoBuilder.build());
        }
        return builder.build();
    }

    /***
     * Build master certificate info.
     *
     * @return
     */
    private ClientMaster.MasterCertificateInfo.Builder genMasterCertificateInfo() {
        ClientMaster.MasterCertificateInfo.Builder authInfoBuilder = null;
        if (tubeConfig.isVisitMasterAuth()) {
            authInfoBuilder = ClientMaster.MasterCertificateInfo.newBuilder();
            authInfoBuilder.setAuthInfo(clientAuthHandler
                    .genMasterAuthenticateToken(tubeConfig.getVisitName(),
                            tubeConfig.getVisitPassword()));
        }
        return authInfoBuilder;
    }

    // valid and get need removed group-topic info
    public boolean validAndGetGroupTopicInfo(Set<String> groupSet,
        Set<String> topicSet,
        ProcessResult result) {
        Map<String, Map<String, Set<Integer>>> groupTopicPartMap = new HashMap<>();
        // filter group
        Set<String> targetGroupSet = new HashSet<>();
        Set<String> bookedGroups = offsetManager.getBookedGroups();
        for (String orgGroup : groupSet) {
            if (bookedGroups.contains(orgGroup)) {
                targetGroupSet.add(orgGroup);
            }
        }
        // valid specified topic set
        for (String group : targetGroupSet) {
            if (validAndGetTopicPartInfo(group, WebFieldDef.GROUPNAME.name, topicSet, result)) {
                Map<String, Set<Integer>> topicPartMap =
                    (Map<String, Set<Integer>>) result.retData1;
                groupTopicPartMap.put(group, topicPartMap);
            }
        }
        result.setSuccResult(groupTopicPartMap);
        return true;
    }


    // builder group's offset info
    public Map<String, Map<String, Map<Integer, GroupOffsetInfo>>> getGroupOffsetInfo(
        String groupParamName, Set<String> groupSet, Set<String> topicSet) {
        ProcessResult result = new ProcessResult();
        Map<String, Map<String, Map<Integer, GroupOffsetInfo>>> groupOffsetMaps = new HashMap<>();
        for (String group : groupSet) {
            Map<String, Map<Integer, GroupOffsetInfo>> topicOffsetRet = new HashMap<>();
            // valid and get topic's partitionIds
            if (validAndGetTopicPartInfo(group, groupParamName, topicSet, result)) {
                Map<String, Set<Integer>> topicPartMap =
                    (Map<String, Set<Integer>>) result.retData1;
                // get topic's publish info
                Map<String, Map<Integer, TopicPubStoreInfo>> topicStorePubInfoMap =
                    storeManager.getTopicPublishInfos(topicPartMap.keySet());
                // get group's booked offset info
                Map<String, Map<Integer, Tuple2<Long, Long>>> groupOffsetMap =
                    offsetManager.queryGroupOffset(group, topicPartMap);
                // form offset info array
                getOffsetInfoArray(group, topicOffsetRet, topicPartMap, topicStorePubInfoMap,
                    groupOffsetMap);
            }
            groupOffsetMaps.put(group, topicOffsetRet);
        }
        return groupOffsetMaps;
    }

    private void getOffsetInfoArray(String group, Map<String, Map<Integer, GroupOffsetInfo>> topicOffsetRet,
        Map<String, Set<Integer>> topicPartMap,
        Map<String, Map<Integer, TopicPubStoreInfo>> topicStorePubInfoMap,
        Map<String, Map<Integer, Tuple2<Long, Long>>> groupOffsetMap) {
        for (Map.Entry<String, Set<Integer>> entry : topicPartMap.entrySet()) {
            String topic = entry.getKey();
            Map<Integer, GroupOffsetInfo> partOffsetRet = new HashMap<>();
            Map<Integer, TopicPubStoreInfo> storeInfoMap = topicStorePubInfoMap.get(topic);
            Map<Integer, Tuple2<Long, Long>> partBookedMap = groupOffsetMap.get(topic);
            for (Integer partitionId : entry.getValue()) {
                GroupOffsetInfo offsetInfo = new GroupOffsetInfo(partitionId);
                offsetInfo.setPartPubStoreInfo(
                    storeInfoMap == null ? null : storeInfoMap.get(partitionId));
                offsetInfo.setConsumeOffsetInfo(
                    partBookedMap == null ? null : partBookedMap.get(partitionId));
                String queryKey = buildQueryID(group, topic, partitionId);
                ConsumerNodeInfo nodeInfo = getConsumerNodeInfo(queryKey);
                if (nodeInfo != null) {
                    offsetInfo.setConsumeDataOffsetInfo(nodeInfo.getLastDataRdOffset());
                }
                offsetInfo.calculateLag();
                partOffsetRet.put(partitionId, offsetInfo);
            }
            topicOffsetRet.put(topic, partOffsetRet);
        }
    }


    private String buildQueryID(String group, String topic, int partitionId) {
        return new StringBuilder(512).append(group)
            .append(TokenConstants.ATTR_SEP).append(topic)
            .append(TokenConstants.ATTR_SEP).append(partitionId).toString();
    }

    public boolean validAndGetTopicPartInfo(String groupName,
        String groupParamName,
        Set<String> topicSet,
        ProcessResult result) {
        Set<String> subTopicSet =
            offsetManager.getGroupSubInfo(groupName);
        if (subTopicSet == null || subTopicSet.isEmpty()) {
            result.setFailResult(400, new StringBuilder(512)
                .append("Parameter ").append(groupParamName)
                .append(": subscribed topic set of ").append(groupName)
                .append(" query result is null!").toString());
            return result.success;
        }
        // filter valid topic set
        Set<String> tgtTopicSet = new HashSet<>();
        if (topicSet.isEmpty()) {
            tgtTopicSet = subTopicSet;
        } else {
            for (String topic : topicSet) {
                if (subTopicSet.contains(topic)) {
                    tgtTopicSet.add(topic);
                }
            }
            if (tgtTopicSet.isEmpty()) {
                result.setFailResult(400, new StringBuilder(512)
                    .append("Parameter ").append(groupParamName)
                    .append(": ").append(groupName)
                    .append(" unsubscribed to the specified topic set!").toString());
                return result.success;
            }
        }
        Map<String, Set<Integer>> topicPartMap = getTopicPartitions(tgtTopicSet);
        if (topicPartMap.isEmpty()) {
            result.setFailResult(400, new StringBuilder(512)
                .append("Parameter ").append(groupParamName)
                .append(": all topics subscribed by the group have been deleted!").toString());
            return result.success;
        }
        result.setSuccResult(topicPartMap);
        return result.success;
    }



    public Map<String, Set<Integer>> getTopicPartitions(Set<String> topicSet) {
        Map<String, Set<Integer>> topicPartMap = new HashMap<>();
        if (topicSet != null) {
            Map<String, TopicMetadata> topicConfigMap =
                metadataManager.getTopicConfigMap();
            if (topicConfigMap != null) {
                for (String topic : topicSet) {
                    TopicMetadata topicMetadata = topicConfigMap.get(topic);
                    if (topicMetadata != null) {
                        topicPartMap.put(topic, topicMetadata.getAllPartitionIds());
                    }
                }
            }
        }
        return topicPartMap;
    }

    /***
     * Shutdown hook.
     */
    private final class ShutdownHook extends Thread {
        @Override
        public void run() {
            if (shutdownHooked.compareAndSet(false, true)) {
                TubeBroker.this.stop("Shutdown by Hook");
            }
        }
    }
}
