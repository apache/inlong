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

import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.log4j.LogManager;
import org.apache.tubemq.corebase.TErrCodeConstants;
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
import org.apache.tubemq.corerpc.RpcConfig;
import org.apache.tubemq.corerpc.RpcConstants;
import org.apache.tubemq.corerpc.RpcServiceFactory;
import org.apache.tubemq.corerpc.netty.NettyClientFactory;
import org.apache.tubemq.corerpc.service.MasterService;
import org.apache.tubemq.server.Stoppable;
import org.apache.tubemq.server.broker.exception.StartupException;
import org.apache.tubemq.server.broker.metadata.BrokerMetadataManager;
import org.apache.tubemq.server.broker.metadata.MetadataManager;
import org.apache.tubemq.server.broker.msgstore.MessageStoreManager;
import org.apache.tubemq.server.broker.nodeinfo.ConsumerNodeInfo;
import org.apache.tubemq.server.broker.offset.DefaultOffsetManager;
import org.apache.tubemq.server.broker.offset.OffsetService;
import org.apache.tubemq.server.broker.utils.BrokerSamplePrint;
import org.apache.tubemq.server.broker.web.WebServer;
import org.apache.tubemq.server.common.TubeServerVersion;
import org.apache.tubemq.server.common.aaaserver.SimpleCertificateBrokerHandler;
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
    private final BrokerSamplePrint samplePrintCtrl =
            new BrokerSamplePrint();
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
                            FlowCtrlRuleHandler flowCtrlRuleHandler =
                                metadataManager.getFlowCtrlRuleHandler();
                            long flowCheckId = flowCtrlRuleHandler.getFlowCtrlId();
                            int qryPriorityId = flowCtrlRuleHandler.getQryPriorityId();
                            ServiceStatusHolder
                                .setReadWriteServiceStatus(response.getStopRead(),
                                    response.getStopWrite(), "Master");
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
                            requireReportConf = response.getNeedReportData();
                            StringBuilder sBuilder = new StringBuilder(512);
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
                            if (response.hasBrokerAuthorizedInfo()) {
                                serverAuthHandler.appendVisitToken(response.getBrokerAuthorizedInfo());
                            }
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
        try {
            Thread.sleep(2000);
        } catch (InterruptedException e) {
            //
        }
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
        try {
            Thread.sleep(2000);
        } catch (Throwable ee) {
            //
        }
    }

    private String generateBrokerClientId() {
        return new StringBuilder(512).append(tubeConfig.getBrokerId()).append(":")
                .append(tubeConfig.getHostName()).append(":")
                .append(tubeConfig.getPort()).append(":")
                .append(TubeServerVersion.BROKER_VERSION).toString();
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
                ServiceStatusHolder
                        .setReadWriteServiceStatus(response.getStopRead(),
                                response.getStopWrite(), "Master");
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
                updateEnableBrokerFunInfo(response);
                logger.info(sBuilder
                    .append("[Register response] received broker metadata info: brokerConfId=")
                    .append(response.getCurBrokerConfId())
                    .append(",stopWrite=").append(response.getStopWrite())
                    .append(",stopRead=").append(response.getStopRead())
                    .append(",configCheckSumId=").append(response.getConfCheckSumId())
                    .append(",hasFlowCtrl=").append(response.hasFlowCheckId())
                    .append(",enableVisitTokenCheck=")
                    .append(serverAuthHandler.isEnableVisitTokenCheck())
                    .append(",enableProduceAuthenticate=")
                    .append(serverAuthHandler.isEnableProduceAuthenticate())
                    .append(",enableProduceAuthorize=").append(serverAuthHandler.isEnableProduceAuthorize())
                    .append(",enableConsumeAuthenticate=")
                    .append(serverAuthHandler.isEnableConsumeAuthenticate())
                    .append(",enableConsumeAuthorize=")
                    .append(serverAuthHandler.isEnableConsumeAuthorize())
                    .append(",curFlowCtrlId=").append(flowCtrlRuleHandler.getFlowCtrlId())
                    .append(",curQryPriorityId=").append(flowCtrlRuleHandler.getQryPriorityId())
                    .append(",brokerDefaultConfInfo=").append(response.getBrokerDefaultConfInfo())
                    .append(",brokerTopicSetConfList=")
                    .append(response.getBrokerTopicSetConfInfoList().toString()).toString());
                sBuilder.delete(0, sBuilder.length());
                metadataManager.updateBrokerTopicConfigMap(response.getCurBrokerConfId(),
                    response.getConfCheckSumId(), response.getBrokerDefaultConfInfo(),
                    response.getBrokerTopicSetConfInfoList(), true, sBuilder);
                isKeepAlive.set(true);
                lastRegTime.set(System.currentTimeMillis());
                break;
            } catch (Throwable e) {
                sBuilder.delete(0, sBuilder.length());
                remainingRetry--;
                if (remainingRetry == 0) {
                    throw new StartupException("Register to master failed!", e);
                }
            }
        }
    }

    private void updateEnableBrokerFunInfo(final RegisterResponseM2B response) {
        serverAuthHandler.configure(response.getEnableBrokerInfo());
        if (response.hasBrokerAuthorizedInfo()) {
            serverAuthHandler.appendVisitToken(response.getBrokerAuthorizedInfo());
        }
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
