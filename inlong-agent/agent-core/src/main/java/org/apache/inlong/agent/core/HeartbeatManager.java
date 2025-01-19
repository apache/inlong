/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.inlong.agent.core;

import org.apache.inlong.agent.common.AbstractDaemon;
import org.apache.inlong.agent.conf.AgentConfiguration;
import org.apache.inlong.agent.constant.CommonConstants;
import org.apache.inlong.agent.core.task.MemoryManager;
import org.apache.inlong.agent.utils.AgentUtils;
import org.apache.inlong.agent.utils.HttpManager;
import org.apache.inlong.agent.utils.ThreadUtils;
import org.apache.inlong.common.enums.ComponentTypeEnum;
import org.apache.inlong.common.enums.NodeSrvStatus;
import org.apache.inlong.common.heartbeat.AbstractHeartbeatManager;
import org.apache.inlong.common.heartbeat.HeartbeatMsg;
import org.apache.inlong.sdk.dataproxy.DefaultMessageSender;
import org.apache.inlong.sdk.dataproxy.TcpMsgSenderConfig;

import io.netty.util.concurrent.DefaultThreadFactory;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ThreadFactory;

import static org.apache.inlong.agent.constant.AgentConstants.AGENT_CLUSTER_IN_CHARGES;
import static org.apache.inlong.agent.constant.AgentConstants.AGENT_CLUSTER_NAME;
import static org.apache.inlong.agent.constant.AgentConstants.AGENT_CLUSTER_TAG;
import static org.apache.inlong.agent.constant.AgentConstants.AGENT_NODE_GROUP;
import static org.apache.inlong.agent.constant.FetcherConstants.AGENT_MANAGER_ADDR;
import static org.apache.inlong.agent.constant.FetcherConstants.AGENT_MANAGER_AUTH_SECRET_ID;
import static org.apache.inlong.agent.constant.FetcherConstants.AGENT_MANAGER_AUTH_SECRET_KEY;
import static org.apache.inlong.agent.constant.FetcherConstants.AGENT_MANAGER_HEARTBEAT_HTTP_PATH;
import static org.apache.inlong.agent.constant.FetcherConstants.DEFAULT_AGENT_MANAGER_HEARTBEAT_HTTP_PATH;

/**
 * report heartbeat to inlong-manager
 */
public class HeartbeatManager extends AbstractDaemon implements AbstractHeartbeatManager {

    private static final Logger LOGGER = LoggerFactory.getLogger(HeartbeatManager.class);
    public static final int PRINT_MEMORY_PERMIT_INTERVAL_SECOND = 60;
    public static final int HEARTBEAT_INTERVAL_SECOND = 60;
    public static final String INLONG_AGENT_SYSTEM = "inlong_agent_system";

    private static HeartbeatManager heartbeatManager = null;
    private final AgentConfiguration conf;
    private final HttpManager httpManager;
    private final String baseManagerUrl;
    private final String reportHeartbeatUrl;
    private DefaultMessageSender sender;

    /**
     * Init heartbeat manager.
     */
    private HeartbeatManager(AgentManager agentManager) {
        this.conf = AgentConfiguration.getAgentConf();
        httpManager = new HttpManager(conf);
        baseManagerUrl = httpManager.getBaseUrl();
        reportHeartbeatUrl = buildReportHeartbeatUrl(baseManagerUrl);
    }

    public static HeartbeatManager getInstance(AgentManager agentManager) {
        if (heartbeatManager == null) {
            synchronized (HeartbeatManager.class) {
                if (heartbeatManager == null) {
                    heartbeatManager = new HeartbeatManager(agentManager);
                }
            }
        }
        return heartbeatManager;
    }

    public static HeartbeatManager getInstance() {
        if (heartbeatManager == null) {
            throw new RuntimeException("HeartbeatManager has not been initialized by agentManager");
        }
        return heartbeatManager;
    }

    @Override
    public void start() throws Exception {
        submitWorker(heartbeatReportThread());
        submitWorker(printMemoryPermitThread());
    }

    private Runnable printMemoryPermitThread() {
        return () -> {
            Thread.currentThread().setName("heartBeat-printMemoryPermit");
            while (isRunnable()) {
                MemoryManager.getInstance().printAll();
                AgentUtils.silenceSleepInSeconds(PRINT_MEMORY_PERMIT_INTERVAL_SECOND);
            }
        };
    }

    private Runnable heartbeatReportThread() {
        return () -> {
            Thread.currentThread().setName("heartBeat-heartbeatReportThread");
            while (isRunnable()) {
                try {
                    HeartbeatMsg heartbeatMsg = buildHeartbeatMsg();
                    reportHeartbeat(heartbeatMsg);
                    if (LOGGER.isDebugEnabled()) {
                        LOGGER.debug(" {} report heartbeat to manager", heartbeatMsg);
                    }
                    AgentStatusManager.sendStatusMsg(sender);
                    FileStaticManager.sendStaticMsg(sender);
                } catch (Throwable e) {
                    LOGGER.error("interrupted while report heartbeat", e);
                    ThreadUtils.threadThrowableHandler(Thread.currentThread(), e);
                } finally {
                    AgentUtils.silenceSleepInSeconds(HEARTBEAT_INTERVAL_SECOND);
                }
            }
        };
    }

    @Override
    public void stop() throws Exception {
        waitForTerminate();
    }

    @Override
    public void reportHeartbeat(HeartbeatMsg heartbeat) {
        httpManager.doSentPost(reportHeartbeatUrl, heartbeat);
    }

    /**
     * build heartbeat message of agent
     */
    private HeartbeatMsg buildHeartbeatMsg() {
        final String agentIp = AgentUtils.fetchLocalIp();
        final String clusterName = conf.get(AGENT_CLUSTER_NAME);
        final String clusterTag = conf.get(AGENT_CLUSTER_TAG);
        final String inCharges = conf.get(AGENT_CLUSTER_IN_CHARGES);
        final String nodeGroup = conf.get(AGENT_NODE_GROUP);

        HeartbeatMsg heartbeatMsg = new HeartbeatMsg();
        heartbeatMsg.setIp(agentIp);
        heartbeatMsg.setComponentType(ComponentTypeEnum.Agent.getType());
        heartbeatMsg.setReportTime(System.currentTimeMillis());
        if (StringUtils.isNotBlank(clusterName)) {
            heartbeatMsg.setClusterName(clusterName);
        }
        if (StringUtils.isNotBlank(clusterTag)) {
            heartbeatMsg.setClusterTag(clusterTag);
        }
        if (StringUtils.isNotBlank(inCharges)) {
            heartbeatMsg.setInCharges(inCharges);
        }
        if (StringUtils.isNotBlank(nodeGroup)) {
            heartbeatMsg.setNodeGroup(nodeGroup);
        }

        return heartbeatMsg;
    }

    /**
     * build dead heartbeat message of agent
     */
    private HeartbeatMsg buildDeadHeartbeatMsg() {
        HeartbeatMsg heartbeatMsg = new HeartbeatMsg();
        heartbeatMsg.setNodeSrvStatus(NodeSrvStatus.SERVICE_UNINSTALL);
        heartbeatMsg.setInCharges(conf.get(AGENT_CLUSTER_IN_CHARGES));
        heartbeatMsg.setIp(AgentUtils.fetchLocalIp());
        heartbeatMsg.setComponentType(ComponentTypeEnum.Agent.getType());
        heartbeatMsg.setClusterName(conf.get(AGENT_CLUSTER_NAME));
        heartbeatMsg.setClusterTag(conf.get(AGENT_CLUSTER_TAG));
        return heartbeatMsg;
    }

    private String buildReportHeartbeatUrl(String baseUrl) {
        return baseUrl + conf.get(AGENT_MANAGER_HEARTBEAT_HTTP_PATH, DEFAULT_AGENT_MANAGER_HEARTBEAT_HTTP_PATH);
    }

    private void createMessageSender() {
        String managerAddr = conf.get(AGENT_MANAGER_ADDR);
        String authSecretId = conf.get(AGENT_MANAGER_AUTH_SECRET_ID);
        String authSecretKey = conf.get(AGENT_MANAGER_AUTH_SECRET_KEY);
        TcpMsgSenderConfig proxyClientConfig = null;
        try {
            proxyClientConfig = new TcpMsgSenderConfig(managerAddr, INLONG_AGENT_SYSTEM, authSecretId, authSecretKey);
            proxyClientConfig.setTotalAsyncCallbackSize(CommonConstants.DEFAULT_PROXY_TOTAL_ASYNC_PROXY_SIZE);
            proxyClientConfig.setAliveConnections(CommonConstants.DEFAULT_PROXY_ALIVE_CONNECTION_NUM);
            proxyClientConfig.setNettyWorkerThreadNum(CommonConstants.DEFAULT_PROXY_CLIENT_IO_THREAD_NUM);
            proxyClientConfig.setRequestTimeoutMs(30000L);
            ThreadFactory SHARED_FACTORY = new DefaultThreadFactory("agent-sender-manager-heartbeat",
                    Thread.currentThread().isDaemon());
            sender = new DefaultMessageSender(proxyClientConfig, SHARED_FACTORY);
        } catch (Exception e) {
            LOGGER.error("heartbeat manager create sdk failed: ", e);
        }
    }
}