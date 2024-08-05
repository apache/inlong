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

import org.apache.inlong.agent.conf.AgentConfiguration;
import org.apache.inlong.agent.constant.CommonConstants;
import org.apache.inlong.agent.core.task.MemoryManager;
import org.apache.inlong.agent.core.task.OffsetManager;
import org.apache.inlong.agent.core.task.TaskManager;
import org.apache.inlong.agent.utils.AgentUtils;
import org.apache.inlong.agent.utils.ExcuteLinux;
import org.apache.inlong.common.constant.ProtocolType;
import org.apache.inlong.sdk.dataproxy.DefaultMessageSender;
import org.apache.inlong.sdk.dataproxy.ProxyClientConfig;
import org.apache.inlong.sdk.dataproxy.common.SendResult;

import com.google.common.collect.Lists;
import io.netty.util.concurrent.DefaultThreadFactory;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.management.ManagementFactory;
import java.lang.management.OperatingSystemMXBean;
import java.lang.management.RuntimeMXBean;
import java.lang.management.ThreadMXBean;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.apache.inlong.agent.constant.AgentConstants.AGENT_CLUSTER_NAME;
import static org.apache.inlong.agent.constant.AgentConstants.AGENT_CLUSTER_TAG;
import static org.apache.inlong.agent.constant.AgentConstants.AGENT_INSTALL_PLATFORM;
import static org.apache.inlong.agent.constant.FetcherConstants.AGENT_GLOBAL_READER_QUEUE_PERMIT;
import static org.apache.inlong.agent.constant.FetcherConstants.AGENT_GLOBAL_READER_SOURCE_PERMIT;
import static org.apache.inlong.agent.constant.FetcherConstants.AGENT_GLOBAL_WRITER_PERMIT;
import static org.apache.inlong.agent.constant.FetcherConstants.AGENT_MANAGER_ADDR;
import static org.apache.inlong.agent.constant.FetcherConstants.AGENT_MANAGER_AUTH_SECRET_ID;
import static org.apache.inlong.agent.constant.FetcherConstants.AGENT_MANAGER_AUTH_SECRET_KEY;

/**
 * Collect various indicators of agent processes for backend problem analysis
 */
public class AgentStatusManager {

    private static final Logger LOGGER = LoggerFactory.getLogger(AgentStatusManager.class);
    public static final String INLONG_AGENT_SYSTEM = "inlong_agent_system";
    public static final String INLONG_AGENT_STATUS = "inlong_agent_status";

    private static AgentStatusManager manager = null;
    private final AgentConfiguration conf;
    private final SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss"); // 设置格式
    private Runtime runtime = Runtime.getRuntime();
    final long GB = 1024 * 1024 * 1024;
    private OperatingSystemMXBean osMxBean;
    private ThreadMXBean threadBean;
    private long INVALID_CPU = -1;
    private RuntimeMXBean runtimeMXBean = ManagementFactory.getRuntimeMXBean();
    private ThreadMXBean threadMXBean = ManagementFactory.getThreadMXBean();
    private AgentManager agentManager;
    public static AtomicLong sendDataLen = new AtomicLong();
    public static AtomicLong sendPackageCount = new AtomicLong();
    private DefaultMessageSender sender;
    private List<String> statusFieldsPre = Lists.newArrayList();

    private AgentStatusManager(AgentManager agentManager) {
        this.agentManager = agentManager;
        this.conf = AgentConfiguration.getAgentConf();
        osMxBean = ManagementFactory.getOperatingSystemMXBean();
        threadBean = ManagementFactory.getThreadMXBean();
        initStatusFieldsPre();
        createMessageSender();
    }

    public static AgentStatusManager getInstance(AgentManager agentManager) {
        if (manager == null) {
            synchronized (AgentStatusManager.class) {
                if (manager == null) {
                    manager = new AgentStatusManager(agentManager);
                }
            }
        }
        return manager;
    }

    public static AgentStatusManager getInstance() {
        if (manager == null) {
            throw new RuntimeException("HeartbeatManager has not been initialized by agentManager");
        }
        return manager;
    }

    public void sendStatusMsg(List<String> fields) {
        if (sender == null) {
            LOGGER.error("sender is null");
            createMessageSender();
            return;
        }
        SendResult ret = sender.sendMessage(
                StringUtils.join(fields, ",").getBytes(StandardCharsets.UTF_8),
                INLONG_AGENT_SYSTEM,
                INLONG_AGENT_STATUS,
                AgentUtils.getCurrentTime(),
                "", 30, TimeUnit.SECONDS);
        if (ret != SendResult.OK) {
            LOGGER.error("send status failed: ret {}", ret);
        }
    }

    public void printStatusMsg(List<String> fields) {
        List<String> toPrint = new ArrayList<>();
        for (int i = 0; i < statusFieldsPre.size(); i++) {
            toPrint.add(statusFieldsPre.get(i) + ": " + fields.get(i));
        }
        LOGGER.info("status detail:\n{}", StringUtils.join(toPrint, "\n"));
    }

    private void createMessageSender() {
        String managerAddr = conf.get(AGENT_MANAGER_ADDR);
        String authSecretId = conf.get(AGENT_MANAGER_AUTH_SECRET_ID);
        String authSecretKey = conf.get(AGENT_MANAGER_AUTH_SECRET_KEY);
        ProxyClientConfig proxyClientConfig = null;
        try {
            proxyClientConfig = new ProxyClientConfig(managerAddr, INLONG_AGENT_SYSTEM, authSecretId, authSecretKey);
            proxyClientConfig.setTotalAsyncCallbackSize(CommonConstants.DEFAULT_PROXY_TOTAL_ASYNC_PROXY_SIZE);
            proxyClientConfig.setAliveConnections(CommonConstants.DEFAULT_PROXY_ALIVE_CONNECTION_NUM);
            proxyClientConfig.setIoThreadNum(CommonConstants.DEFAULT_PROXY_CLIENT_IO_THREAD_NUM);
            proxyClientConfig.setProtocolType(ProtocolType.TCP);
            ThreadFactory SHARED_FACTORY = new DefaultThreadFactory("agent-sender-manager-heartbeat",
                    Thread.currentThread().isDaemon());
            sender = new DefaultMessageSender(proxyClientConfig, SHARED_FACTORY);
        } catch (Exception e) {
            LOGGER.error("heartbeat manager create sdk failed: ", e);
        }
    }

    private double getProcessCpu() {
        double cpu = tryGetProcessCpu();
        int tryTimes = 0;
        while (cpu < 0 && tryTimes < 10) {
            cpu = tryGetProcessCpu();
            tryTimes++;
        }
        return cpu;
    }

    private double tryGetProcessCpu() {
        long[] startThreads = threadBean.getAllThreadIds();
        long startTime = System.nanoTime();
        long startUseTime = 0;
        long temp;
        for (long id : startThreads) {
            temp = threadBean.getThreadCpuTime(id);
            if (temp >= 0) {
                startUseTime += temp;
            } else {
                return INVALID_CPU;
            }
        }
        AgentUtils.silenceSleepInMs(5000);
        long[] endThreads = threadBean.getAllThreadIds();
        long endTime = System.nanoTime();
        long endUseTime = 0;
        for (long id : endThreads) {
            temp = threadBean.getThreadCpuTime(id);
            if (temp >= 0) {
                endUseTime += temp;
            } else {
                return INVALID_CPU;
            }
        }
        long usedTime = endUseTime - startUseTime;
        long totalPassedTime = endTime - startTime;
        return (((double) usedTime) / totalPassedTime) * 100;
    }

    public List<String> getStatusMessage() {
        List<String> fields = Lists.newArrayList();
        fields.add(AgentUtils.fetchLocalIp());
        fields.add(conf.get(AGENT_CLUSTER_NAME));
        fields.add(conf.get(AGENT_CLUSTER_TAG));
        fields.add(TaskManager.class.getPackage().getImplementationVersion());
        fields.add(format.format(runtimeMXBean.getStartTime()));
        fields.add(String.valueOf(runtime.availableProcessors()));
        fields.add(String.valueOf(twoDecimal(getProcessCpu())));
        fields.add(String.valueOf(twoDecimal((double) runtime.freeMemory() / GB)));
        fields.add(String.valueOf(twoDecimal((double) runtime.maxMemory() / GB)));
        fields.add(String.valueOf(twoDecimal((double) runtime.totalMemory() / GB)));
        fields.add(System.getProperty("os.version"));
        fields.add(conf.get(AGENT_INSTALL_PLATFORM, ""));
        fields.add(System.getProperty("user.dir"));
        fields.add(System.getProperty("user.name"));
        fields.add(String.valueOf(getProcessId()));
        if (AgentManager.getAgentConfigInfo() != null) {
            fields.add(AgentManager.getAgentConfigInfo().getMd5());
        } else {
            fields.add("");
        }
        fields.add(agentManager.getTaskManager().getTaskResultMd5());
        fields.add(String.valueOf(agentManager.getTaskManager().getTaskStore().getTasks().size()));
        fields.add(String.valueOf(OffsetManager.getInstance().getRunningInstanceCount()));
        fields.add(ExcuteLinux.exeCmd("who -b|awk '{print $(NF-1), $NF}'").replaceAll("\r|\n", ""));
        fields.add(String.valueOf(sendPackageCount.getAndSet(0)));
        fields.add(String.valueOf(sendDataLen.getAndSet(0)));
        fields.add(String.valueOf(MemoryManager.getInstance().getLeft(AGENT_GLOBAL_READER_SOURCE_PERMIT)));
        fields.add(String.valueOf(MemoryManager.getInstance().getLeft(AGENT_GLOBAL_READER_QUEUE_PERMIT)));
        fields.add(String.valueOf(MemoryManager.getInstance().getLeft(AGENT_GLOBAL_WRITER_PERMIT)));
        fields.add(String.valueOf(threadMXBean.getThreadCount()));
        return fields;
    }

    private void initStatusFieldsPre() {
        statusFieldsPre.add("ip: ");
        statusFieldsPre.add("cluster: ");
        statusFieldsPre.add("tag: ");
        statusFieldsPre.add("agent version: ");
        statusFieldsPre.add("agent start time: ");
        statusFieldsPre.add("cpu core: ");
        statusFieldsPre.add("proc cpu: ");
        statusFieldsPre.add("free mem: ");
        statusFieldsPre.add("max mem: ");
        statusFieldsPre.add("use mem: ");
        statusFieldsPre.add("os: ");
        statusFieldsPre.add("install platform: ");
        statusFieldsPre.add("usr dir: ");
        statusFieldsPre.add("usr name: ");
        statusFieldsPre.add("process id: ");
        statusFieldsPre.add("global config md5: ");
        statusFieldsPre.add("task md5: ");
        statusFieldsPre.add("task num: ");
        statusFieldsPre.add("instance num: ");
        statusFieldsPre.add("boot time: ");
        statusFieldsPre.add("send package count: ");
        statusFieldsPre.add("send data len: ");
        statusFieldsPre.add("source permit left: ");
        statusFieldsPre.add("queue permit left: ");
        statusFieldsPre.add("writer permit left: ");
        statusFieldsPre.add("active thread count: ");
    }

    public double twoDecimal(double doubleValue) {
        BigDecimal bigDecimal = new BigDecimal(doubleValue).setScale(2, RoundingMode.HALF_UP);
        return bigDecimal.doubleValue();
    }

    private Long getProcessId() {
        try {
            RuntimeMXBean runtime = ManagementFactory.getRuntimeMXBean();
            String name = runtime.getName();
            String pid = name.substring(0, name.indexOf('@'));
            return Long.parseLong(pid);
        } catch (Exception e) {
            return null;
        }
    }
}