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
import org.apache.inlong.agent.core.task.MemoryManager;
import org.apache.inlong.agent.core.task.OffsetManager;
import org.apache.inlong.agent.core.task.TaskManager;
import org.apache.inlong.agent.utils.AgentUtils;
import org.apache.inlong.agent.utils.ExcuteLinux;
import org.apache.inlong.sdk.dataproxy.DefaultMessageSender;
import org.apache.inlong.sdk.dataproxy.common.SendResult;

import com.google.common.collect.Lists;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.logging.log4j.util.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import java.lang.management.ThreadMXBean;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.apache.inlong.agent.constant.AgentConstants.AGENT_CLUSTER_NAME;
import static org.apache.inlong.agent.constant.AgentConstants.AGENT_CLUSTER_TAG;
import static org.apache.inlong.agent.constant.AgentConstants.AGENT_INSTALL_PLATFORM;
import static org.apache.inlong.agent.constant.FetcherConstants.AGENT_GLOBAL_READER_QUEUE_PERMIT;
import static org.apache.inlong.agent.constant.FetcherConstants.AGENT_GLOBAL_READER_SOURCE_PERMIT;
import static org.apache.inlong.agent.constant.FetcherConstants.AGENT_GLOBAL_WRITER_PERMIT;

/**
 * Collect various indicators of agent processes for backend problem analysis
 */
public class AgentStatusManager {

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static class AgentStatus {

        private String agentIp;
        private String tag;
        private String cluster;
        private String agentVersion;
        private String agentStartTime;
        private String cpuCore;
        private String procCpu;
        private String freeMem;
        private String maxMem;
        private String useMem;
        private String os;
        private String installPlatform;
        private String usrDir;
        private String usrName;
        private String processId;
        private String globalConfigMd5;
        private String taskMd5;
        private String taskNum;
        private String instanceNum;
        private String bootTime;
        private String sendPackageCount;
        private String sendDataLen;
        private String sourcePermitLeft;
        private String queuePermitLeft;
        private String writerPermitLeft;
        private String activeThreadCount;

        public String getFieldsString() {
            List<String> fields = Lists.newArrayList();
            fields.add(agentIp);
            fields.add(tag);
            fields.add(cluster);
            fields.add(agentVersion);
            fields.add(agentStartTime);
            fields.add(cpuCore);
            fields.add(procCpu);
            fields.add(freeMem);
            fields.add(maxMem);
            fields.add(useMem);
            fields.add(os);
            fields.add(installPlatform);
            fields.add(usrDir);
            fields.add(usrName);
            fields.add(processId);
            fields.add(globalConfigMd5);
            fields.add(taskMd5);
            fields.add(taskNum);
            fields.add(instanceNum);
            fields.add(bootTime);
            fields.add(sendPackageCount);
            fields.add(sendDataLen);
            fields.add(sourcePermitLeft);
            fields.add(queuePermitLeft);
            fields.add(writerPermitLeft);
            fields.add(activeThreadCount);
            return Strings.join(fields, ',');
        }
    }

    private static final Logger LOGGER = LoggerFactory.getLogger(AgentStatusManager.class);
    public static final String INLONG_AGENT_SYSTEM = "inlong_agent_system";
    public static final String INLONG_AGENT_STATUS = "inlong_agent_status";

    private static AgentStatusManager manager = null;
    private final AgentConfiguration conf;
    private final SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    private Runtime runtime = Runtime.getRuntime();
    final long GB = 1024 * 1024 * 1024;
    private ThreadMXBean threadBean;
    private final long INVALID_CPU = -1;
    private RuntimeMXBean runtimeMXBean = ManagementFactory.getRuntimeMXBean();
    private ThreadMXBean threadMXBean = ManagementFactory.getThreadMXBean();
    private AgentManager agentManager;
    public static AtomicLong sendDataLen = new AtomicLong();
    public static AtomicLong sendPackageCount = new AtomicLong();
    private String processStartupTime = format.format(runtimeMXBean.getStartTime());
    private String systemStartupTime = ExcuteLinux.exeCmd("uptime -s").replaceAll("\r|\n", "");

    private AgentStatusManager(AgentManager agentManager) {
        this.agentManager = agentManager;
        this.conf = AgentConfiguration.getAgentConf();
        threadBean = ManagementFactory.getThreadMXBean();
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

    public void sendStatusMsg(DefaultMessageSender sender) {
        AgentStatus data = AgentStatusManager.getInstance().getStatus();
        LOGGER.info("status detail: {}", data);
        if (sender == null) {
            return;
        }
        SendResult ret = sender.sendMessage(data.getFieldsString().getBytes(StandardCharsets.UTF_8),
                INLONG_AGENT_SYSTEM,
                INLONG_AGENT_STATUS,
                AgentUtils.getCurrentTime(),
                "", 30, TimeUnit.SECONDS);
        if (ret != SendResult.OK) {
            LOGGER.error("send status failed: ret {}", ret);
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

    private AgentStatus getStatus() {
        AgentStatus data = new AgentStatus();
        data.setAgentIp(AgentUtils.fetchLocalIp());
        data.setTag(conf.get(AGENT_CLUSTER_TAG));
        data.setCluster(conf.get(AGENT_CLUSTER_NAME));
        data.setAgentVersion(TaskManager.class.getPackage().getImplementationVersion());
        data.setAgentStartTime(processStartupTime);
        data.setCpuCore(String.valueOf(runtime.availableProcessors()));
        data.setProcCpu(String.valueOf(twoDecimal(getProcessCpu())));
        data.setFreeMem(String.valueOf(twoDecimal((double) runtime.freeMemory() / GB)));
        data.setMaxMem(String.valueOf(twoDecimal((double) runtime.maxMemory() / GB)));
        data.setUseMem(String.valueOf(twoDecimal((double) runtime.totalMemory() / GB)));
        data.setOs(System.getProperty("os.version"));
        data.setInstallPlatform(conf.get(AGENT_INSTALL_PLATFORM, ""));
        data.setUsrDir(System.getProperty("user.dir"));
        data.setUsrName(System.getProperty("user.name"));
        data.setProcessId(String.valueOf(getProcessId()));
        if (AgentManager.getAgentConfigInfo() != null) {
            data.setGlobalConfigMd5(AgentManager.getAgentConfigInfo().getMd5());
        }
        data.setTaskMd5(agentManager.getTaskManager().getTaskResultMd5());
        data.setTaskNum(String.valueOf(agentManager.getTaskManager().getTaskStore().getTasks().size()));
        data.setInstanceNum(String.valueOf(OffsetManager.getInstance().getRunningInstanceCount()));
        data.setBootTime(systemStartupTime);
        data.setSendPackageCount(String.valueOf(sendPackageCount.getAndSet(0)));
        data.setSendDataLen(String.valueOf(sendDataLen.getAndSet(0)));
        data.setSourcePermitLeft(
                String.valueOf(MemoryManager.getInstance().getLeft(AGENT_GLOBAL_READER_SOURCE_PERMIT)));
        data.setQueuePermitLeft(String.valueOf(MemoryManager.getInstance().getLeft(AGENT_GLOBAL_READER_QUEUE_PERMIT)));
        data.setWriterPermitLeft(String.valueOf(MemoryManager.getInstance().getLeft(AGENT_GLOBAL_WRITER_PERMIT)));
        data.setActiveThreadCount(String.valueOf(threadMXBean.getThreadCount()));
        return data;
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