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

package org.apache.inlong.agent.installer;

import org.apache.inlong.agent.common.AbstractDaemon;
import org.apache.inlong.agent.installer.conf.InstallerConfiguration;
import org.apache.inlong.agent.metrics.audit.AuditUtils;
import org.apache.inlong.agent.utils.AgentUtils;
import org.apache.inlong.agent.utils.HttpManager;
import org.apache.inlong.agent.utils.ThreadUtils;
import org.apache.inlong.common.pojo.agent.installer.ConfigResult;
import org.apache.inlong.common.pojo.agent.installer.ModuleConfig;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import static org.apache.inlong.agent.constant.FetcherConstants.AGENT_MANAGER_REQUEST_TIMEOUT;
import static org.apache.inlong.agent.constant.FetcherConstants.AGENT_MANAGER_VIP_HTTP_PREFIX_PATH;
import static org.apache.inlong.agent.constant.FetcherConstants.DEFAULT_AGENT_MANAGER_REQUEST_TIMEOUT;
import static org.apache.inlong.agent.constant.FetcherConstants.DEFAULT_AGENT_MANAGER_VIP_HTTP_PREFIX_PATH;

/**
 * Installer Manager, the bridge for job manager, task manager, db e.t.c it manages agent level operations and
 * communicates with outside system.
 */
public class ModuleManager extends AbstractDaemon {

    public static final String MANAGER_ADDR = "manager.addr";
    public static final String MANAGER_AUTH_SECRET_ID = "manager.auth.secretId";
    public static final String MANAGER_AUTH_SECRET_KEY = "manager.auth.secretKey";
    public static final int CONFIG_QUEUE_CAPACITY = 1;
    public static final int CORE_THREAD_SLEEP_TIME = 1000;
    private static final Logger LOGGER = LoggerFactory.getLogger(ModuleManager.class);
    private final InstallerConfiguration conf;
    private final BlockingQueue<ConfigResult> configQueue;
    private String currentMd5 = "";
    private String currentStoragePath = "/tmp";
    private ClassLoader classLoader;
    private static final GsonBuilder gsonBuilder = new GsonBuilder().setDateFormat("yyyy-MM-dd HH:mm:ss");
    private static final Gson GSON = gsonBuilder.create();
    private final HttpManager httpManager;

    public ModuleManager() {
        conf = InstallerConfiguration.getInstallerConf();
        configQueue = new LinkedBlockingQueue<>(CONFIG_QUEUE_CAPACITY);
        classLoader = Thread.currentThread().getContextClassLoader();
        if (classLoader == null) {
            classLoader = ModuleManager.class.getClassLoader();
        }
        if (requiredKeys(conf)) {
            httpManager = getHttpManager(conf);
        } else {
            throw new RuntimeException("init module manager error, cannot find required key");
        }
    }

    public HttpManager getHttpManager(InstallerConfiguration conf) {
        String managerAddr = conf.get(MANAGER_ADDR);
        String managerHttpPrefixPath = conf.get(AGENT_MANAGER_VIP_HTTP_PREFIX_PATH,
                DEFAULT_AGENT_MANAGER_VIP_HTTP_PREFIX_PATH);
        int timeout = conf.getInt(AGENT_MANAGER_REQUEST_TIMEOUT,
                DEFAULT_AGENT_MANAGER_REQUEST_TIMEOUT);
        String secretId = conf.get(MANAGER_AUTH_SECRET_ID);
        String secretKey = conf.get(MANAGER_AUTH_SECRET_KEY);
        return new HttpManager(managerAddr, managerHttpPrefixPath, timeout, secretId, secretKey);
    }

    private boolean requiredKeys(InstallerConfiguration conf) {
        return conf.hasKey(MANAGER_ADDR);
    }

    public void submitConfig(ConfigResult config) {
        if (config == null) {
            return;
        }
        configQueue.clear();
        for (int i = 0; i < config.getModuleList().size(); i++) {
            LOGGER.info("submitModules index {} total {} {}", i, config.getModuleList().size(),
                    config.getModuleList().get(i));
        }
        configQueue.add(config);
    }

    public String getCurrentMd5() {
        return currentMd5;
    }

    /**
     * thread for core thread.
     *
     * @return runnable profile.
     */
    private Runnable coreThread() {
        return () -> {
            Thread.currentThread().setName("task-manager-core");
            while (isRunnable()) {
                try {
                    AgentUtils.silenceSleepInMs(CORE_THREAD_SLEEP_TIME);
                    dealWithConfigQueue(configQueue);
                    AuditUtils.add(AuditUtils.AUDIT_ID_AGENT_TASK_MGR_HEARTBEAT, "", "",
                            AgentUtils.getCurrentTime(), 1, 1);
                } catch (Throwable ex) {
                    LOGGER.error("exception caught", ex);
                    ThreadUtils.threadThrowableHandler(Thread.currentThread(), ex);
                }
            }
        };
    }

    private void dealWithConfigQueue(BlockingQueue<ConfigResult> queue) {
        ConfigResult config = queue.poll();
        if (config == null) {
            return;
        }
        LOGGER.info("Deal with config {}", config);
        if (currentMd5.compareTo(config.getMd5()) == 0) {
            LOGGER.info("md5 no change {}, skip update", currentMd5);
            return;
        }
        if (updateModules(config.getModuleList())) {
            currentMd5 = config.getMd5();
        } else {
            LOGGER.error("Update modules failed!");
        }
    }

    private boolean updateModules(List<ModuleConfig> modules) {
        return true;
    }

    @Override
    public void start() throws Exception {
        submitWorker(coreThread());
    }

    @Override
    public void join() {
        super.join();
    }

    /**
     * It should guarantee thread-safe, and can be invoked many times.
     *
     * @throws Exception exceptions
     */
    @Override
    public void stop() throws Exception {

        LOGGER.info("stopping installer manager");
    }
}
