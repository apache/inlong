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
import org.apache.inlong.agent.conf.ProfileFetcher;
import org.apache.inlong.agent.constant.AgentConstants;
import org.apache.inlong.agent.core.task.TaskManager;
import org.apache.inlong.common.pojo.agent.AgentConfigInfo;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Constructor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Agent Manager, the bridge for task manager, task store e.t.c it manages agent level operations and communicates
 * with outside system.
 */
public class AgentManager extends AbstractDaemon {

    private static final Logger LOGGER = LoggerFactory.getLogger(AgentManager.class);
    private final TaskManager taskManager;
    private final HeartbeatManager heartbeatManager;
    private final ProfileFetcher fetcher;
    private final AgentConfiguration conf;
    private final ExecutorService agentConfMonitor;
    private static AgentConfigInfo agentConfigInfo;

    public AgentManager() {
        conf = AgentConfiguration.getAgentConf();
        agentConfMonitor = Executors.newSingleThreadExecutor();
        taskManager = new TaskManager();
        fetcher = initFetcher(this);
        heartbeatManager = HeartbeatManager.getInstance(this);
    }

    public static AgentConfigInfo getAgentConfigInfo() {
        return agentConfigInfo;
    }

    public void subNewAgentConfigInfo(AgentConfigInfo info) {
        if (info == null) {
            return;
        }
        agentConfigInfo = info;
    }

    /**
     * init fetch by class name
     */
    private ProfileFetcher initFetcher(AgentManager agentManager) {
        try {
            Constructor<?> constructor =
                    Class.forName(conf.get(AgentConstants.AGENT_FETCHER_CLASSNAME))
                            .getDeclaredConstructor(AgentManager.class);
            constructor.setAccessible(true);
            return (ProfileFetcher) constructor.newInstance(agentManager);
        } catch (Exception ex) {
            LOGGER.warn("cannot find fetcher: ", ex);
        }
        return null;
    }

    private Runnable startHotConfReplace() {
        return new Runnable() {

            private long lastModifiedTime = 0L;

            @Override
            public void run() {
                Thread.currentThread().setName("agent-manager-hotConfReplace");
                while (true) {
                    try {
                        Thread.sleep(10 * 1000); // 10s check
                        long maxLastModifiedTime = conf.maxLastModifiedTime();
                        if (maxLastModifiedTime > lastModifiedTime) {
                            conf.reloadFromLocalPropertiesFile();
                            lastModifiedTime = maxLastModifiedTime;
                        }
                    } catch (InterruptedException e) {
                        LOGGER.error("Interrupted when flush agent conf.", e);
                    }
                }
            }
        };
    }

    public ProfileFetcher getFetcher() {
        return fetcher;
    }

    public TaskManager getTaskManager() {
        return taskManager;
    }

    public HeartbeatManager getHeartbeatManager() {
        return heartbeatManager;
    }

    @Override
    public void join() {
        super.join();
        taskManager.join();
    }

    @Override
    public void start() throws Exception {
        LOGGER.info("starting agent manager");
        agentConfMonitor.submit(startHotConfReplace());
        LOGGER.info("starting task manager");
        taskManager.start();
        LOGGER.info("starting heartbeat manager");
        heartbeatManager.start();
        LOGGER.info("starting task position manager");
        LOGGER.info("starting read job from local");
        LOGGER.info("starting fetcher");
        if (fetcher != null) {
            fetcher.start();
        }
        LOGGER.info("starting agent manager end");
    }

    /**
     * It should guarantee thread-safe, and can be invoked many times.
     *
     * @throws Exception exceptions
     */
    @Override
    public void stop() throws Exception {
        if (fetcher != null) {
            fetcher.stop();
        }
        // TODO: change job state which is in running state.
        LOGGER.info("stopping agent manager");
        taskManager.stop();
        heartbeatManager.stop();
        agentConfMonitor.shutdown();
    }
}
