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

package org.apache.inlong.agent.plugin.sinks.dataproxy;

import org.apache.inlong.agent.common.AbstractDaemon;
import org.apache.inlong.agent.conf.InstanceProfile;
import org.apache.inlong.agent.core.task.OffsetManager;
import org.apache.inlong.agent.core.task.TaskManager;
import org.apache.inlong.agent.utils.AgentUtils;
import org.apache.inlong.agent.utils.ThreadUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentHashMap;

public class SenderManager extends AbstractDaemon {

    private static final Logger LOGGER = LoggerFactory.getLogger(SenderManager.class);
    public static final int CORE_THREAD_SLEEP_TIME_MS = 60 * 1000;
    private static volatile SenderManager SenderManager = null;
    private final ConcurrentHashMap<String, Sender> senderMap = new ConcurrentHashMap<>();

    private SenderManager() {
    }

    public static SenderManager getInstance() {
        if (SenderManager == null) {
            synchronized (OffsetManager.class) {
                if (SenderManager == null) {
                    SenderManager = new SenderManager();
                    try {
                        SenderManager.start();
                    } catch (Exception e) {
                        LOGGER.error("start sender manager failed:", e);
                    }
                }
            }
        }
        return SenderManager;
    }

    public Sender getSender(String taskId, InstanceProfile profile) {
        Sender sender = senderMap.get(taskId);
        if (sender == null) {
            synchronized (OffsetManager.class) {
                if (sender == null) {
                    createSender(taskId, profile);
                }
                return senderMap.get(taskId);
            }
        } else {
            return sender;
        }
    }

    private void createSender(String taskId, InstanceProfile profile) {
        Sender sender = new Sender(profile, profile.getInlongGroupId(), profile.getInstanceId());
        try {
            sender.Start();
        } catch (Throwable ex) {
            LOGGER.error("error while init sender for group id {}", profile.getInstanceId());
            ThreadUtils.threadThrowableHandler(Thread.currentThread(), ex);
            throw new IllegalStateException(ex);
        }
        senderMap.put(taskId, sender);
    }

    private Runnable coreThread() {
        return () -> {
            Thread.currentThread().setName("sender-manager-core-thread");
            while (isRunnable()) {
                try {
                    AgentUtils.silenceSleepInMs(CORE_THREAD_SLEEP_TIME_MS);
                    senderMap.entrySet().forEach(entry -> {
                        if (TaskManager.getTaskStore().getTask(entry.getKey()) == null) {
                            Sender sender = senderMap.remove(entry.getKey());
                            sender.Stop();
                            LOGGER.info("task {} not exist, remove the related sender", entry.getKey());
                        }
                    });
                } catch (Throwable ex) {
                    LOGGER.error("sender-manager-core-thread: ", ex);
                    ThreadUtils.threadThrowableHandler(Thread.currentThread(), ex);
                }
            }
        };
    }

    @Override
    public void start() throws Exception {
        submitWorker(coreThread());
    }

    @Override
    public void stop() throws Exception {

    }
}
