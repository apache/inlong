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

package org.apache.inlong.sort.standalone.v2;

import org.apache.inlong.common.pojo.sort.TaskConfig;
import org.apache.inlong.sort.standalone.PropertiesConfigurationProvider;
import org.apache.inlong.sort.standalone.config.holder.v2.SortConfigHolder;
import org.apache.inlong.sort.standalone.utils.v2.FlumeConfigGenerator;

import com.google.common.eventbus.Subscribe;
import lombok.extern.slf4j.Slf4j;
import org.apache.flume.Channel;
import org.apache.flume.SinkRunner;
import org.apache.flume.SourceRunner;
import org.apache.flume.lifecycle.LifecycleState;
import org.apache.flume.lifecycle.LifecycleSupervisor;
import org.apache.flume.node.MaterializedConfiguration;

import java.util.Map;
import java.util.concurrent.locks.ReentrantLock;

@Slf4j
public class SortTask {

    private final String taskName;
    private final LifecycleSupervisor supervisor;
    private MaterializedConfiguration materializedConfiguration;
    private final ReentrantLock lifecycleLock = new ReentrantLock();

    public SortTask(String taskName) {
        this.taskName = taskName;
        this.supervisor = new LifecycleSupervisor();
    }

    public void start() {
        TaskConfig config = SortConfigHolder.getTaskConfig(taskName);
        if (config == null) {
            return;
        }
        Map<String, String> flumeConfiguration = FlumeConfigGenerator.generateFlumeConfiguration(config);
        log.info("start sort task={}, config={}", taskName, flumeConfiguration);
        PropertiesConfigurationProvider configurationProvider = new PropertiesConfigurationProvider(
                config.getSortTaskName(), flumeConfiguration);
        this.handleConfigurationEvent(configurationProvider.getConfiguration());
    }

    public String getTaskName() {
        return taskName;
    }

    @Subscribe
    public void handleConfigurationEvent(MaterializedConfiguration conf) {
        try {
            lifecycleLock.lockInterruptibly();
            stopAllComponents();
            startAllComponents(conf);
        } catch (InterruptedException e) {
            log.error("interrupted while trying to handle configuration event", e);
        } finally {
            // If interrupted while trying to lock, we don't own the lock, so must not attempt to unlock
            if (lifecycleLock.isHeldByCurrentThread()) {
                lifecycleLock.unlock();
            }
        }
    }

    public void stop() {
        lifecycleLock.lock();
        stopAllComponents();
        try {
            supervisor.stop();
        } finally {
            lifecycleLock.unlock();
        }
    }

    private void stopAllComponents() {
        if (this.materializedConfiguration != null) {
            log.info("shutting down configuration: {}", this.materializedConfiguration);
            for (Map.Entry<String, SourceRunner> entry : this.materializedConfiguration.getSourceRunners().entrySet()) {
                try {
                    log.info("stopping Source " + entry.getKey());
                    supervisor.unsupervise(entry.getValue());
                } catch (Exception e) {
                    log.error("error while stopping {}", entry.getValue(), e);
                }
            }

            for (Map.Entry<String, SinkRunner> entry : this.materializedConfiguration.getSinkRunners().entrySet()) {
                try {
                    log.info("stopping Sink " + entry.getKey());
                    supervisor.unsupervise(entry.getValue());
                } catch (Exception e) {
                    log.error("error while stopping {}", entry.getValue(), e);
                }
            }

            for (Map.Entry<String, Channel> entry : this.materializedConfiguration.getChannels().entrySet()) {
                try {
                    log.info("stopping Channel " + entry.getKey());
                    supervisor.unsupervise(entry.getValue());
                } catch (Exception e) {
                    log.error("error while stopping {}", entry.getValue(), e);
                }
            }
        }
    }

    private void startAllComponents(MaterializedConfiguration materializedConfiguration) {
        log.info("starting new configuration:{}", materializedConfiguration);

        this.materializedConfiguration = materializedConfiguration;

        for (Map.Entry<String, Channel> entry : materializedConfiguration.getChannels().entrySet()) {
            try {
                log.info("starting channel " + entry.getKey());
                supervisor.supervise(entry.getValue(),
                        new LifecycleSupervisor.SupervisorPolicy.AlwaysRestartPolicy(), LifecycleState.START);
            } catch (Exception e) {
                log.error("error while starting {}", entry.getValue(), e);
            }
        }

        // Wait for all channels to start.
        for (Channel ch : materializedConfiguration.getChannels().values()) {
            while (ch.getLifecycleState() != LifecycleState.START
                    && !supervisor.isComponentInErrorState(ch)) {
                try {
                    log.info("waiting for channel: " + ch.getName() + " to start. Sleeping for 500 ms");
                    Thread.sleep(500);
                } catch (InterruptedException e) {
                    log.error("interrupted while waiting for channel to start.", e);
                }
            }
        }

        for (Map.Entry<String, SinkRunner> entry : materializedConfiguration.getSinkRunners().entrySet()) {
            try {
                log.info("starting sink " + entry.getKey());
                supervisor.supervise(entry.getValue(),
                        new LifecycleSupervisor.SupervisorPolicy.AlwaysRestartPolicy(), LifecycleState.START);
            } catch (Exception e) {
                log.error("error while starting {}", entry.getValue(), e);
            }
        }

        for (Map.Entry<String, SourceRunner> entry : materializedConfiguration.getSourceRunners().entrySet()) {
            try {
                log.info("starting source " + entry.getKey());
                supervisor.supervise(entry.getValue(),
                        new LifecycleSupervisor.SupervisorPolicy.AlwaysRestartPolicy(), LifecycleState.START);
            } catch (Exception e) {
                log.error("error while starting {}", entry.getValue(), e);
            }
        }
    }
}
