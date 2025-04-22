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

import org.apache.inlong.common.pojo.sort.SortConfig;
import org.apache.inlong.common.pojo.sort.TaskConfig;
import org.apache.inlong.sdk.commons.admin.AdminTask;
import org.apache.inlong.sort.standalone.config.holder.CommonPropertiesHolder;
import org.apache.inlong.sort.standalone.config.holder.v2.SortConfigHolder;
import org.apache.inlong.sort.standalone.metrics.status.SortTaskStatusRepository;

import lombok.extern.slf4j.Slf4j;
import org.apache.flume.Context;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;

import static org.apache.inlong.sort.standalone.utils.Constants.RELOAD_INTERVAL;

@Slf4j
public class SortCluster {

    private Timer reloadTimer;
    private Map<String, SortTask> taskMap = new ConcurrentHashMap<>();
    private List<SortTask> deletingTasks = new ArrayList<>();
    private AdminTask adminTask;

    public void start() {
        try {
            this.reload();
            this.setReloadTimer();
            // start admin task
            this.adminTask = new AdminTask(new Context(CommonPropertiesHolder.get()));
            this.adminTask.start();
        } catch (Exception e) {
            log.error("failed to start sort cluster", e);
        }
    }

    public void close() {
        try {
            this.reloadTimer.cancel();
            // stop sort task
            for (Map.Entry<String, SortTask> entry : this.taskMap.entrySet()) {
                entry.getValue().stop();
            }
            // stop admin task
            if (this.adminTask != null) {
                this.adminTask.stop();
            }
        } catch (Exception e) {
            log.error("failed to close sort cluster", e);
        }
    }

    private void setReloadTimer() {
        reloadTimer = new Timer(true);
        TimerTask task = new TimerTask() {

            public void run() {
                reload();
            }
        };
        long reloadInterval = CommonPropertiesHolder.getLong(RELOAD_INTERVAL, 60000L);
        reloadTimer.schedule(task, new Date(System.currentTimeMillis() + reloadInterval), reloadInterval);
    }

    public void reload() {
        try {
            // get new config
            SortConfig newConfig = SortConfigHolder.getSortConfig();
            if (newConfig == null) {
                return;
            }
            // add new task
            for (TaskConfig taskConfig : newConfig.getTasks()) {
                String newTaskName = taskConfig.getSortTaskName();
                if (taskMap.containsKey(newTaskName)) {
                    continue;
                }
                if (SortTaskStatusRepository.canResumeSortTask(newTaskName)) {
                    SortTaskStatusRepository.resetStatus(newTaskName);
                    SortTask newTask = new SortTask(newTaskName);
                    newTask.start();
                    this.taskMap.put(newTaskName, newTask);
                }
            }
            // remove task
            deletingTasks.clear();
            for (Map.Entry<String, SortTask> entry : taskMap.entrySet()) {
                String taskName = entry.getKey();
                boolean isFound = false;
                for (TaskConfig taskConfig : newConfig.getTasks()) {
                    if (taskName.equals(taskConfig.getSortTaskName())) {
                        isFound = true;
                        break;
                    }
                }
                if (!isFound) {
                    this.deletingTasks.add(entry.getValue());
                }
            }
            // failPauseTask
            for (Map.Entry<String, SortTask> entry : taskMap.entrySet()) {
                String taskName = entry.getKey();
                if (SortTaskStatusRepository.needPauseSortTask(taskName)) {
                    this.deletingTasks.add(entry.getValue());
                }
            }
            // stop deleting task list
            for (SortTask task : deletingTasks) {
                task.stop();
                taskMap.remove(task.getTaskName());
            }
        } catch (Throwable e) {
            log.error("failed to reload cluster", e);
        }
    }
}
