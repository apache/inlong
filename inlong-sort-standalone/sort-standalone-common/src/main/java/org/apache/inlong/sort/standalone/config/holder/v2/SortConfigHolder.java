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

package org.apache.inlong.sort.standalone.config.holder.v2;

import org.apache.inlong.common.pojo.sort.ClusterTagConfig;
import org.apache.inlong.common.pojo.sort.SortConfig;
import org.apache.inlong.common.pojo.sort.TaskConfig;
import org.apache.inlong.common.pojo.sort.dataflow.DataFlowConfig;
import org.apache.inlong.common.pojo.sort.mq.MqClusterConfig;
import org.apache.inlong.sort.standalone.config.holder.CommonPropertiesHolder;
import org.apache.inlong.sort.standalone.config.loader.v2.ClassResourceSortConfigLoader;
import org.apache.inlong.sort.standalone.config.loader.v2.ManagerSortConfigLoader;
import org.apache.inlong.sort.standalone.config.loader.v2.SortConfigLoader;
import org.apache.inlong.sort.standalone.config.pojo.InlongId;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.ClassUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.flume.Context;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.stream.Collectors;

import static org.apache.inlong.sort.standalone.utils.Constants.RELOAD_INTERVAL;

@Slf4j
public class SortConfigHolder {

    private static SortConfigHolder instance;

    private long reloadInterval;
    private Timer reloadTimer;
    private SortConfigLoader loader;
    private SortConfig lastConfig = null;
    private SortConfig currentConfig = null;
    private SortConfig finalConfig = null;
    private Map<String, Map<String, String>> auditTagMap;

    private SortConfigHolder() {
        this.auditTagMap = new HashMap<>();
    }

    private static SortConfigHolder get() {
        if (instance != null) {
            return instance;
        }

        synchronized (SortConfigHolder.class) {
            if (instance != null) {
                return instance;
            }

            instance = new SortConfigHolder();
            instance.reloadInterval = CommonPropertiesHolder.getLong(RELOAD_INTERVAL, 60000L);
            String loaderType = CommonPropertiesHolder
                    .getString(SortConfigType.KEY_TYPE, SortConfigType.MANAGER.name());

            if (SortConfigType.FILE.name().equalsIgnoreCase(loaderType)) {
                instance.loader = new ClassResourceSortConfigLoader();
            } else if (SortConfigType.MANAGER.name().equalsIgnoreCase(loaderType)) {
                instance.loader = new ManagerSortConfigLoader();
            } else {
                // user-defined
                try {
                    Class<?> loaderClass = ClassUtils.getClass(loaderType);
                    Object loaderObject = loaderClass.getDeclaredConstructor().newInstance();
                    if (loaderObject instanceof SortConfigLoader) {
                        instance.loader = (SortConfigLoader) loaderObject;
                    }
                } catch (Throwable t) {
                    log.error("failed to init loader, loaderType={}", loaderType);
                }
            }
            if (instance.loader == null) {
                instance.loader = new ClassResourceSortConfigLoader();
            }
            try {
                instance.loader.configure(new Context(CommonPropertiesHolder.get()));
                instance.reload();
                instance.setReloadTimer();
            } catch (Exception e) {
                log.error("failed to reload instance", e);
            }
        }
        return instance;

    }

    private void setReloadTimer() {
        reloadTimer = new Timer(true);
        TimerTask task = new TimerTask() {

            public void run() {
                reload();
            }
        };
        reloadTimer.schedule(task, new Date(System.currentTimeMillis() + reloadInterval), reloadInterval);
    }

    private void reload() {
        try {
            SortConfig newConfig = this.loader.load();
            if (newConfig == null && currentConfig == null) {
                return;
            }
            this.lastConfig = currentConfig;
            this.currentConfig = newConfig;
            SortConfig finalConfig = new SortConfig();
            finalConfig.setTasks(new ArrayList<>());
            if (this.lastConfig != null) {
                this.mergeSortConfig(finalConfig, lastConfig);
            }
            if (this.currentConfig != null) {
                this.mergeSortConfig(finalConfig, currentConfig);
            }

            // <SortTaskName, <InlongId, AuditTag>>
            this.auditTagMap = finalConfig.getTasks()
                    .stream()
                    .collect(Collectors.toMap(TaskConfig::getSortTaskName,
                            v -> v.getClusterTagConfigs()
                                    .stream()
                                    .map(ClusterTagConfig::getDataFlowConfigs)
                                    .flatMap(Collection::stream)
                                    .filter(flow -> StringUtils.isNotEmpty(flow.getAuditTag()))
                                    .collect(Collectors.toMap(flow -> InlongId.generateUid(flow.getInlongGroupId(),
                                            flow.getInlongStreamId()),
                                            DataFlowConfig::getAuditTag,
                                            (flow1, flow2) -> flow1))));
            this.finalConfig = finalConfig;
        } catch (Throwable e) {
            log.error("failed to reload sort config", e);
        }
    }

    /**
     * mergeSortConfig
     */
    private void mergeSortConfig(SortConfig finalConfig, SortConfig appendConfig) {
        if (appendConfig == null) {
            return;
        }
        finalConfig.setSortClusterName(appendConfig.getSortClusterName());
        Map<String, TaskConfig> finalMap = new HashMap<>();
        finalConfig.getTasks().forEach(v -> finalMap.put(v.getSortTaskName(), v));
        for (TaskConfig taskConfig : appendConfig.getTasks()) {
            String taskName = taskConfig.getSortTaskName();
            TaskConfig finalTask = finalMap.get(taskName);
            // new
            if (finalTask == null) {
                finalMap.put(taskName, taskConfig);
                continue;
            }
            this.mergeTaskConfig(finalTask, taskConfig);
        }
        finalConfig.getTasks().clear();
        finalConfig.getTasks().addAll(finalMap.values());
    }

    private void mergeTaskConfig(TaskConfig finalConfig, TaskConfig appendConfig) {
        if (appendConfig == null) {
            return;
        }
        finalConfig.setSortTaskName(appendConfig.getSortTaskName());
        finalConfig.setNodeConfig(appendConfig.getNodeConfig());
        Map<String, ClusterTagConfig> finalMap = new HashMap<>();
        finalConfig.getClusterTagConfigs().forEach(v -> finalMap.put(v.getClusterTag(), v));
        for (ClusterTagConfig tagConfig : appendConfig.getClusterTagConfigs()) {
            String clusterTag = tagConfig.getClusterTag();
            ClusterTagConfig finalTag = finalMap.get(clusterTag);
            // new
            if (finalTag == null) {
                finalMap.put(clusterTag, tagConfig);
                continue;
            }
            this.mergeTagConfig(finalTag, tagConfig);
        }
        finalConfig.getClusterTagConfigs().clear();
        finalConfig.getClusterTagConfigs().addAll(finalMap.values());
    }

    private void mergeTagConfig(ClusterTagConfig finalConfig, ClusterTagConfig appendConfig) {
        if (appendConfig == null) {
            return;
        }
        finalConfig.setClusterTag(appendConfig.getClusterTag());
        // mqClusterConfigs
        Map<String, MqClusterConfig> finalMqMap = new HashMap<>();
        finalConfig.getMqClusterConfigs().forEach(v -> finalMqMap.put(v.getClusterName(), v));
        appendConfig.getMqClusterConfigs().forEach(v -> finalMqMap.put(v.getClusterName(), v));
        finalConfig.getMqClusterConfigs().clear();
        finalConfig.getMqClusterConfigs().addAll(finalMqMap.values());
        // dataFlowConfigs
        Map<String, DataFlowConfig> finalMap = new HashMap<>();
        finalConfig.getDataFlowConfigs().forEach(v -> finalMap.put(v.getDataflowId(), v));
        appendConfig.getDataFlowConfigs().forEach(v -> finalMap.put(v.getDataflowId(), v));
        finalConfig.getDataFlowConfigs().clear();
        finalConfig.getDataFlowConfigs().addAll(finalMap.values());
    }

    public static SortConfig getSortConfig() {
        return get().finalConfig;
    }

    public static TaskConfig getTaskConfig(String sortTaskName) {
        SortConfig config = get().finalConfig;
        if (config != null && config.getTasks() != null) {
            for (TaskConfig task : config.getTasks()) {
                if (StringUtils.equals(sortTaskName, task.getSortTaskName())) {
                    return task;
                }
            }
        }
        return null;
    }

    public static String getAuditTag(
            String sortTaskName,
            String inlongGroupId,
            String inlongStreamId) {
        Map<String, String> taskMap = get().auditTagMap.get(sortTaskName);
        if (taskMap == null) {
            return null;
        }
        return taskMap.get(InlongId.generateUid(inlongGroupId, inlongStreamId));
    }
}
