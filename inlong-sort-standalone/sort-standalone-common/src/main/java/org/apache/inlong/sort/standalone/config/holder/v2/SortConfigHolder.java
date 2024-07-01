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
import org.apache.inlong.sort.standalone.config.holder.CommonPropertiesHolder;
import org.apache.inlong.sort.standalone.config.loader.v2.ClassResourceSortConfigLoader;
import org.apache.inlong.sort.standalone.config.loader.v2.ManagerSortConfigLoader;
import org.apache.inlong.sort.standalone.config.loader.v2.SortConfigLoader;
import org.apache.inlong.sort.standalone.config.pojo.InlongId;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.ClassUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.flume.Context;

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
    private SortConfig config;
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
            if (newConfig == null) {
                return;
            }

            // <SortTaskName, <InlongId, AuditTag>>
            this.auditTagMap = newConfig.getTasks()
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
            this.config = newConfig;
        } catch (Throwable e) {
            log.error("failed to reload sort config", e);
        }
    }

    public static SortConfig getSortConfig() {
        return get().config;
    }

    public static TaskConfig getTaskConfig(String sortTaskName) {
        SortConfig config = get().config;
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
