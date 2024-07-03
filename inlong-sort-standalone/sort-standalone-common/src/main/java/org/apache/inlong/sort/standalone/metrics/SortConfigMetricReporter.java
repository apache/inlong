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

package org.apache.inlong.sort.standalone.metrics;

import org.apache.inlong.common.pojo.sort.SortConfig;
import org.apache.inlong.sort.standalone.config.holder.CommonPropertiesHolder;
import org.apache.inlong.sort.standalone.config.pojo.IdConfig;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.ClassUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.flume.Context;
import org.apache.flume.conf.Configurable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

@Slf4j
public class SortConfigMetricReporter {

    private static final String KEY_SORT_CONFIG_METRIC_LISTENER = "sortConfig.metricListener";
    private static final AtomicBoolean isInited = new AtomicBoolean(false);
    private static List<SortConfigMetricListener> listeners;

    public static void init(Map<String, String> commonProperties) {
        if (!isInited.compareAndSet(false, true)) {
            return;
        }

        String listenerStr = commonProperties.get(KEY_SORT_CONFIG_METRIC_LISTENER);
        if (StringUtils.isBlank(listenerStr)) {
            log.warn("There is no specified SortConfigMetricListener");
            listeners = new ArrayList<>();
            return;
        }
        String[] listenerList = listenerStr.split("\\s+");
        listeners = Arrays.stream(listenerList)
                .map(SortConfigMetricReporter::loadConfigMetricReporter)
                .filter(Objects::nonNull)
                .collect(Collectors.toList());
        log.info("SortConfigMetricListeners={}", listeners);
    }

    private static SortConfigMetricListener loadConfigMetricReporter(String type) {
        if (StringUtils.isEmpty(type)) {
            log.warn("There is no specified SortConfigMetricReporter type");
            return null;
        }
        log.info("Create SortConfigMetricReporter:{}", type);
        try {
            Class<?> loaderClass = ClassUtils.getClass(type);
            Object loaderObject = loaderClass.getDeclaredConstructor().newInstance();
            if (loaderObject instanceof Configurable) {
                ((Configurable) loaderObject).configure(new Context(CommonPropertiesHolder.get()));
            }
            if (!(loaderObject instanceof SortConfigMetricListener)) {
                log.error("Got exception when create SortConfigMetricReporter instance, config class:{}", type);
                return null;
            }
            return (SortConfigMetricListener) loaderObject;
        } catch (Exception e) {
            log.info("failed to load SortConfigMetricReporter, type={}", type);
            return null;
        }
    }

    public static void reportOffline(SortConfig sortConfig) {
        listeners.forEach(listener -> listener.reportOffline(sortConfig));
    }

    public static void reportOnline(SortConfig sortConfig) {
        listeners.forEach(listener -> listener.reportOnline(sortConfig));
    }

    public static void reportUpdate(SortConfig sortConfig) {
        listeners.forEach(listener -> listener.reportUpdate(sortConfig));
    }

    public static void reportParseFail(String dataflowId) {
        listeners.forEach(listener -> listener.reportParseFail(dataflowId));
    }

    public static void reportRequestConfigFail() {
        listeners.forEach(SortConfigMetricListener::reportRequestConfigFail);
    }

    public static void reportDecompressFail() {
        listeners.forEach(SortConfigMetricListener::reportDecompressFail);
    }

    public static void reportCheckFail() {
        listeners.forEach(SortConfigMetricListener::reportCheckFail);
    }

    public static void reportRequestNoUpdate() {
        listeners.forEach(SortConfigMetricListener::reportRequestNoUpdate);
    }

    public static void reportRequestUpdate() {
        listeners.forEach(SortConfigMetricListener::reportRequestUpdate);
    }

    public static void reportClusterDiff(
            String sortClusterName,
            String sortTaskName,
            Map<String, ? extends IdConfig> fromTaskConfig,
            Map<String, ? extends IdConfig> fromSortTaskConfig) {
        Collection<String> intersection = CollectionUtils.intersection(fromTaskConfig.keySet(),
                fromSortTaskConfig.keySet());
        List<IdConfig> diff = intersection.stream()
                .filter(k -> !fromTaskConfig.get(k).equals(fromSortTaskConfig.get(k)))
                .map(fromSortTaskConfig::get)
                .collect(Collectors.toList());
        // report diff
        diff.forEach(idConfig -> {
            listeners.forEach(listener -> listener.reportClusterDiff(sortClusterName, sortTaskName,
                    idConfig.getInlongGroupId(), idConfig.getInlongStreamId()));
        });

        // report miss in sort cluster config
        fromTaskConfig.forEach((k, v) -> {
            if (!intersection.contains(k)) {
                listeners.forEach(listener -> listener.reportMissInSortClusterConfig(sortClusterName, sortTaskName,
                        v.getInlongGroupId(), v.getInlongStreamId()));
            }
        });

        // report miss in sort config
        fromSortTaskConfig.forEach((k, v) -> {
            if (!intersection.contains(k)) {
                listeners.forEach(listener -> listener.reportMissInSortConfig(sortClusterName, sortTaskName,
                        v.getInlongGroupId(), v.getInlongStreamId()));
            }
        });
    }

    public static void reportSourceDiff(
            String sortClusterName, String sortTaskName,
            String topic, String mqClusterName) {
        listeners.forEach(listener -> listener.reportSourceDiff(sortClusterName, sortTaskName, topic, mqClusterName));
    }

    public static void reportSourceMissInSortClusterConfig(
            String sortClusterName, String sortTaskName,
            String topic, String mqClusterName) {
        listeners.forEach(listener -> listener.reportSourceMissInSortClusterConfig(sortClusterName, sortTaskName, topic,
                mqClusterName));
    }

    public static void reportSourceMissInSortConfig(
            String sortClusterName, String sortTaskName,
            String topic, String mqClusterName) {
        listeners.forEach(
                listener -> listener.reportSourceMissInSortConfig(sortClusterName, sortTaskName, topic, mqClusterName));
    }
}
