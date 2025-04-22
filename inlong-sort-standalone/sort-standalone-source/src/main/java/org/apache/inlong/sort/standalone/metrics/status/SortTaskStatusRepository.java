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

package org.apache.inlong.sort.standalone.metrics.status;

import org.apache.inlong.common.metric.MetricItemValue;
import org.apache.inlong.common.metric.MetricValue;
import org.apache.inlong.sort.standalone.config.holder.CommonPropertiesHolder;
import org.apache.inlong.sort.standalone.metrics.SortMetricItem;
import org.apache.inlong.sort.standalone.utils.InlongLoggerFactory;

import org.apache.flume.Context;
import org.slf4j.Logger;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * SortTaskStatusRepository
 * 
 */
public class SortTaskStatusRepository {

    public static final Logger LOG = InlongLoggerFactory.getLogger(SortTaskStatusRepository.class);

    public static final String KEY_FAIL_PAUSE_ENABLE = "sorttask.status.failPauseEnable";
    public static final boolean DEFAULT_FAIL_PAUSE_ENABLE = false;
    public static final String KEY_FAIL_COUNT_LIMIT = "sorttask.status.failCountLimit";
    public static final int DEFAULT_FAIL_COUNT_LIMIT = 10;
    public static final String KEY_FAIL_COUNT_PERCENT_LIMIT = "sorttask.status.failCountPercentLimit";
    public static final int DEFAULT_FAIL_COUNT_PERCENT_LIMIT = 50;
    public static final String KEY_PAUSE_INTERVAL_MS = "sorttask.status.pauseIntervalMs";
    public static final long DEFAULT_PAUSE_INTERVAL_MS = 300000;

    private static final AtomicBoolean hasInited = new AtomicBoolean(false);
    private static Context context;
    private static final ConcurrentHashMap<String, SortTaskStatus> statusMap = new ConcurrentHashMap<>();

    private static boolean failPauseEnable;
    private static int failCountLimit;
    private static int failCountPercentLimit;
    private static long pauseIntervalMs;

    public static void init() {
        LOG.info("start to init SortTaskStatusRepository");
        if (!hasInited.compareAndSet(false, true)) {
            return;
        }
        SortTaskStatusRepository.context = CommonPropertiesHolder.getContext();
        SortTaskStatusRepository.failPauseEnable = context.getBoolean(KEY_FAIL_PAUSE_ENABLE, DEFAULT_FAIL_PAUSE_ENABLE);
        SortTaskStatusRepository.failCountLimit = context.getInteger(KEY_FAIL_COUNT_LIMIT, DEFAULT_FAIL_COUNT_LIMIT);
        SortTaskStatusRepository.failCountPercentLimit = context.getInteger(KEY_FAIL_COUNT_PERCENT_LIMIT,
                DEFAULT_FAIL_COUNT_PERCENT_LIMIT);
        SortTaskStatusRepository.pauseIntervalMs = context.getLong(KEY_PAUSE_INTERVAL_MS, DEFAULT_PAUSE_INTERVAL_MS);
    }

    public static void resetStatus(String taskName) {
        statusMap.put(taskName, new SortTaskStatus(taskName));
    }

    public static void acquirePutChannel(String taskName) {
        if (!hasInited.get()) {
            init();
        }
        if (!failPauseEnable) {
            return;
        }
        SortTaskStatus taskStatus = statusMap.computeIfAbsent(taskName, k -> new SortTaskStatus(k));
        if (taskStatus.isHasFirstSuccess()) {
            return;
        }
        try {
            taskStatus.tryFirstSend();
            return;
        } catch (InterruptedException e) {
            LOG.error(e.getMessage(), e);
        }
    }

    public static boolean needPauseSortTask(String taskName) {
        if (!hasInited.get()) {
            init();
        }
        if (!failPauseEnable) {
            return false;
        }
        SortTaskStatus taskStatus = statusMap.computeIfAbsent(taskName, k -> new SortTaskStatus(k));
        return taskStatus.needPauseSortTask(failCountLimit, failCountPercentLimit);
    }

    public static boolean canResumeSortTask(String taskName) {
        if (!hasInited.get()) {
            init();
        }
        if (!failPauseEnable) {
            return true;
        }
        SortTaskStatus taskStatus = statusMap.computeIfAbsent(taskName, k -> new SortTaskStatus(k));
        return taskStatus.canResumeSortTask(pauseIntervalMs);
    }

    public static void snapshot(List<MetricItemValue> itemValues) {
        if (!hasInited.get()) {
            init();
        }
        if (!failPauseEnable) {
            return;
        }
        LOG.info("start to SortTaskStatusRepository status:{}", statusMap);
        for (MetricItemValue itemValue : itemValues) {
            Map<String, String> dimensions = itemValue.getDimensions();
            String taskName = dimensions.get(SortMetricItem.KEY_TASK_NAME);
            if (taskName == null) {
                continue;
            }
            SortTaskStatus taskStatus = statusMap.computeIfAbsent(taskName, k -> new SortTaskStatus(k));
            Map<String, MetricValue> metrics = itemValue.getMetrics();
            MetricValue sCount = metrics.get(SortMetricItem.M_SEND_COUNT);
            if (sCount != null) {
                taskStatus.getSendCount().addAndGet(sCount.value);
            }
            MetricValue sfCount = metrics.get(SortMetricItem.M_SEND_FAIL_COUNT);
            if (sfCount != null) {
                taskStatus.getSendFailCount().addAndGet(sfCount.value);
            }
            MetricValue ssCount = metrics.get(SortMetricItem.M_SEND_SUCCESS_COUNT);
            if (ssCount != null) {
                for (int i = 0; i < ssCount.value; i++) {
                    taskStatus.firstSuccess();
                }
            }
        }
        LOG.info("end to SortTaskStatusRepository status:{}", statusMap);
    }
}
