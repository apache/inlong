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

package org.apache.inlong.audit.store.metric;

import org.apache.inlong.audit.file.ConfigManager;
import org.apache.inlong.audit.metric.AbstractMetric;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static org.apache.inlong.audit.store.config.ConfigConstants.DEFAULT_STORE_METRIC_CLASSNAME;
import static org.apache.inlong.audit.store.config.ConfigConstants.KEY_STORE_METRIC_CLASSNAME;

public class MetricsManager {

    private static final Logger LOGGER = LoggerFactory.getLogger(MetricsManager.class);

    private static class Holder {

        private static final MetricsManager INSTANCE = new MetricsManager();
    }

    private AbstractMetric metric;

    public void init() {
        try {
            String metricClassName =
                    ConfigManager.getInstance().getValue(KEY_STORE_METRIC_CLASSNAME, DEFAULT_STORE_METRIC_CLASSNAME);
            LOGGER.info("Metric class name: {}", metricClassName);
            Constructor<?> constructor = Class.forName(metricClassName)
                    .getDeclaredConstructor(MetricItem.class);
            constructor.setAccessible(true);
            metric = (AbstractMetric) constructor.newInstance(metricItem);

            timer.scheduleWithFixedDelay(() -> {
                metric.report();
                metricItem.resetAllMetrics();
            }, 0, 1, TimeUnit.MINUTES);
        } catch (ClassNotFoundException | NoSuchMethodException | InstantiationException | IllegalAccessException
                | InvocationTargetException exception) {
            LOGGER.error("Init metrics manager has exception: ", exception);
        }
    }

    public static MetricsManager getInstance() {
        return Holder.INSTANCE;
    }

    private final MetricItem metricItem = new MetricItem();
    protected final ScheduledExecutorService timer = Executors.newSingleThreadScheduledExecutor();

    public void addReceiveSuccess(long count) {
        metricItem.getReceiveCountSuccess().addAndGet(count);
    }

    public void addReceiveFailed(long pack) {
        metricItem.getReceiveFailed().addAndGet(pack);
    }

    public void addSendSuccess(long count, long duration) {
        metricItem.getSendCountSuccess().addAndGet(count);
        metricItem.getSendDuration().addAndGet(duration);
    }

    public void addSendFailed(long count, long duration) {
        metricItem.getSendCountFailed().addAndGet(count);
        metricItem.getSendDuration().addAndGet(duration);
    }

    public void shutdown() {
        timer.shutdown();
        metric.stop();
    }
}
