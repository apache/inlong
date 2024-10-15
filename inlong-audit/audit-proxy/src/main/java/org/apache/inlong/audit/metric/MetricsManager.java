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

package org.apache.inlong.audit.metric;

import org.apache.inlong.audit.file.ConfigManager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static org.apache.inlong.audit.config.ConfigConstants.DEFAULT_PROMETHEUS_PORT;
import static org.apache.inlong.audit.config.ConfigConstants.DEFAULT_PROXY_METRIC_CLASSNAME;
import static org.apache.inlong.audit.config.ConfigConstants.KEY_PROMETHEUS_PORT;
import static org.apache.inlong.audit.config.ConfigConstants.KEY_PROXY_METRIC_CLASSNAME;

public class MetricsManager {

    private static final Logger LOGGER = LoggerFactory.getLogger(MetricsManager.class);

    private static class Holder {

        private static final MetricsManager INSTANCE = new MetricsManager();
    }

    private AbstractMetric metric;

    public void init(String metricName) {
        try {
            ConfigManager configManager = ConfigManager.getInstance();
            String metricClassName = configManager.getValue(KEY_PROXY_METRIC_CLASSNAME, DEFAULT_PROXY_METRIC_CLASSNAME);
            LOGGER.info("Metric class name: {}", metricClassName);
            Constructor<?> constructor = Class.forName(metricClassName)
                    .getDeclaredConstructor(String.class, MetricItem.class, int.class);
            constructor.setAccessible(true);
            metric = (AbstractMetric) constructor.newInstance(metricName, metricItem,
                    configManager.getValue(KEY_PROMETHEUS_PORT, DEFAULT_PROMETHEUS_PORT));

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

    public void addReceiveCountInvalid(long count) {
        metricItem.getReceiveCountInvalid().addAndGet(count);
    }

    public void addReceiveCountExpired(long count) {
        metricItem.getReceiveCountExpired().addAndGet(count);
    }

    public void addReceiveSuccess(long count, long pack, long size) {
        metricItem.getReceiveCountSuccess().addAndGet(count);
        metricItem.getReceivePackSuccess().addAndGet(pack);
        metricItem.getReceiveSizeSuccess().addAndGet(size);
    }

    public void addSendSuccess(long count) {
        metricItem.getSendCountSuccess().addAndGet(count);
    }
    public void addSendFailed(long count) {
        metricItem.getSendCountFailed().addAndGet(count);
    }
    public void shutdown() {
        timer.shutdown();
    }
}
