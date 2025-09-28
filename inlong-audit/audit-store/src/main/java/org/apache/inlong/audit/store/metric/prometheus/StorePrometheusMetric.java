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

package org.apache.inlong.audit.store.metric.prometheus;

import org.apache.inlong.audit.file.ConfigManager;
import org.apache.inlong.audit.metric.AbstractMetric;
import org.apache.inlong.audit.store.metric.MetricDimension;
import org.apache.inlong.audit.store.metric.MetricItem;

import io.prometheus.client.Collector;
import io.prometheus.client.exporter.HTTPServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.apache.inlong.audit.store.config.ConfigConstants.AUDIT_STORE_SERVER_NAME;
import static org.apache.inlong.audit.store.config.ConfigConstants.DEFAULT_PROMETHEUS_PORT;
import static org.apache.inlong.audit.store.config.ConfigConstants.KEY_PROMETHEUS_PORT;

/**
 * PrometheusMetric
 */
public class StorePrometheusMetric extends Collector implements AbstractMetric {

    private static final Logger LOGGER = LoggerFactory.getLogger(StorePrometheusMetric.class);
    private static final String HELP_DESCRIPTION = "help";

    private final MetricItem metricItem;
    private HTTPServer server;

    public StorePrometheusMetric(MetricItem metricItem) {
        this.metricItem = metricItem;
        try {
            server = new HTTPServer(ConfigManager.getInstance().getValue(KEY_PROMETHEUS_PORT, DEFAULT_PROMETHEUS_PORT));
            this.register();
        } catch (IOException e) {
            LOGGER.error("Construct store prometheus metric has IOException", e);
        }
    }

    @Override
    public List<MetricFamilySamples> collect() {
        List<MetricFamilySamples.Sample> samples = Arrays.asList(
                createSample(MetricDimension.RECEIVE_COUNT_SUCCESS, metricItem.getReceiveCountSuccess().doubleValue()),
                createSample(MetricDimension.RECEIVE_FAILED, metricItem.getReceiveFailed().doubleValue()),
                createSample(MetricDimension.SEND_COUNT_SUCCESS, metricItem.getSendCountSuccess().doubleValue()),
                createSample(MetricDimension.SEND_COUNT_FAILED, metricItem.getSendCountFailed().doubleValue()),
                createSample(MetricDimension.SEND_DURATION, metricItem.getSendDuration().doubleValue()),
                createSample(MetricDimension.INVALID_DATA, metricItem.getInvalidData().doubleValue()),
                createSample(MetricDimension.FILTER_SUCCESS, metricItem.getFilterSuccess().doubleValue()));

        MetricFamilySamples metricFamilySamples =
                new MetricFamilySamples(AUDIT_STORE_SERVER_NAME, Type.GAUGE, HELP_DESCRIPTION, samples);

        return Collections.singletonList(metricFamilySamples);
    }

    private MetricFamilySamples.Sample createSample(MetricDimension key, double value) {
        return new MetricFamilySamples.Sample(AUDIT_STORE_SERVER_NAME,
                Collections.singletonList(MetricItem.K_DIMENSION_KEY),
                Collections.singletonList(key.getKey()), value);
    }

    @Override
    public void report() {
        if (metricItem != null) {
            LOGGER.info("Report store Prometheus metric: {}", metricItem);
        } else {
            LOGGER.warn("MetricItem is null, nothing to report.");
        }
    }

    @Override
    public void stop() {
        if (server != null) {
            server.close();
        }
    }
}