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

package org.apache.inlong.audit.metric.prometheus;

import org.apache.inlong.audit.file.ConfigManager;
import org.apache.inlong.audit.metric.AbstractMetric;
import org.apache.inlong.audit.metric.MetricDimension;
import org.apache.inlong.audit.metric.MetricItem;

import io.prometheus.client.Collector;
import io.prometheus.client.exporter.HTTPServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.apache.inlong.audit.config.ConfigConstants.DEFAULT_PROMETHEUS_PORT;
import static org.apache.inlong.audit.config.ConfigConstants.KEY_PROMETHEUS_PORT;

/**
 * PrometheusMetric
 */
public class ProxyPrometheusMetric extends Collector implements AbstractMetric {

    private static final Logger LOGGER = LoggerFactory.getLogger(ProxyPrometheusMetric.class);
    private static final String HELP_DESCRIPTION = "Audit Proxy metrics help description";
    private static final String AUDIT_PROXY_SERVER_NAME = "audit-proxy";

    private final MetricItem metricItem;
    private HTTPServer server;

    public ProxyPrometheusMetric(MetricItem metricItem) {
        this.metricItem = metricItem;
        try {
            server = new HTTPServer(ConfigManager.getInstance().getValue(KEY_PROMETHEUS_PORT, DEFAULT_PROMETHEUS_PORT));
            this.register();
        } catch (IOException e) {
            LOGGER.error("Construct proxy prometheus metric has IOException", e);
        }
    }

    @Override
    public List<MetricFamilySamples> collect() {
        List<MetricFamilySamples.Sample> samples = Arrays.asList(
                createSample(MetricDimension.RECEIVE_COUNT_SUCCESS, metricItem.getReceiveCountSuccess().doubleValue()),
                createSample(MetricDimension.RECEIVE_PACK_SUCCESS, metricItem.getReceivePackSuccess().doubleValue()),
                createSample(MetricDimension.RECEIVE_SIZE_SUCCESS, metricItem.getReceiveSizeSuccess().doubleValue()),
                createSample(MetricDimension.RECEIVE_COUNT_INVALID, metricItem.getReceiveCountInvalid().doubleValue()),
                createSample(MetricDimension.RECEIVE_COUNT_EXPIRED, metricItem.getReceiveCountExpired().doubleValue()),
                createSample(MetricDimension.SEND_COUNT_SUCCESS, metricItem.getSendCountSuccess().doubleValue()),
                createSample(MetricDimension.SEND_COUNT_FAILED, metricItem.getSendCountFailed().doubleValue()));

        MetricFamilySamples metricFamilySamples =
                new MetricFamilySamples(AUDIT_PROXY_SERVER_NAME, Type.GAUGE, HELP_DESCRIPTION, samples);

        return Collections.singletonList(metricFamilySamples);
    }

    private MetricFamilySamples.Sample createSample(MetricDimension key, double value) {
        return new MetricFamilySamples.Sample(AUDIT_PROXY_SERVER_NAME,
                Collections.singletonList(MetricItem.K_DIMENSION_KEY),
                Collections.singletonList(key.getKey()), value);
    }

    @Override
    public void report() {
        if (metricItem != null) {
            LOGGER.info("Report proxy Prometheus metric: {}", metricItem);
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