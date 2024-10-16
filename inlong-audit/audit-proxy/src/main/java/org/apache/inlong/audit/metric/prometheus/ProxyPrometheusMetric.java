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

/**
 * PrometheusMetric
 */
public class ProxyPrometheusMetric extends Collector implements AbstractMetric {

    private static final Logger LOGGER = LoggerFactory.getLogger(ProxyPrometheusMetric.class);
    private static final String HELP_DESCRIPTION = "help";

    private final MetricItem metricItem;
    private final String metricName;
    private HTTPServer server;

    public ProxyPrometheusMetric(String metricName, MetricItem metricItem, int prometheusPort) {
        this.metricName = metricName;
        this.metricItem = metricItem;
        try {
            server = new HTTPServer(prometheusPort);
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
                new MetricFamilySamples(metricName, Type.GAUGE, HELP_DESCRIPTION, samples);

        return Collections.singletonList(metricFamilySamples);
    }

    private MetricFamilySamples.Sample createSample(MetricDimension key, double value) {
        return new MetricFamilySamples.Sample(metricName, Collections.singletonList(MetricItem.K_DIMENSION_KEY),
                Collections.singletonList(key.getKey()), value);
    }

    @Override
    public void report() {
        LOGGER.info("Report proxy prometheus metric: {} ", metricItem.toString());
    }

    @Override
    public void stop() {
        server.close();
    }
}