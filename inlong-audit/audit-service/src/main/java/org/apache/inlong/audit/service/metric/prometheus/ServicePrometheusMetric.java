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

package org.apache.inlong.audit.service.metric.prometheus;

import org.apache.inlong.audit.metric.AbstractMetric;
import org.apache.inlong.audit.service.config.Configuration;
import org.apache.inlong.audit.service.metric.MetricDimension;
import org.apache.inlong.audit.service.metric.MetricItem;
import org.apache.inlong.audit.service.metric.MetricStat;

import io.prometheus.client.Collector;
import io.prometheus.client.exporter.HTTPServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.apache.inlong.audit.service.config.ConfigConstants.DEFAULT_AUDIT_SERVICE_PROMETHEUS_PORT;
import static org.apache.inlong.audit.service.config.ConfigConstants.KEY_AUDIT_SERVICE_PROMETHEUS_PORT;
import static org.apache.inlong.audit.service.metric.MetricDimension.COUNT;
import static org.apache.inlong.audit.service.metric.MetricDimension.DURATION;

/**
 * PrometheusMetric
 */
public class ServicePrometheusMetric extends Collector implements AbstractMetric {

    private static final Logger LOGGER = LoggerFactory.getLogger(ServicePrometheusMetric.class);
    private static final String HELP_DESCRIPTION = "Audit service metrics help description";
    public static final String AUDIT_SERVICE_SERVER_NAME = "audit-service";
    private static final String METRIC_API_TYPE = "apiType";
    private static final String METRIC_DIMENSION = "dimension";

    private final MetricItem metricItem;
    private HTTPServer server;

    public ServicePrometheusMetric(MetricItem metricItem) {
        this.metricItem = metricItem;
        try {
            server = new HTTPServer(Configuration.getInstance().get(KEY_AUDIT_SERVICE_PROMETHEUS_PORT,
                    DEFAULT_AUDIT_SERVICE_PROMETHEUS_PORT));
            this.register();
        } catch (IOException e) {
            LOGGER.error("Construct store prometheus metric has IOException", e);
        }
    }

    @Override
    public List<MetricFamilySamples> collect() {
        List<MetricFamilySamples.Sample> samples = new ArrayList<>();
        for (Map.Entry<String, MetricStat> entry : metricItem.getMetricStatMap().entrySet()) {
            String apiType = entry.getKey();
            MetricStat stat = entry.getValue();
            samples.add(createSample(COUNT, apiType, stat.getCount().get()));
            samples.add(createSample(DURATION, apiType, stat.getDuration().get()));
        }

        MetricFamilySamples metricFamilySamples = new MetricFamilySamples(
                AUDIT_SERVICE_SERVER_NAME, Type.GAUGE, HELP_DESCRIPTION, samples);

        return Collections.singletonList(metricFamilySamples);
    }

    private MetricFamilySamples.Sample createSample(MetricDimension metricDimension, String apiType, long statValue) {
        return new MetricFamilySamples.Sample(
                AUDIT_SERVICE_SERVER_NAME,
                Arrays.asList(METRIC_API_TYPE, METRIC_DIMENSION),
                Arrays.asList(apiType, metricDimension.getKey()),
                statValue);
    }

    @Override
    public void report() {
        if (metricItem != null) {
            LOGGER.info("Report Service Prometheus metric: {}", metricItem);
        } else {
            LOGGER.warn("MetricItem is null, nothing to report.");
        }
    }

    @Override
    public void stop() {
        if (server != null) {
            server.close();
            LOGGER.info("Prometheus HTTP server stopped successfully.");
        } else {
            LOGGER.warn("Prometheus HTTP server is not running.");
        }
    }

}