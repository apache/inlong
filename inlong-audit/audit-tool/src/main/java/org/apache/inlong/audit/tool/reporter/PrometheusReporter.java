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

package org.apache.inlong.audit.tool.reporter;

import org.apache.inlong.audit.tool.metric.AuditMetric;

import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.Gauge;
import io.prometheus.client.exporter.HTTPServer;
import lombok.Getter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;

import static org.apache.inlong.audit.tool.config.ConfigConstants.AUDIT_TOOL_ALERTS_TOTAL;
import static org.apache.inlong.audit.tool.config.ConfigConstants.AUDIT_TOOL_DATA_LOSS_RATE;
import static org.apache.inlong.audit.tool.config.ConfigConstants.DEFAULT_PROMETHEUS_PORT;
import static org.apache.inlong.audit.tool.config.ConfigConstants.DESC_AUDIT_TOOL_ALERTS_TOTAL;
import static org.apache.inlong.audit.tool.config.ConfigConstants.DESC_AUDIT_TOOL_DATA_LOSS_RATE;
import static org.apache.inlong.audit.tool.config.ConfigConstants.KEY_ALERT_TYPE;
import static org.apache.inlong.audit.tool.config.ConfigConstants.KEY_GROUP_ID;
import static org.apache.inlong.audit.tool.config.ConfigConstants.KEY_PROMETHEUS_PORT;
import static org.apache.inlong.audit.tool.config.ConfigConstants.KEY_STREAM_ID;

/**
 * PrometheusReporter implements the MetricReporter interface to expose audit tool metrics
 * to a Prometheus monitoring system.
 */
public class PrometheusReporter implements MetricReporter {

    private static final Logger LOGGER = LoggerFactory.getLogger(PrometheusReporter.class);

    private final CollectorRegistry registry;
    @Getter
    private final AuditMetric auditMetric;

    /**
     * Constructor for PrometheusReporter.
     * Initializes a new CollectorRegistry for DTO isolation.
     */
    public PrometheusReporter() {
        this.registry = new CollectorRegistry();
        auditMetric = new AuditMetric(registry);
    }

    /**
     * Initializes the Prometheus reporter.
     * Starts an HTTP server to expose metrics and registers the gauges.
     *
     * @param config A map containing configuration for the reporter, e.g., port.
     */
    @Override
    public void init(Map<String, Object> config) {
        // It's better to get configuration from the passed map than a global singleton.
        int port = (int) config.getOrDefault(KEY_PROMETHEUS_PORT, DEFAULT_PROMETHEUS_PORT);
        try {
            // Start the Prometheus HTTP server on the configured port with our registry.
            HTTPServer server = new HTTPServer.Builder()
                    .withPort(port)
                    .withRegistry(registry)
                    .build();
            LOGGER.info("Prometheus server started on port {}", port);

            // Define and register the 'alerts total' gauge.
            Gauge alertGauge = Gauge.build()
                    .name(AUDIT_TOOL_ALERTS_TOTAL)
                    .help(DESC_AUDIT_TOOL_ALERTS_TOTAL)
                    .labelNames(KEY_GROUP_ID, KEY_STREAM_ID, KEY_ALERT_TYPE)
                    .register(registry); // Register with our specific registry instance

            // Define and register the 'data loss rate' gauge.
            Gauge dataLossRateGauge = Gauge.build()
                    .name(AUDIT_TOOL_DATA_LOSS_RATE)
                    .help(DESC_AUDIT_TOOL_DATA_LOSS_RATE)
                    .labelNames(KEY_GROUP_ID, KEY_STREAM_ID)
                    .register(registry); // Register with our specific registry instance

        } catch (IOException e) {
            LOGGER.error("Failed to start Prometheus server on port {}", port, e);
            // Throwing a runtime exception is appropriate if the reporter cannot start.
            throw new RuntimeException("Failed to start Prometheus server", e);
        }
    }

}
