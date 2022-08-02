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

package org.apache.inlong.agent.metrics;

import io.prometheus.client.exporter.HTTPServer;
import io.prometheus.client.hotspot.DefaultExports;
import org.apache.inlong.agent.conf.AgentConfiguration;
import org.apache.inlong.agent.metrics.global.PrometheusGlobalMetrics;
import org.apache.inlong.agent.metrics.job.JobPrometheusMetrics;
import org.apache.inlong.agent.metrics.task.TaskPrometheusMetrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import static org.apache.inlong.agent.constant.AgentConstants.DEFAULT_PROMETHEUS_EXPORTER_PORT;
import static org.apache.inlong.agent.constant.AgentConstants.PROMETHEUS_EXPORTER_PORT;

/**
 * prometheus metric handler
 */
public class AgentPrometheusMetricListener extends AgentMetricBaseListener {

    private static final Logger LOGGER = LoggerFactory.getLogger(AgentPrometheusMetricListener.class);
    private static HTTPServer metricsServer;

    static {
        DefaultExports.initialize();
    }

    public AgentPrometheusMetricListener() {
        jobMetrics = new JobPrometheusMetrics();
        taskMetrics = new TaskPrometheusMetrics();
        globalMetrics = new PrometheusGlobalMetrics();
    }

    @Override
    public void init() {
        // starting metrics server
        int metricsServerPort = AgentConfiguration.getAgentConf()
                .getInt(PROMETHEUS_EXPORTER_PORT, DEFAULT_PROMETHEUS_EXPORTER_PORT);
        LOGGER.info("Starting prometheus metrics server on port {}", metricsServerPort);
        try {
            metricsServer = new HTTPServer(metricsServerPort);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void close() {
        if (metricsServer != null) {
            metricsServer.close();
        }
    }
}
