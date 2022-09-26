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

import static org.apache.inlong.agent.constant.AgentConstants.DEFAULT_PROMETHEUS_EXPORTER_PORT;
import static org.apache.inlong.agent.constant.AgentConstants.PROMETHEUS_EXPORTER_PORT;
import static org.apache.inlong.agent.metrics.AgentMetricItem.KEY_COMPONENT_NAME;
import static org.apache.inlong.agent.metrics.AgentMetricItem.KEY_INLONG_GROUP_ID;
import static org.apache.inlong.agent.metrics.AgentMetricItem.KEY_INLONG_STREAM_ID;
import static org.apache.inlong.agent.metrics.AgentMetricItem.KEY_PLUGIN_ID;
import static org.apache.inlong.agent.metrics.AgentMetricItem.M_JOB_FATAL_COUNT;
import static org.apache.inlong.agent.metrics.AgentMetricItem.M_JOB_RUNNING_COUNT;
import static org.apache.inlong.agent.metrics.AgentMetricItem.M_PLUGIN_READ_COUNT;
import static org.apache.inlong.agent.metrics.AgentMetricItem.M_PLUGIN_READ_FAIL_COUNT;
import static org.apache.inlong.agent.metrics.AgentMetricItem.M_PLUGIN_READ_SUCCESS_COUNT;
import static org.apache.inlong.agent.metrics.AgentMetricItem.M_PLUGIN_SEND_COUNT;
import static org.apache.inlong.agent.metrics.AgentMetricItem.M_PLUGIN_SEND_FAIL_COUNT;
import static org.apache.inlong.agent.metrics.AgentMetricItem.M_PLUGIN_SEND_SUCCESS_COUNT;
import static org.apache.inlong.agent.metrics.AgentMetricItem.M_SINK_FAIL_COUNT;
import static org.apache.inlong.agent.metrics.AgentMetricItem.M_SINK_SUCCESS_COUNT;
import static org.apache.inlong.agent.metrics.AgentMetricItem.M_SOURCE_FAIL_COUNT;
import static org.apache.inlong.agent.metrics.AgentMetricItem.M_SOURCE_SUCCESS_COUNT;
import static org.apache.inlong.agent.metrics.AgentMetricItem.M_TASK_FATAL_COUNT;
import static org.apache.inlong.agent.metrics.AgentMetricItem.M_TASK_RETRYING_COUNT;
import static org.apache.inlong.agent.metrics.AgentMetricItem.M_TASK_RUNNING_COUNT;

import io.prometheus.client.Collector;
import io.prometheus.client.Collector.MetricFamilySamples.Sample;
import io.prometheus.client.CounterMetricFamily;
import io.prometheus.client.exporter.HTTPServer;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.apache.inlong.agent.conf.AgentConfiguration;
import org.apache.inlong.common.metric.MetricItemValue;
import org.apache.inlong.common.metric.MetricListener;
import org.apache.inlong.common.metric.MetricValue;

public class AgentCustomPrometheusListener extends Collector implements MetricListener {

    List<MetricFamilySamples> mfs = new ArrayList<>();
    private static final MetricValue ZERO = MetricValue.of(null, 0);
    protected HTTPServer httpServer;


    public AgentCustomPrometheusListener() {
        try {
            int metricsServerPort = AgentConfiguration.getAgentConf()
                    .getInt(PROMETHEUS_EXPORTER_PORT, DEFAULT_PROMETHEUS_EXPORTER_PORT);
            httpServer = new HTTPServer(metricsServerPort);
            this.register();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public List<MetricFamilySamples> getMfs() {
        return mfs;
    }

    @Override
    public List<MetricFamilySamples> collect() {
        return mfs;
    }

    @Override
    public void snapshot(String domain, List<MetricItemValue> itemValues) {
        for (MetricItemValue itemValue : itemValues) {
            Map<String, String> dimensionMap = itemValue.getDimensions();
            // add dimension
            String metricName = "total";
            Sample sample = new Sample(metricName,
                    Arrays.asList(KEY_PLUGIN_ID, KEY_INLONG_GROUP_ID, KEY_INLONG_STREAM_ID, KEY_COMPONENT_NAME),
                    Arrays.asList(dimensionMap.getOrDefault(KEY_PLUGIN_ID, "-"),
                            dimensionMap.getOrDefault(KEY_INLONG_GROUP_ID, "-"),
                            dimensionMap.getOrDefault(KEY_INLONG_STREAM_ID, "-"),
                            dimensionMap.getOrDefault(KEY_COMPONENT_NAME, "-")), Math.random());

            MetricFamilySamples samples = new MetricFamilySamples(metricName, Type.GAUGE, "The guage of dimension map",
                    Arrays.asList(sample));
            mfs.add(samples);

            //add metrics
            CounterMetricFamily metricsCounter = new CounterMetricFamily("group=total", "The metric of  dimension",
                    Arrays.asList("dimension"));
            Map<String, MetricValue> metricMap = itemValue.getMetrics();
            metricsCounter.addMetric(Arrays.asList(M_JOB_RUNNING_COUNT),
                    (double) metricMap.getOrDefault(M_JOB_RUNNING_COUNT, ZERO).value);
            metricsCounter.addMetric(Arrays.asList(M_JOB_FATAL_COUNT), (double)
                    metricMap.getOrDefault(M_JOB_FATAL_COUNT, ZERO).value);
            metricsCounter.addMetric(Arrays.asList(M_TASK_RUNNING_COUNT),
                    (double) metricMap.getOrDefault(M_TASK_RUNNING_COUNT, ZERO).value);
            metricsCounter.addMetric(Arrays.asList(M_TASK_RETRYING_COUNT),
                    (double) metricMap.getOrDefault(M_TASK_RETRYING_COUNT, ZERO).value);
            metricsCounter.addMetric(Arrays.asList(M_TASK_FATAL_COUNT),
                    (double) metricMap.getOrDefault(M_TASK_FATAL_COUNT, ZERO).value);
            metricsCounter.addMetric(Arrays.asList(M_SINK_SUCCESS_COUNT),
                    (double) metricMap.getOrDefault(M_SINK_SUCCESS_COUNT, ZERO).value);
            metricsCounter.addMetric(Arrays.asList(M_SINK_FAIL_COUNT),
                    (double) metricMap.getOrDefault(M_SINK_FAIL_COUNT, ZERO).value);
            metricsCounter.addMetric(Arrays.asList(M_SOURCE_SUCCESS_COUNT),
                    (double) metricMap.getOrDefault(M_SOURCE_SUCCESS_COUNT, ZERO).value);
            metricsCounter.addMetric(Arrays.asList(M_SOURCE_FAIL_COUNT),
                    (double) metricMap.getOrDefault(M_SOURCE_FAIL_COUNT, ZERO).value);
            metricsCounter.addMetric(Arrays.asList(M_PLUGIN_READ_COUNT),
                    (double) metricMap.getOrDefault(M_PLUGIN_READ_COUNT, ZERO).value);
            metricsCounter.addMetric(Arrays.asList(M_PLUGIN_SEND_COUNT),
                    (double) metricMap.getOrDefault(M_PLUGIN_SEND_COUNT, ZERO).value);
            metricsCounter.addMetric(Arrays.asList(M_PLUGIN_READ_FAIL_COUNT),
                    (double) metricMap.getOrDefault(M_PLUGIN_READ_FAIL_COUNT, ZERO).value);
            metricsCounter.addMetric(Arrays.asList(M_PLUGIN_SEND_FAIL_COUNT),
                    (double) metricMap.getOrDefault(M_PLUGIN_SEND_FAIL_COUNT, ZERO).value);
            metricsCounter.addMetric(Arrays.asList(M_PLUGIN_READ_SUCCESS_COUNT),
                    (double) metricMap.getOrDefault(M_PLUGIN_READ_SUCCESS_COUNT, ZERO).value);
            metricsCounter.addMetric(Arrays.asList(M_PLUGIN_SEND_SUCCESS_COUNT),
                    (double) metricMap.getOrDefault(M_PLUGIN_SEND_SUCCESS_COUNT, ZERO).value);
            mfs.add(metricsCounter);

        }
    }
}
