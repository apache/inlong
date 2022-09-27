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
import static org.apache.inlong.common.metric.MetricItem.LOGGER;

import io.prometheus.client.Collector.MetricFamilySamples;
import io.prometheus.client.Collector.Type;
import io.prometheus.client.CounterMetricFamily;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import org.junit.Assert;
import org.junit.Test;

public class TestAgentCustomPrometheusListener {

    @Test
    public void testCollector() throws IOException {
        AgentCustomPrometheusListener myCustomCollector = new AgentCustomPrometheusListener();
        myCustomCollector.register();
        Map<String, String> dimensionMap = new HashMap<>();
        // add dimension
        String metricName = "total";
        MetricFamilySamples.Sample sample = new MetricFamilySamples.Sample(metricName,
                Arrays.asList(KEY_PLUGIN_ID, KEY_INLONG_GROUP_ID, KEY_INLONG_STREAM_ID, KEY_COMPONENT_NAME),
                Arrays.asList(dimensionMap.getOrDefault(KEY_PLUGIN_ID, "-"),
                        dimensionMap.getOrDefault(KEY_INLONG_GROUP_ID, "-"),
                        dimensionMap.getOrDefault(KEY_INLONG_STREAM_ID, "-"),
                        dimensionMap.getOrDefault(KEY_COMPONENT_NAME, "-")), Math.random());

        MetricFamilySamples samples = new MetricFamilySamples(metricName, Type.GAUGE, "The guage of dimension map",
                Arrays.asList(sample));
        myCustomCollector.getMfs().add(samples);

        AgentMetricItem metricItem = new AgentMetricItem();
        metricItem.jobFatalCount.incrementAndGet();
        //add metrics
        CounterMetricFamily totalCounter = new CounterMetricFamily("group=total", "The metric of  dimension",
                Arrays.asList("dimension"));
        totalCounter.addMetric(Arrays.asList(M_JOB_RUNNING_COUNT), metricItem.jobRunningCount.get());
        totalCounter.addMetric(Arrays.asList(M_JOB_FATAL_COUNT), metricItem.jobFatalCount.get());
        totalCounter.addMetric(Arrays.asList(M_TASK_RUNNING_COUNT), metricItem.taskRunningCount.get());
        totalCounter.addMetric(Arrays.asList(M_TASK_RETRYING_COUNT), metricItem.taskRetryingCount.get());
        totalCounter.addMetric(Arrays.asList(M_TASK_FATAL_COUNT), metricItem.taskFatalCount.get());
        totalCounter.addMetric(Arrays.asList(M_SINK_SUCCESS_COUNT), metricItem.sinkSuccessCount.get());
        totalCounter.addMetric(Arrays.asList(M_SINK_FAIL_COUNT), metricItem.sinkFailCount.get());
        totalCounter.addMetric(Arrays.asList(M_SOURCE_SUCCESS_COUNT), metricItem.sourceSuccessCount.get());
        totalCounter.addMetric(Arrays.asList(M_SOURCE_FAIL_COUNT), metricItem.sourceFailCount.get());
        totalCounter.addMetric(Arrays.asList(M_PLUGIN_READ_COUNT), metricItem.pluginReadCount.get());
        totalCounter.addMetric(Arrays.asList(M_PLUGIN_SEND_COUNT), metricItem.pluginSendCount.get());
        totalCounter.addMetric(Arrays.asList(M_PLUGIN_READ_FAIL_COUNT), metricItem.pluginReadFailCount.get());
        totalCounter.addMetric(Arrays.asList(M_PLUGIN_SEND_FAIL_COUNT), metricItem.pluginSendFailCount.get());
        totalCounter.addMetric(Arrays.asList(M_PLUGIN_READ_SUCCESS_COUNT), metricItem.pluginReadSuccessCount.get());
        totalCounter.addMetric(Arrays.asList(M_PLUGIN_SEND_SUCCESS_COUNT), metricItem.pluginSendSuccessCount.get());
        myCustomCollector.getMfs().add(totalCounter);
        String httpReply = visitWeb("http://127.0.0.1:9080/metrics");
        LOGGER.debug(httpReply);
        Assert.assertTrue(httpReply.contains(
                "total{pluginId=\"-\",inlongGroupId=\"-\",inlongStreamId=\"-\",componentName=\"-\",}"));
        Assert.assertTrue(httpReply.contains("ngroup=total_total{dimension=\"jobFatalCount\",} 1.0"));
    }

    public String visitWeb(String httpUrl) throws IOException {
        URL url = new URL(httpUrl);
        HttpURLConnection urlcon = (HttpURLConnection) url.openConnection();
        urlcon.connect();         //获取连接
        InputStream is = urlcon.getInputStream();
        BufferedReader buffer = new BufferedReader(new InputStreamReader(is));
        StringBuffer bs = new StringBuffer();
        String l = null;
        while ((l = buffer.readLine()) != null) {
            bs.append(l).append("/n");
        }
        return bs.toString();
    }
}
