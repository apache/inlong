/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.inlong.agent.plugin.metrics;

import io.prometheus.client.Counter;

public class PluginPrometheusMetric implements PluginMetric {

    // agent-metrics
    public static final String AGENT_PLUGIN_METRICS_PREFIX = "inlong_agent_plugin_";
    public static final String READ_NUM_COUNTER_NAME = "read_num_count";
    public static final String SEND_NUM_COUNTER_NAME = "send_num_count";
    public static final String READ_FAILED_NUM_COUNTER_NAME = "read_failed_num_count";
    public static final String SEND_FAILED_NUM_COUNTER_NAME = "send_failed_num_count";
    public static final String READ_SUCCESS_NUM_COUNTER_NAME = "read_success_num_count";
    public static final String SEND_SUCCESS_NUM_COUNTER_NAME = "send_success_num_count";

    // agent-counters
    private final Counter readNumCounter = Counter.build()
            .name(AGENT_PLUGIN_METRICS_PREFIX + READ_NUM_COUNTER_NAME)
            .help("The total number of reads.")
            .labelNames("tag")
            .register();
    private final Counter sendNumCounter = Counter.build()
            .name(AGENT_PLUGIN_METRICS_PREFIX + SEND_NUM_COUNTER_NAME)
            .help("The total number of sends.")
            .labelNames("tag")
            .register();
    private final Counter readFailedNumCounter = Counter.build()
            .name(AGENT_PLUGIN_METRICS_PREFIX + READ_FAILED_NUM_COUNTER_NAME)
            .help("The total number of failed reads.")
            .labelNames("tag")
            .register();
    private final Counter sendFailedNumCounter = Counter.build()
            .name(AGENT_PLUGIN_METRICS_PREFIX + SEND_FAILED_NUM_COUNTER_NAME)
            .help("The total number of failed sends.")
            .labelNames("tag")
            .register();
    private final Counter readSuccessNumCounter = Counter.build()
            .name(AGENT_PLUGIN_METRICS_PREFIX + READ_SUCCESS_NUM_COUNTER_NAME)
            .help("The total number of successful reads.")
            .labelNames("tag")
            .register();
    private final Counter sendSuccessNumCounter = Counter.build()
            .name(AGENT_PLUGIN_METRICS_PREFIX + SEND_SUCCESS_NUM_COUNTER_NAME)
            .help("The total number of successful sends.")
            .labelNames("tag")
            .register();

    private String tagName;

    public PluginPrometheusMetric(String tagName) {
        this.tagName = tagName;
    }

    @Override
    public String getTagName() {
        return tagName;
    }

    @Override
    public void incReadNum() {
        readNumCounter.labels(tagName).inc();
    }

    @Override
    public long getReadNum() {
        return (long) readNumCounter.labels(tagName).get();
    }

    @Override
    public void incSendNum() {
        sendNumCounter.labels(tagName).inc();
    }

    @Override
    public long getSendNum() {
        return (long) sendNumCounter.labels(tagName).get();
    }

    @Override
    public void incReadFailedNum() {
        readFailedNumCounter.labels(tagName).inc();
    }

    @Override
    public long getReadFailedNum() {
        return (long) readFailedNumCounter.labels(tagName).get();
    }

    @Override
    public void incSendFailedNum() {
        sendFailedNumCounter.labels(tagName).inc();
    }

    @Override
    public long getSendFailedNum() {
        return (long) sendFailedNumCounter.labels(tagName).get();
    }

    @Override
    public void incReadSuccessNum() {
        readSuccessNumCounter.labels(tagName).inc();
    }

    @Override
    public long getReadSuccessNum() {
        return (long) readSuccessNumCounter.labels(tagName).get();
    }

    @Override
    public void incSendSuccessNum() {
        sendSuccessNumCounter.labels(tagName).inc();
    }

    @Override
    public void incSendSuccessNum(int delta) {
        sendSuccessNumCounter.labels(tagName).inc(delta);
    }

    @Override
    public long getSendSuccessNum() {
        return (long) sendSuccessNumCounter.labels(tagName).get();
    }
}
