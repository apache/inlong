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

    public static final String AGENT_PLUGIN_METRICS_PREFIX = "inlong_agent_plugin_";

    public static final String READ_NUM_COUNTER_NAME = "read_num_counter";
    public static final String SEND_NUM_COUNTER_NAME = "send_num_counter";
    public static final String READ_FAILED_NUM_COUNTER_NAME = "read_failed_num_counter";
    public static final String SEND_FAILED_NUM_COUNTER_NAME = "send_failed_num_counter";
    public static final String READ_SUCCESS_NUM_COUNTER_NAME = "read_success_num_counter";
    public static final String SEND_SUCCESS_NUM_COUNTER_NAME = "send_success_num_counter";

    private final Counter readNumCounter = Counter.build()
            .name(AGENT_PLUGIN_METRICS_PREFIX + READ_NUM_COUNTER_NAME)
            .help("The total number of current fatal jobs.")
            .register();

    private final Counter sendNumCounter = Counter.build()
            .name(AGENT_PLUGIN_METRICS_PREFIX + SEND_NUM_COUNTER_NAME)
            .help("The total number of current fatal jobs.")
            .register();

    private final Counter readFailedNumCounter = Counter.build()
            .name(AGENT_PLUGIN_METRICS_PREFIX + READ_FAILED_NUM_COUNTER_NAME)
            .help("The total number of current fatal jobs.")
            .register();

    private final Counter sendFailedNumCounter = Counter.build()
            .name(AGENT_PLUGIN_METRICS_PREFIX + SEND_FAILED_NUM_COUNTER_NAME)
            .help("The total number of current fatal jobs.")
            .register();

    private final Counter readSuccessNumCounter = Counter.build()
            .name(AGENT_PLUGIN_METRICS_PREFIX + READ_SUCCESS_NUM_COUNTER_NAME)
            .help("The total number of current fatal jobs.")
            .register();

    private final Counter sendSuccessNumCounter = Counter.build()
            .name(AGENT_PLUGIN_METRICS_PREFIX + SEND_SUCCESS_NUM_COUNTER_NAME)
            .help("The total number of current fatal jobs.")
            .register();

    @Override
    public void incReadNum() {
        readNumCounter.inc();
    }

    @Override
    public long getReadNum() {
        return (long) readNumCounter.get();
    }

    @Override
    public void incSendNum() {
        sendNumCounter.inc();
    }

    @Override
    public long getSendNum() {
        return (long) sendNumCounter.get();
    }

    @Override
    public void incReadFailedNum() {
        readFailedNumCounter.inc();
    }

    @Override
    public long getReadFailedNum() {
        return (long) readFailedNumCounter.get();
    }

    @Override
    public void incSendFailedNum() {
        sendFailedNumCounter.inc();
    }

    @Override
    public long getSendFailedNum() {
        return (long) sendFailedNumCounter.get();
    }

    @Override
    public void incReadSuccessNum() {
        readSuccessNumCounter.inc();
    }

    @Override
    public long getReadSuccessNum() {
        return (long) readSuccessNumCounter.get();
    }

    @Override
    public void incSendSuccessNum() {
        sendSuccessNumCounter.inc();
    }

    @Override
    public void incSendSuccessNum(int delta) {
        sendSuccessNumCounter.inc(delta);
    }

    @Override
    public long getSendSuccessNum() {
        return (long) sendSuccessNumCounter.get();
    }
}
