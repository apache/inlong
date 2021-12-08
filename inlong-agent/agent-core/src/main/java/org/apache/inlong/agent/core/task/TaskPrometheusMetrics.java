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

package org.apache.inlong.agent.core.task;

import io.prometheus.client.Counter;
import io.prometheus.client.Gauge;

public class TaskPrometheusMetrics implements TaskMetrics {

    public static final String AGENT_TASK_METRICS_PREFIX = "inlong_agent_task_";

    public static final String RUNNING_TASK_GAUGE_NAME = "running_task_gauge";
    public static final String RETRYING_TASK_GAUGE_NAME = "retrying_task_gauge";
    public static final String FATAL_TASK_COUNTER_NAME = "fatal_task_counter";

    private final Gauge runningTaskGauge = Gauge.build()
            .name(AGENT_TASK_METRICS_PREFIX + RUNNING_TASK_GAUGE_NAME)
            .help("The count of tasks currently running.")
            .register();

    private final Gauge retryingTaskGauge = Gauge.build()
            .name(AGENT_TASK_METRICS_PREFIX + RETRYING_TASK_GAUGE_NAME)
            .help("The count of tasks currently retrying.")
            .register();

    private final Counter fatalTaskCounter = Counter.build()
            .name(AGENT_TASK_METRICS_PREFIX + FATAL_TASK_COUNTER_NAME)
            .help("The total number of current fatal tasks.")
            .register();

    @Override
    public void incRunningTaskCount() {
        runningTaskGauge.inc();
    }

    @Override
    public void decRunningTaskCount() {
        runningTaskGauge.dec();
    }

    @Override
    public void incRetryingTaskCount() {
        retryingTaskGauge.inc();
    }

    @Override
    public void decRetryingTaskCount() {
        retryingTaskGauge.dec();
    }

    @Override
    public void incFatalTaskCount() {
        fatalTaskCounter.inc();
    }
}
