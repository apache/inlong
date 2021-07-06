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

package org.apache.inlong.agent.core.task;

import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.inlong.agent.metrics.Metric;
import org.apache.inlong.agent.metrics.Metrics;
import org.apache.inlong.agent.metrics.MetricsRegister;
import org.apache.inlong.agent.metrics.counter.CounterLong;
import org.apache.inlong.agent.metrics.gauge.GaugeInt;

/**
 * Metric collector for task level.
 */
@Metrics
public class TaskMetrics {

    private static final TaskMetrics TASK_METRICS = new TaskMetrics();
    private static final AtomicBoolean REGISTER_ONCE = new AtomicBoolean(false);

    @Metric
    GaugeInt runningTasks;

    @Metric
    GaugeInt retryingTasks;

    @Metric
    CounterLong fatalTasks;

    private TaskMetrics() {
    }

    public static TaskMetrics create() {
        // register one time.
        if (REGISTER_ONCE.compareAndSet(false, true)) {
            MetricsRegister.register("Task", "StateSummary", null, TASK_METRICS);
        }
        return TASK_METRICS;
    }
}
