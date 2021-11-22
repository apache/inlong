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

package org.apache.inlong.agent.core.job;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.inlong.commons.config.metrics.CountMetric;
import org.apache.inlong.commons.config.metrics.Dimension;
import org.apache.inlong.commons.config.metrics.MetricDomain;
import org.apache.inlong.commons.config.metrics.MetricItem;
import org.apache.inlong.commons.config.metrics.MetricRegister;

@MetricDomain(name = "AgentJob")
public class JobMetrics extends MetricItem {

    private static final JobMetrics JOB_METRICS = new JobMetrics();

    private static final AtomicBoolean REGISTER_ONCE = new AtomicBoolean(false);
    private static final String AGENT_JOB_METRIC = "AgentJobMetric";

    @Dimension
    public String tagName;

    @CountMetric
    public AtomicLong runningJobs = new AtomicLong(0);

    @CountMetric
    public AtomicLong fatalJobs = new AtomicLong(0);

    public static JobMetrics create() {
        if (REGISTER_ONCE.compareAndSet(false, true)) {
            JOB_METRICS.tagName = AGENT_JOB_METRIC;
            MetricRegister.register(JOB_METRICS);
        }
        return JOB_METRICS;
    }
}

