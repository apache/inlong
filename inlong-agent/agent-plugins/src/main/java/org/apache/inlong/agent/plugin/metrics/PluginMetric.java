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

package org.apache.inlong.agent.plugin.metrics;

import org.apache.inlong.agent.metrics.Metric;
import org.apache.inlong.agent.metrics.Metrics;
import org.apache.inlong.agent.metrics.MetricsRegister;
import org.apache.inlong.agent.metrics.Tag;
import org.apache.inlong.agent.metrics.counter.CounterLong;

/**
 * Common plugin metrics
 */
@Metrics
public class PluginMetric {

    @Metric
    public Tag tagName;

    @Metric
    public CounterLong readNum;

    @Metric
    public CounterLong sendNum;

    @Metric
    public CounterLong sendFailedNum;

    @Metric
    public CounterLong readFailedNum;

    @Metric
    public CounterLong readSuccessNum;

    @Metric
    public CounterLong sendSuccessNum;

    public PluginMetric() {
        // every metric should register, otherwise not working.
        MetricsRegister.register("Plugin", "PluginSummary", null, this);
    }
}
