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

package org.apache.inlong.sort.formats.metrics.gauge;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.metrics.Gauge;

/**
 * The metrics type of gauge for connector.
 * And the metric group for each source and sink would be the same as ordinary operator scope,
 * i.e. default to [host].taskmanager.[tm_id].[job_name].[operator_name].[subtask_index],
 * the user can specify a custom group name, it splicing at the end of the default operator scope.
 *
 */
public abstract class AbstractMetricGauge<T> {

    private Gauge gauge;
    private T gaugeValue;

    public AbstractMetricGauge(RuntimeContext context, String gaugeName) {
        this.gauge = context.getMetricGroup().gauge(gaugeName, () -> this.gaugeValue);
    }

    public AbstractMetricGauge(RuntimeContext context, String metricGroupName, String gaugeName) {
        this.gauge = context.getMetricGroup().addGroup(metricGroupName).gauge(gaugeName, () -> this.gaugeValue);
    }

    public AbstractMetricGauge(
            RuntimeContext context,
            String metricGroupKey,
            String metricGroupValue,
            String gaugeName) {
        this.gauge = context.getMetricGroup()
                .addGroup(metricGroupKey, metricGroupValue).gauge(gaugeName, () -> this.gaugeValue);
    }

    public void gauge(T gaugeValue) {
        this.gaugeValue = gaugeValue;
    }
}
