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

import org.apache.inlong.agent.metrics.global.GlobalMetrics;
import org.apache.inlong.agent.metrics.job.JobMetrics;
import org.apache.inlong.agent.metrics.task.TaskMetrics;
import org.apache.inlong.common.metric.MetricItemValue;
import org.apache.inlong.common.metric.MetricListener;

import java.util.List;

/**
 * Agent metric base handler
 */
public abstract class AgentMetricBaseListener implements MetricListener {

    public JobMetrics jobMetrics;

    public TaskMetrics taskMetrics;

    public GlobalMetrics globalMetrics;

    @Override
    public void snapshot(String domain, List<MetricItemValue> itemValues) {
        // nothing
    }

    public abstract void init();

    public abstract void close();
}
