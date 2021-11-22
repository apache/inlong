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

import java.util.concurrent.atomic.AtomicLong;
import org.apache.inlong.agent.metrics.Metric;
import org.apache.inlong.commons.config.metrics.Dimension;
import org.apache.inlong.commons.config.metrics.MetricDomain;
import org.apache.inlong.commons.config.metrics.MetricItem;
import org.apache.inlong.commons.config.metrics.MetricRegister;

/**
 * metrics for agent plugin
 */
@MetricDomain(name = "PluginMetric")
public class PluginMetric extends MetricItem {

    @Dimension
    public String tagName;

    @Metric
    public AtomicLong readNum = new AtomicLong(0);

    @Metric
    public AtomicLong sendNum = new AtomicLong(0);

    @Metric
    public AtomicLong sendFailedNum = new AtomicLong(0);

    @Metric
    public AtomicLong readFailedNum = new AtomicLong(0);

    @Metric
    public AtomicLong readSuccessNum = new AtomicLong(0);

    @Metric
    public AtomicLong sendSuccessNum = new AtomicLong(0);

    public PluginMetric(String tagName) {
        this.tagName = tagName;
        MetricRegister.register(this);
    }

}
