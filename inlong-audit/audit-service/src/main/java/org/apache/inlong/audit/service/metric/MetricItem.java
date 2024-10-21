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

package org.apache.inlong.audit.service.metric;

import lombok.Data;

import java.util.Map;
import java.util.StringJoiner;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

@Data
public class MetricItem {

    private static final String METRIC_ITEM_SPLITERATOR = ";";
    private AtomicLong inLongGroupIdNum = new AtomicLong(0);
    private AtomicLong inLongStreamIdNum = new AtomicLong(0);
    private ConcurrentHashMap<String, MetricStat> metricStatMap = new ConcurrentHashMap<>();

    public void resetAllMetrics() {
        for (MetricStat entry : metricStatMap.values()) {
            entry.getCount().set(0);
            entry.getDuration().set(0);
        }
    }

    public MetricStat getMetricStat(String metricName) {
        return metricStatMap.computeIfAbsent(metricName, k -> new MetricStat(new AtomicLong(), new AtomicLong()));
    }

    public ConcurrentHashMap<String, MetricStat> getMetricStatMap() {
        return metricStatMap;
    }

    @Override
    public String toString() {
        StringJoiner joiner = new StringJoiner(METRIC_ITEM_SPLITERATOR);
        for (Map.Entry<String, MetricStat> entry : metricStatMap.entrySet()) {
            String stat = entry.getKey() + "[" + entry.getValue() + "]";
            joiner.add(stat);
        }

        return joiner.toString();
    }
}
