/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.inlong.tubemq.corebase.metric;

import java.util.concurrent.atomic.AtomicLong;
import org.apache.inlong.tubemq.corebase.utils.Tuple2;

public abstract class AbsMetricItem {

    protected final MetricType metricType;
    protected final MetricValueType valueType;
    protected final String name;
    protected final AtomicLong value = new AtomicLong(0);

    public AbsMetricItem(MetricType metricType, MetricValueType valueType,
                         String name, long initialValue) {
        this.metricType = metricType;
        this.valueType = valueType;
        this.name = name;
        this.value.set(initialValue);
    }

    public String getName() {
        return name;
    }

    public boolean isCounterMetric() {
        return metricType == MetricType.COUNTER;
    }

    public MetricType getMetricType() {
        return metricType;
    }

    public MetricValueType getMetricValueType() {
        return valueType;
    }

    public Tuple2<String, Long> getNameValue() {
        return new Tuple2<>(name, value.get());
    }

    public long incrementAndGet() {
        return value.incrementAndGet();
    }

    public long addAndGet(long dltData) {
        return value.addAndGet(dltData);
    }

    public boolean compareAndSet(long expect, long update) {
        return value.compareAndSet(expect, update);
    }

    public long decrementAndGet() {
        return value.decrementAndGet();
    }

    public abstract long getValue(boolean resetValue);

    public abstract boolean update(long newValue);
}
