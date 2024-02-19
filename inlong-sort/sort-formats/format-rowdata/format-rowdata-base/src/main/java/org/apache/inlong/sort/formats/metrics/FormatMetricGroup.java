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

package org.apache.inlong.sort.formats.metrics;

import org.apache.inlong.sort.formats.metrics.gauge.EventTimeDelayGauge;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.groups.OperatorMetricGroup;
import org.apache.flink.runtime.metrics.groups.ProxyMetricGroup;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.apache.flink.runtime.metrics.scope.ScopeFormat.asVariable;

/**
 * The {@link MetricGroup} for table formats.
 */
public class FormatMetricGroup extends ProxyMetricGroup<MetricGroup> {

    public static final String FORMAT_GROUP_NAME = "format";

    private Counter numRecordsSerializeError;
    private Gauge<?> numRecordsSerializeErrorDiff;

    private Counter numRecordsSerializeErrorIgnored;
    private Gauge<?> numRecordsSerializeErrorIgnoredDiff;

    private Counter numRecordsDeserializeError;
    private Gauge<?> numRecordsDeserializeErrorDiff;

    private Counter numRecordsDeserializeErrorIgnored;
    private Gauge<?> numRecordsDeserializeErrorIgnoredDiff;

    @Deprecated
    private EventTimeDelayGauge eventTimeDelayMillis;

    private final Map<String, String> tags;

    public FormatMetricGroup(OperatorMetricGroup parent, boolean isDeserializer) {
        this(parent, "", isDeserializer);
    }

    public FormatMetricGroup(OperatorMetricGroup parent, String name, boolean isDeserializer) {
        this(parent, name, isDeserializer, Collections.emptyMap());
    }

    public FormatMetricGroup(OperatorMetricGroup parent, String name, boolean isDeserializer,
            Map<String, String> tags) {
        super(parent.addGroup(name, StringUtils.join(tags)));
        this.tags = tags;

        if (isDeserializer) {
            this.eventTimeDelayMillis =
                    gauge(FormatMetricNames.GAUGE_EVENT_TIME_DELAY_MILLIS, new EventTimeDelayGauge());
            this.numRecordsDeserializeError = counter(FormatMetricNames.COUNTER_NUM_RECORDS_DESERIALIZE_ERROR);
            this.numRecordsDeserializeErrorIgnored =
                    counter(FormatMetricNames.COUNTER_NUM_RECORDS_DESERIALIZE_ERROR_IGNORED);
            this.numRecordsDeserializeErrorDiff = gauge(FormatMetricNames.GAUGE_NUM_RECORDS_DESERIALIZE_ERROR_DIFF,
                    new IncrementGauge(numRecordsDeserializeError));
            this.numRecordsDeserializeErrorIgnoredDiff =
                    gauge(FormatMetricNames.GAUGE_NUM_RECORDS_DESERIALIZE_ERROR_IGNORED_DIFF,
                            new IncrementGauge(numRecordsDeserializeErrorIgnored));
        } else {
            this.numRecordsSerializeError = counter(FormatMetricNames.COUNTER_NUM_RECORDS_SERIALIZE_ERROR);
            this.numRecordsSerializeErrorIgnored =
                    counter(FormatMetricNames.COUNTER_NUM_RECORDS_SERIALIZE_ERROR_IGNORED);
            this.numRecordsSerializeErrorDiff = gauge(FormatMetricNames.GAUGE_NUM_RECORDS_SERIALIZE_ERROR_DIFF,
                    new IncrementGauge(numRecordsSerializeError));
            this.numRecordsSerializeErrorIgnoredDiff =
                    gauge(FormatMetricNames.GAUGE_NUM_RECORDS_SERIALIZE_ERROR_IGNORED_DIFF,
                            new IncrementGauge(numRecordsSerializeErrorIgnored));
        }
    }

    @Override
    public Map<String, String> getAllVariables() {
        Map<String, String> variables = new HashMap<>();
        for (Map.Entry<String, String> entry : tags.entrySet()) {
            variables.put(asVariable(entry.getKey()), entry.getValue());
        }
        variables.putAll(parentMetricGroup.getAllVariables());
        return variables;
    }

    // @Override
    public Map<String, String> getTags() {
        return tags;
    }

    public Counter getNumRecordsSerializeError() {
        return numRecordsSerializeError;
    }

    public Counter getNumRecordsSerializeErrorIgnored() {
        return numRecordsSerializeErrorIgnored;
    }

    public Counter getNumRecordsDeserializeError() {
        return numRecordsDeserializeError;
    }

    public Counter getNumRecordsDeserializeErrorIgnored() {
        return numRecordsDeserializeErrorIgnored;
    }

    public EventTimeDelayGauge getEventTimeDelayMillis() {
        return eventTimeDelayMillis;
    }

    public Gauge<?> getNumRecordsSerializeErrorDiff() {
        return numRecordsSerializeErrorDiff;
    }

    public Gauge<?> getNumRecordsSerializeErrorIgnoredDiff() {
        return numRecordsSerializeErrorIgnoredDiff;
    }

    public Gauge<?> getNumRecordsDeserializeErrorDiff() {
        return numRecordsDeserializeErrorDiff;
    }

    public Gauge<?> getNumRecordsDeserializeErrorIgnoredDiff() {
        return numRecordsDeserializeErrorIgnoredDiff;
    }
}
