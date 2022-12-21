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

package org.apache.inlong.sort.base.metric.sub;

import com.google.common.collect.Maps;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.metrics.MetricGroup;
import org.apache.inlong.sort.base.Constants;
import org.apache.inlong.sort.base.metric.MetricOption;
import org.apache.inlong.sort.base.metric.MetricOption.RegisteredMetric;
import org.apache.inlong.sort.base.metric.MetricState;
import org.apache.inlong.sort.base.metric.SinkMetricData;

import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.inlong.sort.base.Constants.DELIMITER;
import static org.apache.inlong.sort.base.Constants.DIRTY_BYTES_OUT;
import static org.apache.inlong.sort.base.Constants.DIRTY_RECORDS_OUT;
import static org.apache.inlong.sort.base.Constants.NUM_BYTES_OUT;
import static org.apache.inlong.sort.base.Constants.NUM_RECORDS_OUT;

public class SinkTopicMetricData extends SinkMetricData implements SinkSubMetricData {

    /**
     * The sink metric data map
     */
    private final Map<String, SinkMetricData> sinkMetricMap = Maps.newHashMap();

    public SinkTopicMetricData(MetricOption option, MetricGroup metricGroup) {
        super(option, metricGroup);
    }

    public void sendOutMetrics(String topic, long rowCount, long rowSize) {
        if (StringUtils.isBlank(topic)) {
            invoke(rowCount, rowSize);
            return;
        }
        SinkMetricData sinkMetricData = getSinkMetricData(topic);

        this.invoke(rowCount, rowSize);
        sinkMetricData.invoke(rowCount, rowSize);
    }

    public void sendDirtyMetrics(String topic, long rowCount, long rowSize) {
        if (StringUtils.isBlank(topic)) {
            invokeDirty(rowCount, rowSize);
            return;
        }
        SinkMetricData sinkMetricData = getSinkMetricData(topic);

        this.invokeDirty(rowCount, rowSize);
        sinkMetricData.invokeDirty(rowCount, rowSize);
    }

    private SinkMetricData getSinkMetricData(String topic) {
        SinkMetricData sinkMetricData;
        if (sinkMetricMap.containsKey(topic)) {
            sinkMetricData = sinkMetricMap.get(topic);
        } else {
            sinkMetricData = buildSinkMetricData(topic, null, this);
            sinkMetricMap.put(topic, sinkMetricData);
        }
        return sinkMetricData;
    }

    private SinkMetricData buildSinkMetricData(String topic, MetricState metricState, SinkMetricData sinkMetricData) {
        Map<String, String> labels = sinkMetricData.getLabels();
        String metricGroupLabels = labels.entrySet().stream().map(entry -> entry.getKey() + "=" + entry.getValue())
                .collect(Collectors.joining(DELIMITER));

        MetricOption metricOption = MetricOption.builder()
                .withInlongLabels(metricGroupLabels + DELIMITER + Constants.TOPIC_NAME + "=" + topic)
                .withInitRecords(metricState != null ? metricState.getMetricValue(NUM_RECORDS_OUT) : 0L)
                .withInitBytes(metricState != null ? metricState.getMetricValue(NUM_BYTES_OUT) : 0L)
                .withInitDirtyRecords(metricState != null ? metricState.getMetricValue(DIRTY_RECORDS_OUT) : 0L)
                .withInitDirtyBytes(metricState != null ? metricState.getMetricValue(DIRTY_BYTES_OUT) : 0L)
                .withRegisterMetric(RegisteredMetric.ALL)
                .build();
        return new SinkMetricData(metricOption, sinkMetricData.getMetricGroup());
    }

    @Override
    public Map<String, SinkMetricData> getSubSourceMetricMap() {
        return this.sinkMetricMap;
    }
}
