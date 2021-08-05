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

package org.apache.inlong.sort.flink.metrics;

import static com.google.common.base.Preconditions.checkNotNull;

import java.io.Serializable;
import java.util.Iterator;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class MetricsAggregator {

    /**
     * Aggregate Metrics.
     */
    public static class MetricsAggregateFunction implements
            AggregateFunction<MetricData, MetricAccumulator, MetricData> {

        private static final long serialVersionUID = -4980743351369740395L;

        @Override
        public MetricAccumulator createAccumulator() {
            return new MetricAccumulator();
        }

        @Override
        public MetricAccumulator add(MetricData value, MetricAccumulator accumulator) {
            if (accumulator.getMetricData() == null) {
                MetricData metricData = new MetricData(
                        value.getMetricSource(),
                        value.getMetricType(),
                        value.getTimestampMillis(),
                        value.getDataFlowId(),
                        value.getAttachment(),
                        value.getPartitions(),
                        value.getCount());
                accumulator.setMetricData(metricData);
            } else {
                accumulator.getMetricData().increase(value.getCount());
            }

            return accumulator;
        }

        @Override
        public MetricData getResult(MetricAccumulator accumulator) {
            return accumulator.getMetricData();
        }

        @Override
        public MetricAccumulator merge(MetricAccumulator a, MetricAccumulator b) {
            a.getMetricData().increase(b.getMetricData().getCount());
            return a;
        }
    }

    /**
     * Attach partition (window start) to the aggregated MetricData.
     */
    public static class MetricsProcessWindowFunction extends
            ProcessWindowFunction<MetricData, MetricData, String, TimeWindow> {

        private static final long serialVersionUID = 914156043444186657L;

        @Override
        public void process(
                String key,
                Context context,
                Iterable<MetricData> elements,
                Collector<MetricData> collector) {
            Iterator<MetricData> it = elements.iterator();

            if (!it.hasNext()) {
                return;
            }

            MetricData result = it.next();
            while (it.hasNext()) {
                result.increase(it.next().getCount());
            }
            result.setTimestampMillis(context.window().getStart());
            collector.collect(result);
        }
    }

    private static class MetricAccumulator implements Serializable {

        private static final long serialVersionUID = -7368055708339786552L;

        private MetricData metricData;

        public void setMetricData(MetricData metricData) {
            this.metricData = checkNotNull(metricData);
        }

        public MetricData getMetricData() {
            return metricData;
        }
    }
}
