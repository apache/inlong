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

package org.apache.inlong.sort.hudi.sink;

import org.apache.inlong.sort.base.metric.MetricOption;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.operators.StreamSink;
import org.apache.hudi.sink.common.AbstractWriteOperator;
import org.apache.hudi.sink.common.WriteOperatorFactory;

/**
 * Operator for {@link StreamSink}.
 *
 * @param <I> The input type
 * <p>
 * Copy from org.apache.hudi:hudi-flink1.15-bundle:0.12.3
 */
public class StreamWriteOperator<I> extends AbstractWriteOperator<I> {

    public StreamWriteOperator(Configuration conf, MetricOption metricOption) {
        super(new StreamWriteFunction<>(conf, metricOption));
    }

    public static <I> WriteOperatorFactory<I> getFactory(Configuration conf, MetricOption metricOption) {
        return WriteOperatorFactory.instance(conf, new StreamWriteOperator<>(conf, metricOption));
    }
}
