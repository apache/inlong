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

package org.apache.inlong.tubemq.server.broker.metrics;

import org.apache.inlong.tubemq.corebase.metric.MetricValues;

/**
 * BrokerMetricMXBean
 * Broker's metric data access interface, including:
 * the getMetric() that  directly obtains data
 * the getAndReSetMetrics() that can clear the values of
 *     the counter, maximum and minimum extremum Gauge data
 */
public interface BrokerMetricMXBean {

    // get current metric data by viewing mode
    MetricValues getMetrics();

    // get current metric data and reset the Counter, maximum/minimum Gauge metric
    MetricValues getAndReSetMetrics();
}
