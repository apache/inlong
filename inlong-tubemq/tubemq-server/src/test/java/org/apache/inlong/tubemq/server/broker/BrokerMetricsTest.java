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

package org.apache.inlong.tubemq.server.broker;

import org.apache.inlong.tubemq.corebase.metric.MetricValues;
import org.apache.inlong.tubemq.server.broker.metrics.BrokerMetrics;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BrokerMetricsTest {
    private static final Logger logger =
            LoggerFactory.getLogger(BrokerMetricsTest.class);

    @Test
    public void testAgentMetrics() {
        try {
            BrokerMetrics metrics = new BrokerMetrics();
            metrics.zkExceptionCnt.incrementAndGet();
            metrics.consumerTmoTotCnt.incrementAndGet();
            metrics.syncDataDurMax.update(10000);
            metrics.syncDataDurMin.update(2000);
            metrics.syncDataDurMax.update(20000);
            metrics.syncDataDurMin.update(1000);
            metrics.syncDataDurMin.update(3000);
            metrics.syncDataDurMax.update(30000);
            MetricValues result1 = metrics.getMetrics();
            Assert.assertEquals(Long.valueOf(1000),
                    result1.getMetricValues().get(metrics.syncDataDurMin.getName()));
            Assert.assertEquals(Long.valueOf(30000),
                    result1.getMetricValues().get(metrics.syncDataDurMax.getName()));
            Assert.assertEquals(Long.valueOf(1),
                    result1.getMetricValues().get(metrics.zkExceptionCnt.getName()));
            Assert.assertEquals(Long.valueOf(1),
                    result1.getMetricValues().get(metrics.consumerTmoTotCnt.getName()));
            // get and reset value
            MetricValues result2 = metrics.getAndReSetMetrics();
            metrics.zkExceptionCnt.incrementAndGet();
            metrics.zkExceptionCnt.getAndSet();
            metrics.consumerTmoTotCnt.incrementAndGet();
            metrics.consumerTmoTotCnt.update(10);
            metrics.syncDataDurMax.update(20000);
            metrics.syncDataDurMin.update(2000);
            MetricValues result3 = metrics.getMetrics();
            Assert.assertEquals(result1.getLastResetTime(),
                    result2.getLastResetTime());
            Assert.assertEquals(Long.valueOf(2000),
                    result3.getMetricValues().get(metrics.syncDataDurMin.getName()));
            Assert.assertEquals(Long.valueOf(20000),
                    result3.getMetricValues().get(metrics.syncDataDurMax.getName()));
            Assert.assertEquals(Long.valueOf(0),
                    result3.getMetricValues().get(metrics.zkExceptionCnt.getName()));
            Assert.assertEquals(Long.valueOf(10),
                    result3.getMetricValues().get(metrics.consumerTmoTotCnt.getName()));
        } catch (Exception ex) {
            logger.error("error happens" + ex);
        }
    }
}
