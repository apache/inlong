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
import org.apache.inlong.tubemq.server.broker.metrics.BrokerMetricsHolder;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BrokerMetricsTest {
    private static final Logger logger =
            LoggerFactory.getLogger(BrokerMetricsTest.class);

    @Test
    public void testBrokerMetrics() {
        try {
            BrokerMetrics metrics = new BrokerMetrics();
            // test case 1, set data
            metrics.getIoExceptionCnt().incrementAndGet();
            metrics.getZkExceptionCnt().incrementAndGet();
            metrics.getConsumerOnlineCnt().incrementAndGet();
            metrics.getConsumerOnlineCnt().incrementAndGet();
            metrics.getConsumerTmoTotCnt().incrementAndGet();
            metrics.getHbExceptionCnt().incrementAndGet();
            metrics.getMasterNoNodeCnt().incrementAndGet();

            metrics.getSyncDataDurMax().update(20000);
            metrics.getSyncDataDurMax().update(10000);
            metrics.getSyncDataDurMax().update(30000);
            metrics.getSyncDataDurMin().update(2000);
            metrics.getSyncDataDurMin().update(1000);
            metrics.getSyncDataDurMin().update(3000);

            metrics.getSyncZkDurMax().update(20000);
            metrics.getSyncZkDurMax().update(1000);
            metrics.getSyncZkDurMax().update(30000);

            metrics.getSyncZkDurMin().update(2000);
            metrics.getSyncZkDurMin().update(100);
            metrics.getSyncZkDurMin().update(3000);
            // get metric and compare data
            MetricValues result1 = metrics.getMetrics();
            Assert.assertEquals(Long.valueOf(1),
                    result1.getMetricValues().get(metrics.getIoExceptionCnt().getName()));
            Assert.assertEquals(Long.valueOf(1),
                    result1.getMetricValues().get(metrics.getZkExceptionCnt().getName()));
            Assert.assertEquals(Long.valueOf(2),
                    result1.getMetricValues().get(metrics.getConsumerOnlineCnt().getName()));
            Assert.assertEquals(Long.valueOf(1),
                    result1.getMetricValues().get(metrics.getConsumerTmoTotCnt().getName()));
            Assert.assertEquals(Long.valueOf(1),
                    result1.getMetricValues().get(metrics.getHbExceptionCnt().getName()));
            Assert.assertEquals(Long.valueOf(1),
                    result1.getMetricValues().get(metrics.getMasterNoNodeCnt().getName()));
            Assert.assertEquals(Long.valueOf(30000),
                    result1.getMetricValues().get(metrics.getSyncDataDurMax().getName()));
            Assert.assertEquals(Long.valueOf(1000),
                    result1.getMetricValues().get(metrics.getSyncDataDurMin().getName()));
            Assert.assertEquals(Long.valueOf(30000),
                    result1.getMetricValues().get(metrics.getSyncZkDurMax().getName()));
            Assert.assertEquals(Long.valueOf(100),
                    result1.getMetricValues().get(metrics.getSyncZkDurMin().getName()));
            // get and reset value 2
            final MetricValues result2 = metrics.getAndReSetMetrics();
            // update metric data to 3
            metrics.getIoExceptionCnt().incrementAndGet();
            metrics.getZkExceptionCnt().incrementAndGet();
            metrics.getConsumerOnlineCnt().incrementAndGet();
            metrics.getConsumerOnlineCnt().decrementAndGet();
            metrics.getConsumerTmoTotCnt().incrementAndGet();
            metrics.getHbExceptionCnt().incrementAndGet();
            metrics.getMasterNoNodeCnt().incrementAndGet();

            metrics.getSyncDataDurMax().update(10);
            metrics.getSyncDataDurMax().update(10000);
            metrics.getSyncDataDurMax().update(20000);
            metrics.getSyncDataDurMin().update(10);
            metrics.getSyncDataDurMin().update(1000);
            metrics.getSyncDataDurMin().update(5000);

            metrics.getSyncZkDurMax().update(10);
            metrics.getSyncZkDurMax().update(1000);
            metrics.getSyncZkDurMax().update(2000);

            metrics.getSyncZkDurMin().update(3000);
            metrics.getSyncZkDurMin().update(10);
            metrics.getSyncZkDurMin().update(6000);

            MetricValues result3 = metrics.getMetrics();
            Assert.assertEquals(result1.getLastResetTime(),
                    result2.getLastResetTime());
            Assert.assertEquals(Long.valueOf(1),
                    result3.getMetricValues().get(metrics.getIoExceptionCnt().getName()));
            Assert.assertEquals(Long.valueOf(1),
                    result3.getMetricValues().get(metrics.getZkExceptionCnt().getName()));
            Assert.assertEquals(Long.valueOf(2),
                    result3.getMetricValues().get(metrics.getConsumerOnlineCnt().getName()));
            Assert.assertEquals(Long.valueOf(1),
                    result3.getMetricValues().get(metrics.getConsumerTmoTotCnt().getName()));
            Assert.assertEquals(Long.valueOf(1),
                    result3.getMetricValues().get(metrics.getHbExceptionCnt().getName()));
            Assert.assertEquals(Long.valueOf(1),
                    result3.getMetricValues().get(metrics.getMasterNoNodeCnt().getName()));
            Assert.assertEquals(Long.valueOf(20000),
                    result3.getMetricValues().get(metrics.getSyncDataDurMax().getName()));
            Assert.assertEquals(Long.valueOf(10),
                    result3.getMetricValues().get(metrics.getSyncDataDurMin().getName()));
            Assert.assertEquals(Long.valueOf(2000),
                    result3.getMetricValues().get(metrics.getSyncZkDurMax().getName()));
            Assert.assertEquals(Long.valueOf(10),
                    result3.getMetricValues().get(metrics.getSyncZkDurMin().getName()));
        } catch (Exception ex) {
            logger.error("error happens" + ex);
        }
    }

    @Test
    public void testBrokerMetricsHolder() {
        try {
            // case 1, set data
            BrokerMetricsHolder.incConsumerCnt();
            BrokerMetricsHolder.decConsumerCnt(false);
            BrokerMetricsHolder.incConsumerCnt();
            BrokerMetricsHolder.decConsumerCnt(true);
            BrokerMetricsHolder.incConsumerCnt();

            BrokerMetricsHolder.incZKExceptionCnt();
            BrokerMetricsHolder.incZKExceptionCnt();

            BrokerMetricsHolder.incMasterNoNodeCnt();
            BrokerMetricsHolder.incHBExceptionCnt();
            BrokerMetricsHolder.incIOExceptionCnt();
            BrokerMetricsHolder.incZKExceptionCnt();

            BrokerMetricsHolder.updSyncDataDurations(10000);
            BrokerMetricsHolder.updSyncDataDurations(2000);
            BrokerMetricsHolder.updSyncDataDurations(20000);

            BrokerMetricsHolder.updSyncZKDurations(1000);
            BrokerMetricsHolder.updSyncZKDurations(30);
            BrokerMetricsHolder.updSyncZKDurations(30000);
            // get data and check
            BrokerMetrics metrics = BrokerMetricsHolder.getStatsInfo();
            MetricValues result1 = metrics.getMetrics();
            Assert.assertEquals(Long.valueOf(1),
                    result1.getMetricValues().get(metrics.getConsumerOnlineCnt().getName()));
            Assert.assertEquals(Long.valueOf(1),
                    result1.getMetricValues().get(metrics.getConsumerTmoTotCnt().getName()));
            Assert.assertEquals(Long.valueOf(3),
                    result1.getMetricValues().get(metrics.getZkExceptionCnt().getName()));
            Assert.assertEquals(Long.valueOf(1),
                    result1.getMetricValues().get(metrics.getMasterNoNodeCnt().getName()));
            Assert.assertEquals(Long.valueOf(1),
                    result1.getMetricValues().get(metrics.getHbExceptionCnt().getName()));
            Assert.assertEquals(Long.valueOf(1),
                    result1.getMetricValues().get(metrics.getIoExceptionCnt().getName()));
            Assert.assertEquals(Long.valueOf(20000),
                    result1.getMetricValues().get(metrics.getSyncDataDurMax().getName()));
            Assert.assertEquals(Long.valueOf(2000),
                    result1.getMetricValues().get(metrics.getSyncDataDurMin().getName()));
            Assert.assertEquals(Long.valueOf(30000),
                    result1.getMetricValues().get(metrics.getSyncZkDurMax().getName()));
            Assert.assertEquals(Long.valueOf(30),
                    result1.getMetricValues().get(metrics.getSyncZkDurMin().getName()));

            // get and reset value 2
            final MetricValues result2 = metrics.getAndReSetMetrics();
            BrokerMetricsHolder.incConsumerCnt();
            BrokerMetricsHolder.incConsumerCnt();
            BrokerMetricsHolder.incConsumerCnt();
            BrokerMetricsHolder.decConsumerCnt(false);
            BrokerMetricsHolder.decConsumerCnt(true);

            BrokerMetricsHolder.incZKExceptionCnt();
            BrokerMetricsHolder.incZKExceptionCnt();

            BrokerMetricsHolder.updSyncDataDurations(1);
            BrokerMetricsHolder.updSyncDataDurations(5000);
            BrokerMetricsHolder.updSyncDataDurations(30000);

            BrokerMetricsHolder.updSyncZKDurations(100);
            BrokerMetricsHolder.updSyncZKDurations(10);
            BrokerMetricsHolder.updSyncZKDurations(5000);
            // get and check 3
            MetricValues result3 = metrics.getMetrics();
            Assert.assertEquals(result1.getLastResetTime(),
                    result2.getLastResetTime());
            Assert.assertEquals(Long.valueOf(2),
                    result3.getMetricValues().get(metrics.getConsumerOnlineCnt().getName()));
            Assert.assertEquals(Long.valueOf(1),
                    result3.getMetricValues().get(metrics.getConsumerTmoTotCnt().getName()));
            Assert.assertEquals(Long.valueOf(2),
                    result3.getMetricValues().get(metrics.getZkExceptionCnt().getName()));
            Assert.assertEquals(Long.valueOf(0),
                    result3.getMetricValues().get(metrics.getMasterNoNodeCnt().getName()));
            Assert.assertEquals(Long.valueOf(0),
                    result3.getMetricValues().get(metrics.getHbExceptionCnt().getName()));
            Assert.assertEquals(Long.valueOf(0),
                    result3.getMetricValues().get(metrics.getIoExceptionCnt().getName()));
            Assert.assertEquals(Long.valueOf(30000),
                    result3.getMetricValues().get(metrics.getSyncDataDurMax().getName()));
            Assert.assertEquals(Long.valueOf(1),
                    result3.getMetricValues().get(metrics.getSyncDataDurMin().getName()));
            Assert.assertEquals(Long.valueOf(5000),
                    result3.getMetricValues().get(metrics.getSyncZkDurMax().getName()));
            Assert.assertEquals(Long.valueOf(10),
                    result3.getMetricValues().get(metrics.getSyncZkDurMin().getName()));
        } catch (Exception ex) {
            logger.error("error happens" + ex);
        }
    }
}
