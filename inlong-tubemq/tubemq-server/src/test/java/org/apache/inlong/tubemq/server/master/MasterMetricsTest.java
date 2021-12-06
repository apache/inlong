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

package org.apache.inlong.tubemq.server.master;

import org.apache.inlong.tubemq.corebase.metric.MetricValues;
import org.apache.inlong.tubemq.server.master.metrics.MasterMetrics;
import org.apache.inlong.tubemq.server.master.metrics.MasterMetricsHolder;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MasterMetricsTest {
    private static final Logger logger = LoggerFactory.getLogger(MasterMetricsTest.class);

    @Test
    public void testMasterMetrics() {
        try {
            MasterMetrics metrics = new MasterMetrics();
            // test case 1, set data
            metrics.getConsumerOnlineCnt().incrementAndGet();
            metrics.getConsumerOnlineCnt().incrementAndGet();
            metrics.getConsumerTmoTotCnt().incrementAndGet();
            metrics.getConsumerTmoTotCnt().incrementAndGet();
            metrics.getConsumeGroupCnt().incrementAndGet();
            metrics.getConsumeGroupCnt().incrementAndGet();
            metrics.getConsumeGroupTmoTotCnt().incrementAndGet();
            metrics.getCltBalConsumeGroupCnt().incrementAndGet();
            metrics.getCltBalGroupTmototCnt().incrementAndGet();

            metrics.getProducerOnlineCnt().incrementAndGet();
            metrics.getProducerOnlineCnt().incrementAndGet();
            metrics.getProducerTmoTotCnt().incrementAndGet();
            metrics.getProducerTmoTotCnt().incrementAndGet();

            metrics.getBrokerConfigCnt().incrementAndGet();
            metrics.getBrokerConfigCnt().incrementAndGet();
            metrics.getBrokerOnlineCnt().incrementAndGet();
            metrics.getBrokerOnlineCnt().incrementAndGet();
            metrics.getBrokerTmoTotCnt().incrementAndGet();

            metrics.getBrokerAbnCurCnt().incrementAndGet();
            metrics.getBrokerAbnCurCnt().incrementAndGet();
            metrics.getBrokerAbnTotCnt().incrementAndGet();
            metrics.getBrokerAbnTotCnt().incrementAndGet();
            metrics.getBrokerFbdCurCnt().incrementAndGet();
            metrics.getBrokerFbdCurCnt().incrementAndGet();
            metrics.getBrokerFbdTotCnt().incrementAndGet();
            metrics.getBrokerFbdTotCnt().incrementAndGet();
            metrics.getBrokerFbdTotCnt().incrementAndGet();

            metrics.getSvrBalDuration().update(100);
            metrics.getSvrBalDuration().update(500);
            metrics.getSvrBalDuration().update(300);

            metrics.getSvrBalDurationMin().update(700);
            metrics.getSvrBalDurationMin().update(200);
            metrics.getSvrBalDurationMin().update(300);

            metrics.getSvrBalDurationMax().update(700);
            metrics.getSvrBalDurationMax().update(1000);
            metrics.getSvrBalDurationMax().update(300);

            metrics.getSvrBalResetDurMin().update(700);
            metrics.getSvrBalResetDurMin().update(200);
            metrics.getSvrBalResetDurMin().update(300);

            metrics.getSvrBalResetDurMax().update(700);
            metrics.getSvrBalResetDurMax().update(1000);
            metrics.getSvrBalResetDurMax().update(300);

            metrics.getSvrBalConEventConsumerCnt().incrementAndGet();
            metrics.getSvrBalConEventConsumerCnt().incrementAndGet();
            metrics.getSvrBalConEventConsumerCnt().incrementAndGet();

            metrics.getSvrBalDisConEventConsumerCnt().incrementAndGet();
            metrics.getSvrBalDisConEventConsumerCnt().incrementAndGet();
            metrics.getSvrBalDisConEventConsumerCnt().incrementAndGet();
            // get metric and compare data
            MetricValues result1 = metrics.getMetrics();
            Assert.assertEquals(Long.valueOf(2),
                    result1.getMetricValues().get(metrics.getConsumerOnlineCnt().getName()));
            Assert.assertEquals(Long.valueOf(2),
                    result1.getMetricValues().get(metrics.getConsumerTmoTotCnt().getName()));
            Assert.assertEquals(Long.valueOf(2),
                    result1.getMetricValues().get(metrics.getConsumeGroupCnt().getName()));
            Assert.assertEquals(Long.valueOf(1),
                    result1.getMetricValues().get(metrics.getCltBalConsumeGroupCnt().getName()));
            Assert.assertEquals(Long.valueOf(1),
                    result1.getMetricValues().get(metrics.getCltBalGroupTmototCnt().getName()));

            Assert.assertEquals(Long.valueOf(2),
                    result1.getMetricValues().get(metrics.getProducerOnlineCnt().getName()));
            Assert.assertEquals(Long.valueOf(2),
                    result1.getMetricValues().get(metrics.getProducerTmoTotCnt().getName()));
            Assert.assertEquals(Long.valueOf(2),
                    result1.getMetricValues().get(metrics.getBrokerConfigCnt().getName()));
            Assert.assertEquals(Long.valueOf(2),
                    result1.getMetricValues().get(metrics.getBrokerOnlineCnt().getName()));
            Assert.assertEquals(Long.valueOf(1),
                    result1.getMetricValues().get(metrics.getBrokerTmoTotCnt().getName()));

            Assert.assertEquals(Long.valueOf(2),
                    result1.getMetricValues().get(metrics.getBrokerAbnCurCnt().getName()));
            Assert.assertEquals(Long.valueOf(2),
                    result1.getMetricValues().get(metrics.getBrokerAbnTotCnt().getName()));
            Assert.assertEquals(Long.valueOf(2),
                    result1.getMetricValues().get(metrics.getBrokerFbdCurCnt().getName()));
            Assert.assertEquals(Long.valueOf(3),
                    result1.getMetricValues().get(metrics.getBrokerFbdTotCnt().getName()));

            Assert.assertEquals(Long.valueOf(300),
                    result1.getMetricValues().get(metrics.getSvrBalDuration().getName()));
            Assert.assertEquals(Long.valueOf(200),
                    result1.getMetricValues().get(metrics.getSvrBalDurationMin().getName()));
            Assert.assertEquals(Long.valueOf(1000),
                    result1.getMetricValues().get(metrics.getSvrBalDurationMax().getName()));
            Assert.assertEquals(Long.valueOf(200),
                    result1.getMetricValues().get(metrics.getSvrBalResetDurMin().getName()));
            Assert.assertEquals(Long.valueOf(1000),
                    result1.getMetricValues().get(metrics.getSvrBalResetDurMax().getName()));

            Assert.assertEquals(Long.valueOf(3),
                    result1.getMetricValues().get(
                            metrics.getSvrBalConEventConsumerCnt().getName()));

            Assert.assertEquals(Long.valueOf(3),
                    result1.getMetricValues().get(
                            metrics.getSvrBalDisConEventConsumerCnt().getName()));

            // get and reset value 2
            final MetricValues result2 = metrics.getAndReSetMetrics();
            // update metric data to 3
            metrics.getConsumerOnlineCnt().incrementAndGet();
            metrics.getConsumerOnlineCnt().decrementAndGet();
            metrics.getConsumeGroupCnt().incrementAndGet();
            metrics.getConsumeGroupTmoTotCnt().incrementAndGet();
            metrics.getCltBalConsumeGroupCnt().incrementAndGet();
            metrics.getCltBalGroupTmototCnt().incrementAndGet();

            metrics.getProducerOnlineCnt().incrementAndGet();
            metrics.getProducerOnlineCnt().incrementAndGet();

            metrics.getBrokerConfigCnt().incrementAndGet();
            metrics.getBrokerConfigCnt().incrementAndGet();
            metrics.getBrokerOnlineCnt().decrementAndGet();
            metrics.getBrokerOnlineCnt().decrementAndGet();
            metrics.getBrokerTmoTotCnt().incrementAndGet();

            metrics.getBrokerAbnCurCnt().incrementAndGet();
            metrics.getBrokerAbnCurCnt().incrementAndGet();
            metrics.getBrokerFbdCurCnt().incrementAndGet();
            metrics.getBrokerFbdCurCnt().incrementAndGet();
            metrics.getBrokerFbdTotCnt().incrementAndGet();

            metrics.getSvrBalDuration().update(100);
            metrics.getSvrBalDuration().update(700);
            metrics.getSvrBalDuration().update(20);

            metrics.getSvrBalDurationMin().update(1000);
            metrics.getSvrBalDurationMin().update(50);
            metrics.getSvrBalDurationMin().update(3000);

            metrics.getSvrBalDurationMax().update(700);
            metrics.getSvrBalDurationMax().update(800);
            metrics.getSvrBalDurationMax().update(300);

            metrics.getSvrBalResetDurMin().update(700);
            metrics.getSvrBalResetDurMin().update(10);
            metrics.getSvrBalResetDurMin().update(300);

            metrics.getSvrBalResetDurMax().update(700);
            metrics.getSvrBalResetDurMax().update(2000);
            metrics.getSvrBalResetDurMax().update(300);

            metrics.getSvrBalConEventConsumerCnt().incrementAndGet();
            metrics.getSvrBalConEventConsumerCnt().incrementAndGet();
            metrics.getSvrBalConEventConsumerCnt().incrementAndGet();

            metrics.getSvrBalDisConEventConsumerCnt().incrementAndGet();
            metrics.getSvrBalDisConEventConsumerCnt().incrementAndGet();

            // get metric and compare data
            MetricValues result3 = metrics.getMetrics();
            Assert.assertEquals(result1.getLastResetTime(),
                    result2.getLastResetTime());
            Assert.assertEquals(Long.valueOf(2),
                    result3.getMetricValues().get(metrics.getConsumerOnlineCnt().getName()));
            Assert.assertEquals(Long.valueOf(0),
                    result3.getMetricValues().get(metrics.getConsumerTmoTotCnt().getName()));
            Assert.assertEquals(Long.valueOf(3),
                    result3.getMetricValues().get(metrics.getConsumeGroupCnt().getName()));
            Assert.assertEquals(Long.valueOf(2),
                    result3.getMetricValues().get(metrics.getCltBalConsumeGroupCnt().getName()));
            Assert.assertEquals(Long.valueOf(1),
                    result3.getMetricValues().get(metrics.getCltBalGroupTmototCnt().getName()));

            Assert.assertEquals(Long.valueOf(4),
                    result3.getMetricValues().get(metrics.getProducerOnlineCnt().getName()));
            Assert.assertEquals(Long.valueOf(0),
                    result3.getMetricValues().get(metrics.getProducerTmoTotCnt().getName()));
            Assert.assertEquals(Long.valueOf(4),
                    result3.getMetricValues().get(metrics.getBrokerConfigCnt().getName()));
            Assert.assertEquals(Long.valueOf(0),
                    result3.getMetricValues().get(metrics.getBrokerOnlineCnt().getName()));
            Assert.assertEquals(Long.valueOf(1),
                    result3.getMetricValues().get(metrics.getBrokerTmoTotCnt().getName()));

            Assert.assertEquals(Long.valueOf(4),
                    result3.getMetricValues().get(metrics.getBrokerAbnCurCnt().getName()));
            Assert.assertEquals(Long.valueOf(2),
                    result3.getMetricValues().get(metrics.getBrokerAbnTotCnt().getName()));
            Assert.assertEquals(Long.valueOf(4),
                    result3.getMetricValues().get(metrics.getBrokerFbdCurCnt().getName()));
            Assert.assertEquals(Long.valueOf(3),
                    result3.getMetricValues().get(metrics.getBrokerFbdTotCnt().getName()));

            Assert.assertEquals(Long.valueOf(20),
                    result3.getMetricValues().get(metrics.getSvrBalDuration().getName()));
            Assert.assertEquals(Long.valueOf(50),
                    result3.getMetricValues().get(metrics.getSvrBalDurationMin().getName()));
            Assert.assertEquals(Long.valueOf(800),
                    result3.getMetricValues().get(metrics.getSvrBalDurationMax().getName()));
            Assert.assertEquals(Long.valueOf(10),
                    result3.getMetricValues().get(metrics.getSvrBalResetDurMin().getName()));
            Assert.assertEquals(Long.valueOf(2000),
                    result3.getMetricValues().get(metrics.getSvrBalResetDurMax().getName()));

            Assert.assertEquals(Long.valueOf(6),
                    result3.getMetricValues().get(
                            metrics.getSvrBalConEventConsumerCnt().getName()));
            Assert.assertEquals(Long.valueOf(5),
                    result3.getMetricValues().get(
                            metrics.getSvrBalDisConEventConsumerCnt().getName()));

        } catch (Exception ex) {
            logger.error("error happens" + ex);
        }
    }

    @Test
    public void testMasterMetricsHolder() {
        try {
            // test case 1, set data
            // add 12 consumer, 8 group, 4 client balance
            MasterMetricsHolder.incConsumerCnt(false, false);
            MasterMetricsHolder.incConsumerCnt(false, true);
            MasterMetricsHolder.incConsumerCnt(false, false);
            MasterMetricsHolder.incConsumerCnt(false, true);
            MasterMetricsHolder.incConsumerCnt(true, false);
            MasterMetricsHolder.incConsumerCnt(true, false);
            MasterMetricsHolder.incConsumerCnt(true, true);
            MasterMetricsHolder.incConsumerCnt(true, true);
            MasterMetricsHolder.incConsumerCnt(true, false);
            MasterMetricsHolder.incConsumerCnt(true, false);
            MasterMetricsHolder.incConsumerCnt(true, true);
            MasterMetricsHolder.incConsumerCnt(true, true);
            // dec 8 consumer, add 4 timeout consumer,
            // dec 4 group, add 2 timeout group, dec 2 client balance group
            MasterMetricsHolder.decConsumerCnt(false, false, false);
            MasterMetricsHolder.decConsumerCnt(false, false, true);
            MasterMetricsHolder.decConsumerCnt(false, true, false);
            MasterMetricsHolder.decConsumerCnt(false, true, true);
            MasterMetricsHolder.decConsumerCnt(true, false, false);
            MasterMetricsHolder.decConsumerCnt(true, false, true);
            MasterMetricsHolder.decConsumerCnt(true, true, false);
            MasterMetricsHolder.decConsumerCnt(true, true, true);
            // dec 4 group, add 2 timeout group, dec 2 client balance group
            MasterMetricsHolder.decConsumeGroupCnt(false, false);
            MasterMetricsHolder.decConsumeGroupCnt(false, true);
            MasterMetricsHolder.decConsumeGroupCnt(true, false);
            MasterMetricsHolder.decConsumeGroupCnt(true, true);
            // add 3 producer
            // dec 3 producer, 2 timeout producer
            MasterMetricsHolder.incProducerCnt();
            MasterMetricsHolder.incProducerCnt();
            MasterMetricsHolder.incProducerCnt();
            MasterMetricsHolder.decProducerCnt(false);
            MasterMetricsHolder.decProducerCnt(true);
            MasterMetricsHolder.decProducerCnt(true);
            // add 3 disconcnt
            // dec 2 disconcnt
            MasterMetricsHolder.incSvrBalDisConConsumerCnt();
            MasterMetricsHolder.incSvrBalDisConConsumerCnt();
            MasterMetricsHolder.incSvrBalDisConConsumerCnt();
            MasterMetricsHolder.decSvrBalDisConConsumerCnt();
            MasterMetricsHolder.decSvrBalDisConConsumerCnt();
            // add 3 concnt
            // dec 2 concnt
            MasterMetricsHolder.incSvrBalConEventConsumerCnt();
            MasterMetricsHolder.incSvrBalConEventConsumerCnt();
            MasterMetricsHolder.incSvrBalConEventConsumerCnt();
            MasterMetricsHolder.decSvrBalConEventConsumerCnt();
            MasterMetricsHolder.decSvrBalConEventConsumerCnt();
            // add 3 broker configure count
            // dec 2 broker configure count
            MasterMetricsHolder.incBrokerConfigCnt();
            MasterMetricsHolder.incBrokerConfigCnt();
            MasterMetricsHolder.incBrokerConfigCnt();
            MasterMetricsHolder.decBrokerConfigCnt();
            MasterMetricsHolder.decBrokerConfigCnt();
            // add 3 broker online count
            // dec 2 broker online count, 1 timeout count
            MasterMetricsHolder.incBrokerOnlineCnt();
            MasterMetricsHolder.incBrokerOnlineCnt();
            MasterMetricsHolder.incBrokerOnlineCnt();
            MasterMetricsHolder.decBrokerOnlineCnt(false);
            MasterMetricsHolder.decBrokerOnlineCnt(true);
            // add 3 broker abnormal count, 3 total abnormal count
            // dec 1 broker abnormal count
            MasterMetricsHolder.incBrokerAbnormalCnt();
            MasterMetricsHolder.decBrokerAbnormalCnt();
            MasterMetricsHolder.incBrokerAbnormalCnt();
            MasterMetricsHolder.incBrokerAbnormalCnt();
            // add 4 broker forbidden count, 4 total forbidden count
            // dec 1 broker forbidden count
            MasterMetricsHolder.incBrokerForbiddenCnt();
            MasterMetricsHolder.decBrokerForbiddenCnt();
            MasterMetricsHolder.incBrokerForbiddenCnt();
            MasterMetricsHolder.incBrokerForbiddenCnt();
            MasterMetricsHolder.incBrokerForbiddenCnt();
            // max: 1000, min 100
            MasterMetricsHolder.updSvrBalanceDurations(300);
            MasterMetricsHolder.updSvrBalanceDurations(500);
            MasterMetricsHolder.updSvrBalanceDurations(100);
            MasterMetricsHolder.updSvrBalanceDurations(1000);
            // max: 5000, min 500
            MasterMetricsHolder.updSvrBalResetDurations(3000);
            MasterMetricsHolder.updSvrBalResetDurations(500);
            MasterMetricsHolder.updSvrBalResetDurations(1000);
            MasterMetricsHolder.updSvrBalResetDurations(5000);
            // get metric and compare data
            MasterMetrics metrics = MasterMetricsHolder.getStatsInfo();
            MetricValues result1 = metrics.getMetrics();
            Assert.assertEquals(Long.valueOf(4),
                    result1.getMetricValues().get(metrics.getConsumerOnlineCnt().getName()));
            Assert.assertEquals(Long.valueOf(4),
                    result1.getMetricValues().get(metrics.getConsumerTmoTotCnt().getName()));
            Assert.assertEquals(Long.valueOf(0),
                    result1.getMetricValues().get(metrics.getConsumeGroupCnt().getName()));
            Assert.assertEquals(Long.valueOf(0),
                    result1.getMetricValues().get(metrics.getCltBalConsumeGroupCnt().getName()));
            Assert.assertEquals(Long.valueOf(2),
                    result1.getMetricValues().get(metrics.getCltBalGroupTmototCnt().getName()));

            Assert.assertEquals(Long.valueOf(0),
                    result1.getMetricValues().get(metrics.getProducerOnlineCnt().getName()));
            Assert.assertEquals(Long.valueOf(2),
                    result1.getMetricValues().get(metrics.getProducerTmoTotCnt().getName()));
            Assert.assertEquals(Long.valueOf(1),
                    result1.getMetricValues().get(metrics.getBrokerConfigCnt().getName()));
            Assert.assertEquals(Long.valueOf(1),
                    result1.getMetricValues().get(metrics.getBrokerOnlineCnt().getName()));
            Assert.assertEquals(Long.valueOf(1),
                    result1.getMetricValues().get(metrics.getBrokerTmoTotCnt().getName()));

            Assert.assertEquals(Long.valueOf(2),
                    result1.getMetricValues().get(metrics.getBrokerAbnCurCnt().getName()));
            Assert.assertEquals(Long.valueOf(3),
                    result1.getMetricValues().get(metrics.getBrokerAbnTotCnt().getName()));
            Assert.assertEquals(Long.valueOf(3),
                    result1.getMetricValues().get(metrics.getBrokerFbdCurCnt().getName()));
            Assert.assertEquals(Long.valueOf(4),
                    result1.getMetricValues().get(metrics.getBrokerFbdTotCnt().getName()));

            Assert.assertEquals(Long.valueOf(1000),
                    result1.getMetricValues().get(metrics.getSvrBalDuration().getName()));
            Assert.assertEquals(Long.valueOf(100),
                    result1.getMetricValues().get(metrics.getSvrBalDurationMin().getName()));
            Assert.assertEquals(Long.valueOf(1000),
                    result1.getMetricValues().get(metrics.getSvrBalDurationMax().getName()));
            Assert.assertEquals(Long.valueOf(500),
                    result1.getMetricValues().get(metrics.getSvrBalResetDurMin().getName()));
            Assert.assertEquals(Long.valueOf(5000),
                    result1.getMetricValues().get(metrics.getSvrBalResetDurMax().getName()));

            Assert.assertEquals(Long.valueOf(1),
                    result1.getMetricValues().get(
                            metrics.getSvrBalConEventConsumerCnt().getName()));
            Assert.assertEquals(Long.valueOf(1),
                    result1.getMetricValues().get(
                            metrics.getSvrBalDisConEventConsumerCnt().getName()));

            // get and reset value 2
            final MetricValues result2 = metrics.getAndReSetMetrics();
            // update metric data to 3
            // test case 3, set data
            // add 3 consumer, 2 group, 1 client balance
            MasterMetricsHolder.incConsumerCnt(false, false);
            MasterMetricsHolder.incConsumerCnt(true, false);
            MasterMetricsHolder.incConsumerCnt(true, true);
            // dec 2 consumer, add 1 timeout consumer,
            // dec 1 group, add 1 timeout group
            MasterMetricsHolder.decConsumerCnt(true, true, true);
            MasterMetricsHolder.decConsumerCnt(false, false, true);
            // dec 1 group, add 1 timeout group
            MasterMetricsHolder.decConsumeGroupCnt(true, false);
            // add 2 producer
            // dec 1 producer
            MasterMetricsHolder.incProducerCnt();
            MasterMetricsHolder.incProducerCnt();
            MasterMetricsHolder.decProducerCnt(false);
            // add 1 abnormal ,dec 1 abnormal
            MasterMetricsHolder.incBrokerAbnormalCnt();
            MasterMetricsHolder.decBrokerAbnormalCnt();

            // max: 1000, min 100
            MasterMetricsHolder.updSvrBalanceDurations(5000);
            MasterMetricsHolder.updSvrBalanceDurations(500);
            MasterMetricsHolder.updSvrBalanceDurations(100);
            MasterMetricsHolder.updSvrBalanceDurations(8000);
            // max: 5000, min 500
            MasterMetricsHolder.updSvrBalResetDurations(2000);
            MasterMetricsHolder.updSvrBalResetDurations(100);
            MasterMetricsHolder.updSvrBalResetDurations(1000);
            MasterMetricsHolder.updSvrBalResetDurations(4000);

            // get metric and compare data
            MetricValues result3 = metrics.getMetrics();
            Assert.assertEquals(result1.getLastResetTime(),
                    result2.getLastResetTime());
            Assert.assertEquals(Long.valueOf(5),
                    result3.getMetricValues().get(metrics.getConsumerOnlineCnt().getName()));
            Assert.assertEquals(Long.valueOf(1),
                    result3.getMetricValues().get(metrics.getConsumerTmoTotCnt().getName()));
            Assert.assertEquals(Long.valueOf(0),
                    result3.getMetricValues().get(metrics.getConsumeGroupCnt().getName()));
            Assert.assertEquals(Long.valueOf(0),
                    result3.getMetricValues().get(metrics.getCltBalConsumeGroupCnt().getName()));
            Assert.assertEquals(Long.valueOf(1),
                    result3.getMetricValues().get(metrics.getCltBalGroupTmototCnt().getName()));

            Assert.assertEquals(Long.valueOf(1),
                    result3.getMetricValues().get(metrics.getProducerOnlineCnt().getName()));
            Assert.assertEquals(Long.valueOf(0),
                    result3.getMetricValues().get(metrics.getProducerTmoTotCnt().getName()));
            Assert.assertEquals(Long.valueOf(1),
                    result3.getMetricValues().get(metrics.getBrokerConfigCnt().getName()));
            Assert.assertEquals(Long.valueOf(1),
                    result3.getMetricValues().get(metrics.getBrokerOnlineCnt().getName()));
            Assert.assertEquals(Long.valueOf(0),
                    result3.getMetricValues().get(metrics.getBrokerTmoTotCnt().getName()));

            Assert.assertEquals(Long.valueOf(2),
                    result3.getMetricValues().get(metrics.getBrokerAbnCurCnt().getName()));
            Assert.assertEquals(Long.valueOf(3),
                    result3.getMetricValues().get(metrics.getBrokerAbnTotCnt().getName()));
            Assert.assertEquals(Long.valueOf(3),
                    result3.getMetricValues().get(metrics.getBrokerFbdCurCnt().getName()));
            Assert.assertEquals(Long.valueOf(3),
                    result3.getMetricValues().get(metrics.getBrokerFbdTotCnt().getName()));

            Assert.assertEquals(Long.valueOf(8000),
                    result3.getMetricValues().get(metrics.getSvrBalDuration().getName()));
            Assert.assertEquals(Long.valueOf(100),
                    result3.getMetricValues().get(metrics.getSvrBalDurationMin().getName()));
            Assert.assertEquals(Long.valueOf(8000),
                    result3.getMetricValues().get(metrics.getSvrBalDurationMax().getName()));
            Assert.assertEquals(Long.valueOf(100),
                    result3.getMetricValues().get(metrics.getSvrBalResetDurMin().getName()));
            Assert.assertEquals(Long.valueOf(4000),
                    result3.getMetricValues().get(metrics.getSvrBalResetDurMax().getName()));

            Assert.assertEquals(Long.valueOf(1),
                    result3.getMetricValues().get(
                            metrics.getSvrBalConEventConsumerCnt().getName()));
            Assert.assertEquals(Long.valueOf(1),
                    result3.getMetricValues().get(
                            metrics.getSvrBalDisConEventConsumerCnt().getName()));
        } catch (Exception ex) {
            logger.error("error happens" + ex);
        }
    }
}
