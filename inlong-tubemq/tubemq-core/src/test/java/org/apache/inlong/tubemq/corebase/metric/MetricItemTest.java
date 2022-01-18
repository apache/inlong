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

import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedHashMap;
import java.util.Map;

public class MetricItemTest {
    private static final Logger logger =
            LoggerFactory.getLogger(MetricItemTest.class);

    @Test
    public void testMetricItem() {
        try {
            final CountMetricItem countMetricItem =
                    new CountMetricItem("CountMetricItem");
            final GaugeNormMetricItem gaugeNormMetricItem =
                    new GaugeNormMetricItem("GaugeNormMetricItem");
            final GaugeMaxMetricItem gaugeMaxMetricItem =
                    new GaugeMaxMetricItem("GaugeMaxMetricItem");
            final GaugeMinMetricItem gaugeMinMetricItem =
                    new GaugeMinMetricItem("GaugeMinMetricItem");

            countMetricItem.incrementAndGet();
            countMetricItem.incrementAndGet();
            countMetricItem.incrementAndGet();
            countMetricItem.decrementAndGet();

            gaugeNormMetricItem.update(1000);
            gaugeNormMetricItem.update(2000);
            gaugeNormMetricItem.update(500);

            gaugeMaxMetricItem.update(1000);
            gaugeMaxMetricItem.update(5000);
            gaugeMaxMetricItem.update(3000);

            gaugeMinMetricItem.update(1000);
            gaugeMinMetricItem.update(1);
            gaugeMinMetricItem.update(10000);

            Assert.assertEquals(2, countMetricItem.getValue(false));
            Assert.assertEquals(500, gaugeNormMetricItem.getValue(false));
            Assert.assertEquals(5000, gaugeMaxMetricItem.getValue(false));
            Assert.assertEquals(1, gaugeMinMetricItem.getValue(false));

            countMetricItem.getValue(true);
            gaugeNormMetricItem.getValue(true);
            gaugeMaxMetricItem.getValue(true);
            gaugeMinMetricItem.getValue(true);

            Assert.assertEquals(0, countMetricItem.getValue(false));
            Assert.assertEquals(500, gaugeNormMetricItem.getValue(false));
            Assert.assertEquals(0, gaugeMaxMetricItem.getValue(false));
            Assert.assertEquals(Long.MAX_VALUE, gaugeMinMetricItem.getValue(false));

            Assert.assertEquals(MetricType.COUNTER.getId(),
                    countMetricItem.getMetricType().getId());
            Assert.assertEquals(MetricValueType.MAX.getId(),
                    countMetricItem.getMetricValueType().getId());
            Assert.assertEquals(MetricType.GAUGE.getId(),
                    gaugeNormMetricItem.getMetricType().getId());
            Assert.assertEquals(MetricValueType.NORMAL.getId(),
                    gaugeNormMetricItem.getMetricValueType().getId());
            Assert.assertEquals(MetricType.GAUGE.getId(),
                    gaugeMaxMetricItem.getMetricType().getId());
            Assert.assertEquals(MetricValueType.MAX.getId(),
                    gaugeMaxMetricItem.getMetricValueType().getId());
            Assert.assertEquals(MetricType.GAUGE.getId(),
                    gaugeMinMetricItem.getMetricType().getId());
            Assert.assertEquals(MetricValueType.MIN.getId(),
                    gaugeMinMetricItem.getMetricValueType().getId());
        } catch (Exception ex) {
            logger.error("error happens" + ex);
        }
    }

    @Test
    public void testProcTimeDltMetricItem() {
        try {
            final TimeDltMetricItem procDltMetricItem =
                    new TimeDltMetricItem("test");

            procDltMetricItem.updProcTimeDlt(2);
            procDltMetricItem.updProcTimeDlt(6);
            procDltMetricItem.updProcTimeDlt(15);
            procDltMetricItem.updProcTimeDlt(30);
            procDltMetricItem.updProcTimeDlt(60);
            procDltMetricItem.updProcTimeDlt(120);
            procDltMetricItem.updProcTimeDlt(240);
            procDltMetricItem.updProcTimeDlt(270);
            procDltMetricItem.updProcTimeDlt(520);
            procDltMetricItem.updProcTimeDlt(1030);
            procDltMetricItem.updProcTimeDlt(2060);
            procDltMetricItem.updProcTimeDlt(3060);
            procDltMetricItem.updProcTimeDlt(8060);
            procDltMetricItem.updProcTimeDlt(16370);
            procDltMetricItem.updProcTimeDlt(20000);
            procDltMetricItem.updProcTimeDlt(33000);
            procDltMetricItem.updProcTimeDlt(55000);

            Map<String, Long> metricValues = new LinkedHashMap<>();
            procDltMetricItem.getMapMetrics(metricValues, false);

            StringBuilder strBuff = new StringBuilder(512);
            procDltMetricItem.getStrMetrics(strBuff, false);
            String result = strBuff.toString();
            strBuff.delete(0, strBuff.length());

            procDltMetricItem.getStrMetrics(strBuff, true);
            result = strBuff.toString();
            strBuff.delete(0, strBuff.length());

            procDltMetricItem.getStrMetrics(strBuff, false);
            result = strBuff.toString();
            strBuff.delete(0, strBuff.length());
        } catch (Exception ex) {
            logger.error("error happens" + ex);
        }
    }
}
