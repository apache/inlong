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

import org.apache.inlong.commons.config.metrics.MetricValue;
import org.apache.inlong.tubemq.server.master.metrics.MasterMetric;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class MasterMetricsTest {
    private static final Logger logger = LoggerFactory.getLogger(MasterMetricsTest.class);

    @Test
    public void testAgentMetrics() {
        try {
            MasterMetric taskMetrics = MasterMetric.create();
            taskMetrics.svrBalLatency.incrementAndGet();
            Map<String, MetricValue> result = taskMetrics.snapshot();
            Assert.assertEquals(1, taskMetrics.svrBalLatency.get());

        } catch (Exception ex) {
            logger.error("error happens" + ex);
        }
    }
}
