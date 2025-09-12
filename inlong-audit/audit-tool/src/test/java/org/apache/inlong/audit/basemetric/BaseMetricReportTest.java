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

package org.apache.inlong.audit.basemetric;

import io.prometheus.client.CollectorRegistry;
import org.apache.inlong.audit.tool.basemetric.BaseMetricReporter;
import org.apache.inlong.audit.tool.basemetric.util.AuditSQLUtil;
import org.apache.inlong.audit.tool.config.AppConfig;
import org.apache.inlong.audit.tool.reporter.PrometheusReporter;
import org.junit.jupiter.api.Test;

import java.util.Map;

public class BaseMetricReportTest {
    /**
     * Unit testing method for reporting basic indicator data to Prometheus
     * @throws InterruptedException
     */
    @Test
    public void testBaseMetricReport() throws InterruptedException {
        // Create an instance of Prometheus metric reporter
        AppConfig appConfig = new AppConfig();
        PrometheusReporter prometheusReporter = new PrometheusReporter();
        Map<String, Object> prometheusConfig = appConfig.getPrometheusConfig();
        prometheusReporter.init(prometheusConfig);

        // Retrieve index registry reference
        CollectorRegistry registry = prometheusReporter.getRegistry();

        // Register basic indicators
        BaseMetricReporter baseMetricReporter = new BaseMetricReporter(registry);

        // If you want to test real data, set useFakeData to false
        // If you want to use simulated fake data, set userFakeData to true
        Boolean useFakeData=true;

        // Simulate timed reporting of basic indicator data: report every 10 seconds, for a total of 1000 reports
        int executionCount = 1000;
        for (int i = 0; i < executionCount; i++) {
            AuditSQLUtil.initialize(appConfig.getProperties());
            baseMetricReporter.reportBaseMetric();
            if (i < executionCount - 1) {
                Thread.sleep(10000);
            }
        }
        // close resource
        prometheusReporter.close();
    }
}
