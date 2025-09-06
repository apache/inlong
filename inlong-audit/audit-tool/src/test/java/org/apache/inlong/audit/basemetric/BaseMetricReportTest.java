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
        BaseMetricReporter baseMetricReporter = new BaseMetricReporter(appConfig.getProperties(),registry);

        // If you want to test real data, set useFakeData to false
        // If you want to use simulated fake data, set userFakeData to true
        Boolean useFakeData=true;

        // Simulate timed reporting of basic indicator data: report every 10 seconds, for a total of 1000 reports
        int executionCount = 1000;
        for (int i = 0; i < executionCount; i++) {
            if(useFakeData){
                baseMetricReporter.reportBaseMetric(true);
            }else{
                // Initialize database query tool
                AuditSQLUtil.initialize(appConfig.getProperties());
                baseMetricReporter.reportBaseMetric(false);
            }
            if (i < executionCount - 1) {
                Thread.sleep(10000);
            }
        }
        // close resource
        prometheusReporter.close();
    }
}
