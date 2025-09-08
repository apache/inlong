package org.apache.inlong.audit.tool;

import org.apache.inlong.audit.tool.basemetric.util.AuditSQLUtil;
import org.apache.inlong.audit.tool.config.AppConfig;
import org.apache.inlong.audit.tool.reporter.PrometheusReporter;
import org.apache.inlong.audit.tool.task.AuditCheckTask;

import java.util.Map;

public class AuditToolMain {
    private static final long DEFAULT_INTERVAL = 30000;
    public static void main(String[] args) {
        AppConfig appConfig=new AppConfig();
        PrometheusReporter prometheusReporter = new PrometheusReporter();
        Map<String, Object> prometheusConfig = appConfig.getPrometheusConfig();
        prometheusReporter.init(prometheusConfig);

        AuditSQLUtil.initialize(appConfig.getProperties());

        // Schedule the audit check task
        AuditCheckTask auditCheckTask = new AuditCheckTask(prometheusReporter, null, appConfig);
        auditCheckTask.start();

        // Keep the application running
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            auditCheckTask.stop();
            System.out.println("Audit Tool stopped.");
        }));

        System.out.println("Audit Tool started.");
    }
}