package org.apache.inlong.audit.tool;

import org.apache.inlong.audit.tool.config.AppConfig;
import org.apache.inlong.audit.tool.evaluator.AlertEvaluator;
import org.apache.inlong.audit.tool.manager.ManagerClient;
import org.apache.inlong.audit.tool.reporter.PrometheusReporter;
import org.apache.inlong.audit.tool.reporter.OpenTelemetryReporter;
import org.apache.inlong.audit.tool.task.AuditCheckTask;

public class AuditToolMain {

    public static void main(String[] args) {
        // Load application configuration
        AppConfig appConfig = AppConfig.load();

        // Initialize manager client
        ManagerClient managerClient = new ManagerClient(appConfig.getManagerUrl());

        // Initialize alert evaluator
        AlertEvaluator alertEvaluator = new AlertEvaluator(managerClient);

        // Initialize reporters
        PrometheusReporter prometheusReporter = new PrometheusReporter(appConfig.getPrometheusConfig());
        OpenTelemetryReporter openTelemetryReporter = new OpenTelemetryReporter(appConfig.getOpenTelemetryConfig());

        // Schedule the audit check task
        AuditCheckTask auditCheckTask = new AuditCheckTask(alertEvaluator, prometheusReporter, openTelemetryReporter);
        auditCheckTask.start();

        // Keep the application running
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            auditCheckTask.stop();
            System.out.println("Audit Tool stopped.");
        }));

        System.out.println("Audit Tool started.");
    }
}