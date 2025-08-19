package org.apache.inlong.audit.tool.task;

import org.apache.inlong.audit.tool.config.AppConfig;
import org.apache.inlong.audit.tool.evaluator.AlertEvaluator;
import org.apache.inlong.audit.tool.manager.ManagerClient;
import org.apache.inlong.audit.tool.reporter.PrometheusReporter;
import org.apache.inlong.audit.tool.reporter.OpenTelemetryReporter;

import java.util.Timer;
import java.util.TimerTask;

public class AuditCheckTask {

    private final ManagerClient managerClient;
    private final AlertEvaluator alertEvaluator;
    private final PrometheusReporter prometheusReporter;
    private final OpenTelemetryReporter openTelemetryReporter;
    private final Timer timer;

    public AuditCheckTask(AppConfig appConfig) {
        this.managerClient = new ManagerClient(appConfig);
        this.alertEvaluator = new AlertEvaluator(managerClient);
        this.prometheusReporter = new PrometheusReporter();
        this.openTelemetryReporter = new OpenTelemetryReporter();
        this.timer = new Timer();
    }

    public void start(long interval) {
        timer.scheduleAtFixedRate(new TimerTask() {
            @Override
            public void run() {
                checkAuditData();
            }
        }, 0, interval);
    }

    private void checkAuditData() {
        // Fetch audit data and evaluate against alert policies
        var auditData = managerClient.fetchAuditData();
        var alerts = alertEvaluator.evaluate(auditData);

        // Report alerts to observability platforms
        if (!alerts.isEmpty()) {
            prometheusReporter.report(alerts);
            openTelemetryReporter.report(alerts);
        }
    }

    public void stop() {
        timer.cancel();
    }
}