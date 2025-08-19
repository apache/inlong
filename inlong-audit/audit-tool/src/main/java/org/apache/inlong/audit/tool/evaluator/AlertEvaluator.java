package org.apache.inlong.audit.tool.evaluator;

import org.apache.inlong.audit.tool.config.AlertPolicy;
import org.apache.inlong.audit.tool.manager.ManagerClient;
import org.apache.inlong.audit.tool.reporter.PrometheusReporter;
import org.apache.inlong.audit.tool.reporter.OpenTelemetryReporter;

import java.util.List;

public class AlertEvaluator {

    private final ManagerClient managerClient;
    private final PrometheusReporter prometheusReporter;
    private final OpenTelemetryReporter openTelemetryReporter;

    public AlertEvaluator(ManagerClient managerClient, PrometheusReporter prometheusReporter, OpenTelemetryReporter openTelemetryReporter) {
        this.managerClient = managerClient;
        this.prometheusReporter = prometheusReporter;
        this.openTelemetryReporter = openTelemetryReporter;
    }

    public void evaluateAlerts(List<AuditData> auditDataList) {
        List<AlertPolicy> alertPolicies = managerClient.getAlertPolicies();

        for (AuditData auditData : auditDataList) {
            for (AlertPolicy policy : alertPolicies) {
                if (shouldTriggerAlert(auditData, policy)) {
                    triggerAlert(auditData, policy);
                }
            }
        }
    }

    private boolean shouldTriggerAlert(AuditData auditData, AlertPolicy policy) {
        // Implement logic to compare auditData against policy thresholds
        return false; // Placeholder for actual comparison logic
    }

    private void triggerAlert(AuditData auditData, AlertPolicy policy) {
        // Report to Prometheus
        prometheusReporter.report(auditData, policy);
        
        // Report to OpenTelemetry
        openTelemetryReporter.report(auditData, policy);
    }
}