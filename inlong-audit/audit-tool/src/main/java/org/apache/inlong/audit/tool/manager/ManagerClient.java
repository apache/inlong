package org.apache.inlong.audit.tool.manager;

import org.apache.inlong.audit.tool.config.AppConfig;
import org.apache.inlong.audit.tool.config.AlertPolicy;
import org.apache.inlong.audit.tool.reporter.PrometheusReporter;
import org.apache.inlong.audit.tool.reporter.OpenTelemetryReporter;

import java.util.List;

public class ManagerClient {

    private final AppConfig appConfig;
    private final PrometheusReporter prometheusReporter;
    private final OpenTelemetryReporter openTelemetryReporter;

    public ManagerClient(AppConfig appConfig) {
        this.appConfig = appConfig;
        this.prometheusReporter = new PrometheusReporter(appConfig.getPrometheusConfig());
        this.openTelemetryReporter = new OpenTelemetryReporter(appConfig.getOpenTelemetryConfig());
    }

    public List<AlertPolicy> fetchAlertPolicies() {
        // Logic to interact with the manager service and fetch alert policies
        // This could involve making an HTTP request to the manager's API
        return null; // Replace with actual implementation
    }

    public void reportAlert(String alertMessage) {
        // Logic to report alerts to Prometheus and OpenTelemetry
        prometheusReporter.report(alertMessage);
        openTelemetryReporter.report(alertMessage);
    }

    public void updateAlertPolicy(AlertPolicy policy) {
        // Logic to update alert policies in the manager service
        // This could involve making an HTTP request to the manager's API
    }
}