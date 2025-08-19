package org.apache.inlong.audit.tool.reporter;

import io.prometheus.client.Gauge;
import io.prometheus.client.exporter.HTTPServer;
import org.apache.inlong.audit.tool.config.AppConfig;

import java.io.IOException;

public class PrometheusReporter {

    private final Gauge auditAlertGauge;

    public PrometheusReporter() {
        auditAlertGauge = Gauge.build()
                .name("audit_alerts_total")
                .help("Total number of audit alerts.")
                .labelNames("alert_type")
                .register();
    }

    public void reportAlert(String alertType, double value) {
        auditAlertGauge.labels(alertType).set(value);
    }

    public void startServer() throws IOException {
        int port = AppConfig.getPrometheusPort();
        new HTTPServer(port);
    }
}