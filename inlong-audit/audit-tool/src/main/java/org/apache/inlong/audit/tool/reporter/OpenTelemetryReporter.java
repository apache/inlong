package org.apache.inlong.audit.tool.reporter;

import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.metrics.LongCounter;
import io.opentelemetry.api.metrics.Meter;
import io.opentelemetry.api.metrics.MeterProvider;
import io.opentelemetry.api.metrics.ObservableLongGauge;
import io.opentelemetry.context.Context;

public class OpenTelemetryReporter {

    private final Meter meter;
    private final LongCounter alertCounter;

    public OpenTelemetryReporter(OpenTelemetry openTelemetry) {
        this.meter = openTelemetry.getMeter("audit-tool");
        this.alertCounter = meter.counterBuilder("audit_alerts_total")
                .setDescription("Total number of audit alerts reported")
                .setUnit("1")
                .build();
    }

    public void reportAlert(String alertType, long count) {
        alertCounter.add(count, Context.current());
        // Additional logic to report to OpenTelemetry can be added here
    }

    public void registerObservableGauge(String gaugeName, ObservableLongGauge gauge) {
        meter.gaugeBuilder(gaugeName)
                .setDescription("Current value of " + gaugeName)
                .setUnit("1")
                .buildWithCallback(observableGauge -> {
                    observableGauge.observe(gauge.getValue(), Context.current());
                });
    }
}