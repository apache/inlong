package org.apache.inlong.audit.tool.basemetric.manager;

import io.prometheus.client.Gauge;
import io.prometheus.client.CollectorRegistry;

public class InlongAuditMetricsManager {

    private final Gauge messageNumGauge;
    private final Gauge messageSizeGauge;
    private final Gauge messageAvgDelayGauge;

    public InlongAuditMetricsManager(CollectorRegistry registry) {
        this.messageNumGauge = Gauge.build()
                .name("inlong_audit_log_message_num_interval")
                .help("Number of audit logs recorded by Inlong in the last interval")
                .register(registry);

        this.messageSizeGauge = Gauge.build()
                .name("inlong_audit_log_message_size_interval")
                .help("Size of audit logs recorded by Inlong in the last interval")
                .register(registry);

        this.messageAvgDelayGauge = Gauge.build()
                .name("inlong_audit_log_message_avg_delay_interval")
                .help("Average delay of audit logs recorded by Inlong in the last interval")
                .register(registry);
    }

    public void reportBasicMetrics(long messageNum, long messageSize, long avgDelay) {
        messageNumGauge.set(messageNum);
        messageSizeGauge.set(messageSize);
        messageAvgDelayGauge.set(avgDelay);
    }

    public void updateMessageNum(long num) {
        messageNumGauge.set(num);
    }

    public void updateMessageSize(long size) {
        messageSizeGauge.set(size);
    }

    public void updateMessageAvgDelay(long avgDelay) {
        messageAvgDelayGauge.set(avgDelay);
    }
}