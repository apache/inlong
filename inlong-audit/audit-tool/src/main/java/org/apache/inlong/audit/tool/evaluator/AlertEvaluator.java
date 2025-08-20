package org.apache.inlong.audit.tool.evaluator;

import org.apache.inlong.audit.tool.config.AlertPolicy;
import org.apache.inlong.audit.tool.reporter.PrometheusReporter;
import org.apache.inlong.audit.tool.reporter.OpenTelemetryReporter;

import java.util.ArrayList;
import java.util.List;

public class AlertEvaluator {
    private final PrometheusReporter prometheusReporter;
    private final OpenTelemetryReporter openTelemetryReporter;

    public AlertEvaluator(PrometheusReporter prometheusReporter, OpenTelemetryReporter openTelemetryReporter) {
        this.prometheusReporter = prometheusReporter;
        this.openTelemetryReporter = openTelemetryReporter;
    }
    public List<String> getEnabledPlatforms(AlertPolicy policy) {
        List<String> enabledPlatforms = new ArrayList<>();
        // 获取策略中配置的目标平台
        List<String> targets = policy.getTargets();
        if (targets != null) {
            for (String target : targets) {
                if ("prometheus".equalsIgnoreCase(target)) {
                    enabledPlatforms.add("prometheus");
                } else if ("opentelemetry".equalsIgnoreCase(target)) {
                    enabledPlatforms.add("opentelemetry");
                }
            }
        }

        return enabledPlatforms;
    }



    public boolean shouldTriggerAlert(AuditData auditData, AlertPolicy policy) {
        // 写一个switch选择语句，根据不同的策略把不同的指标加入运算，看是否超过阈值
        return false;
    }

    public void triggerAlert(AuditData auditData, AlertPolicy policy) {
        // 获取启用的平台
        List<String> enabledPlatforms = getEnabledPlatforms(policy);

        // 根据启用的平台进行上报
        for (String platform : enabledPlatforms) {
            switch (platform.toLowerCase()) {
                case "prometheus":
                    prometheusReporter.report(auditData, policy);
                    break;
                case "opentelemetry":
                    openTelemetryReporter.report(auditData, policy);
                    break;
                default:
                    // 处理未知平台或记录日志
                    break;
            }
        }
    }
}