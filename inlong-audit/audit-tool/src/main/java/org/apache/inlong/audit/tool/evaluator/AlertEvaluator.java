package org.apache.inlong.audit.tool.evaluator;

import org.apache.inlong.audit.tool.config.AlertPolicy;
import org.apache.inlong.audit.tool.manager.ManagerClient;
import org.apache.inlong.audit.tool.metric.AuditData;
import org.apache.inlong.audit.tool.metric.MetricData;
import org.apache.inlong.audit.tool.reporter.PrometheusReporter;
import org.apache.inlong.audit.tool.reporter.OpenTelemetryReporter;

import java.util.ArrayList;
import java.util.List;

public class AlertEvaluator {
    private final PrometheusReporter prometheusReporter;
    private final OpenTelemetryReporter openTelemetryReporter;
    private AuditData auditData;
    private AlertPolicy policy;

    public AlertEvaluator(PrometheusReporter prometheusReporter, OpenTelemetryReporter openTelemetryReporter) {
        this.prometheusReporter = prometheusReporter;
        this.openTelemetryReporter = openTelemetryReporter;
        this.auditData = ManagerClient.fetchAuditData();
        this.policy = (AlertPolicy) ManagerClient.fetchAlertPolicies();
    }



    private MetricData calculateMetricData(AuditData auditData) {
        this.auditData = ManagerClient.fetchAuditData();
        return new MetricData(auditData.getGroupId(), auditData.getStreamId(), auditData.getDataLossRate(),
                auditData.getDataLossCount(), auditData.getAuditCount(), auditData.getExpectedCount(),
                auditData.getReceivedCount());
    }
    public List<String> getEnabledPlatforms(AlertPolicy policy) {
        List<String> enabledPlatforms = new ArrayList<>();
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
        this.auditData = auditData;
        this.policy = policy;
        // TODO: 实现具体的告警判断逻辑
        double dataLossRate = auditData.getDataLossRate(auditData);
        switch (policy.getAlertType()) {
            case "HighVolumeAlertPolicy":
                // 获取数据丢失率
                // 判断是否满足高数据丢失率告警条件
                if (dataLossRate > policy.getThresholds().get("warning").getCount()) {
                    return true;
                }
                break;
            case "LowVolumeAlertPolicy":
                // 获取数据丢失率
                // 判断是否满足低数据丢失率告警条件
                if (dataLossRate < policy.getThresholds().get("critical").getCount()) {
                    return true;
                }
                break;
            default:
                // 获取数据丢失率
                // 默认策略，判断是否满足数据丢失率告警条件
                if (dataLossRate > policy.getThresholds().get("warning").getCount() ||
                        dataLossRate < policy.getThresholds().get("critical").getCount()) {
                    return true;
                    break;
                }
                return false;
        }
    }
        public void triggerAlert(AuditData auditData, AlertPolicy policy){
            this.auditData = auditData;
            this.policy = policy;
            List<String> enabledPlatforms = getEnabledPlatforms(policy);

            // 假设 metricData 是从 auditData 中提取的某种指标数据
            MetricData metricData = calculateMetricData(auditData);

            for (String platform : enabledPlatforms) {
                switch (platform.toLowerCase()) {
                    case "prometheus":
                        prometheusReporter.report(metricData);
                        break;
                    case "opentelemetry":
                        openTelemetryReporter.report(metricData);
                        break;
                    default:
                        // 可添加日志记录
                        break;
                }
            }
        }
    }
