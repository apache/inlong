package org.apache.inlong.audit.tool.evaluator;

import org.apache.inlong.audit.tool.config.AlertPolicy;
import org.apache.inlong.audit.tool.config.AppConfig;
import org.apache.inlong.audit.tool.manager.ManagerClient;
import org.apache.inlong.audit.tool.DTO.AuditData;
import org.apache.inlong.audit.tool.DTO.MetricData;
import org.apache.inlong.audit.tool.reporter.PrometheusReporter;
import org.apache.inlong.audit.tool.reporter.OpenTelemetryReporter;

import java.util.ArrayList;
import java.util.List;

public class AlertEvaluator {
    private final PrometheusReporter prometheusReporter;
    private final OpenTelemetryReporter openTelemetryReporter;
    private AuditData auditData;
    private AlertPolicy policy;
    private ManagerClient managerClient;

    public AlertEvaluator(PrometheusReporter prometheusReporter, OpenTelemetryReporter openTelemetryReporter, AppConfig appConfig) {
        this.prometheusReporter = prometheusReporter;
        this.openTelemetryReporter = openTelemetryReporter;
        // 需要创建ManagerClient实例
        //this.managerClient = new ManagerClient(appConfig); // 实际使用时应传入正确的AppConfig
    }

    private MetricData calculateMetricData(AuditData auditData) {
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
        // 实现具体的告警判断逻辑
        double dataLossRate = auditData.getDataLossRate();
        
        // 获取阈值
        double threshold = policy.getThreshold();
        String comparisonOperator = policy.getComparisonOperator();
        
        // 根据比较操作符判断是否触发告警
        switch (comparisonOperator) {
            case ">":
                return dataLossRate > threshold;
            case ">=":
                return dataLossRate >= threshold;
            case "<":
                return dataLossRate < threshold;
            case "<=":
                return dataLossRate <= threshold;
            case "==":
                return dataLossRate == threshold;
            case "!=":
                return dataLossRate != threshold;
            default:
                return false;
        }
    }

    public void triggerAlert(AuditData auditData, AlertPolicy policy) {
        this.auditData = auditData;
        this.policy = policy;
        List<String> enabledPlatforms = getEnabledPlatforms(policy);

        // 假设 metricData 是从 auditData 中提取的某种指标数据
        MetricData metricData = calculateMetricData(auditData);
        
        // 添加告警信息
        if (metricData.getAlertInfo() == null) {
            metricData.setAlertInfo(new MetricData.AlertInfo(policy.getAlertType()));
        }

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
                    System.out.println("Invalid platform: " + platform);
                    break;
            }
        }
    }

}