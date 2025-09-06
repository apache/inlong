package org.apache.inlong.audit.tool.task;

import org.apache.inlong.audit.tool.basemetric.BaseMetricReporter;
import org.apache.inlong.audit.tool.config.AppConfig;
import org.apache.inlong.audit.tool.evaluator.AlertEvaluator;
import org.apache.inlong.audit.tool.manager.ManagerClient;
import org.apache.inlong.audit.tool.reporter.PrometheusReporter;
import org.apache.inlong.audit.tool.reporter.OpenTelemetryReporter;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * 审计检查任务类，定期获取审计数据并评估告警
 */
public class AuditCheckTask {
    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
    private final AlertEvaluator alertEvaluator;
    private final PrometheusReporter prometheusReporter;
    private final OpenTelemetryReporter openTelemetryReporter;
    //private final ManagerClient managerClient;
    private final BaseMetricReporter baseMetricReporter;
    
    public AuditCheckTask(PrometheusReporter prometheusReporter, OpenTelemetryReporter openTelemetryReporter, AppConfig appconfig) {
        this.prometheusReporter = prometheusReporter;
        this.openTelemetryReporter = openTelemetryReporter;
        this.alertEvaluator = new AlertEvaluator(prometheusReporter, openTelemetryReporter, appconfig);
        // Need to create a Managerial Client instance
        //this.managerClient = new ManagerClient(appconfig);
        this.baseMetricReporter =new BaseMetricReporter(appconfig.getProperties(),prometheusReporter.getRegistry());
    }

    /**
     * Initiate audit inspection task
     */
    public void start() {
        scheduler.scheduleAtFixedRate(this::checkAuditData, 0, 30, TimeUnit.SECONDS);
    }
    
    /**
     * Check audit data and trigger alarm evaluation
     */
    private void checkAuditData() {
        try {
//            // Obtain audit data
//            AuditData auditData = managerClient.fetchAuditData();
//
//            // Obtain alarm strategy
//            List<AlertPolicy> policies = managerClient.fetchAlertPolicies();
//
//            // Evaluate each strategy
//            for (AlertPolicy policy : policies) {
//                if (alertEvaluator.shouldTriggerAlert(auditData, policy)) {
//                    alertEvaluator.triggerAlert(auditData, policy);
//                }
//            }
            try {
                baseMetricReporter.reportBaseMetric(false);
            }catch (Exception e){
                e.printStackTrace();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    
    /**
     * Stop auditing and inspection tasks
     */
    public void stop() {
        scheduler.shutdown();
        try {
            if (!scheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                scheduler.shutdownNow();
            }
        } catch (InterruptedException e) {
            scheduler.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }
}