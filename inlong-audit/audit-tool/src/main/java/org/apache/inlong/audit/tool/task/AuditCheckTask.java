package org.apache.inlong.audit.tool.task;

import lombok.Getter;
import org.apache.inlong.audit.tool.DTO.AlertPolicy;
import org.apache.inlong.audit.tool.evaluator.AlertEvaluator;
import org.apache.inlong.audit.tool.manager.ManagerClient;
import org.apache.inlong.audit.tool.DTO.AuditData;
import org.apache.inlong.audit.tool.reporter.PrometheusReporter;
import org.apache.inlong.audit.tool.reporter.OpenTelemetryReporter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * 审计检查任务类，定期获取审计数据并评估告警
 */
public class AuditCheckTask {
    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
    private final AlertEvaluator alertEvaluator;
    @Getter
    private PrometheusReporter prometheusReporter;
    @Getter
    private OpenTelemetryReporter openTelemetryReporter;
    private final ManagerClient managerClient;
    private static final Logger LOGGER = LoggerFactory.getLogger(ManagerClient.class);

    public AuditCheckTask(PrometheusReporter prometheusReporter, OpenTelemetryReporter openTelemetryReporter, ManagerClient managerClient, AlertEvaluator alertEvaluator) {
        this.prometheusReporter = prometheusReporter;
        this.openTelemetryReporter = openTelemetryReporter;
        this.managerClient = managerClient;
        this.alertEvaluator = alertEvaluator;
    }

    /**
     * 启动审计检查任务
     */
    public void start() {
        scheduler.scheduleAtFixedRate(this::checkAuditData, 0, 30, TimeUnit.SECONDS);
    }

    /**
     * 检查审计数据并触发告警评估
     */
    private void checkAuditData() {
        final long startTime = System.currentTimeMillis();
        final long timeoutMillis = 10 * 60 * 1000; // 10分钟超时
        int attempt = 0;
        boolean success = false;

        while (!success && (System.currentTimeMillis() - startTime) < timeoutMillis) {
            attempt++;
            try {
                LOGGER.info("Attempt #{} to check audit data", attempt);

                // 获取审计数据
                List<AuditData> auditDataList = managerClient.fetchAuditData();

                // 获取告警策略
                List<AlertPolicy> policies = managerClient.fetchAlertPolicies();

                // 对每个审计数据和每个策略进行评估
                for (AuditData auditData : auditDataList) {
                    for (AlertPolicy policy : policies) {
                        if (alertEvaluator.shouldTriggerAlert(auditData, policy)) {
                            alertEvaluator.triggerAlert(auditData, policy);
                        }
                    }
                }

                // 成功完成，退出循环
                success = true;
                LOGGER.info("Successfully checked audit data on attempt #{}", attempt);

            } catch (Exception e) {
                // 记录错误但继续重试
                LOGGER.error("Error occurred on attempt #{}: {}", attempt, e.getMessage());
                LOGGER.debug("Error details", e);

                // 检查是否超时
                if ((System.currentTimeMillis() - startTime) >= timeoutMillis) {
                    LOGGER.error("Timeout reached after {} minutes. Terminating thread.", 10);
                    break;
                }

                // 等待3秒后重试
                try {
                    Thread.sleep(3000);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    LOGGER.warn("Thread interrupted during retry wait");
                    break;
                }
            }
        }

        // 如果10分钟后仍未成功，终止线程
        if (!success) {
            LOGGER.error("Failed to check audit data after {} attempts and {} minutes. Terminating thread.",
                    attempt, 10);
            scheduler.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }
    
    /**
     * 停止审计检查任务
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