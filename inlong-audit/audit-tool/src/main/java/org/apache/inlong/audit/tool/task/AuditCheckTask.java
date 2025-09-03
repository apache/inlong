package org.apache.inlong.audit.tool.task;

import org.apache.inlong.audit.tool.config.AlertPolicy;
import org.apache.inlong.audit.tool.config.AppConfig;
import org.apache.inlong.audit.tool.metric.AuditData;
import org.apache.inlong.audit.tool.evaluator.AlertEvaluator;
import org.apache.inlong.audit.tool.manager.ManagerClient;


import java.util.List;
import java.util.Timer;
import java.util.TimerTask;

public class AuditCheckTask {
    private final ManagerClient managerClient;
    private final AlertEvaluator alertEvaluator;
    private final Timer timer;

    public AuditCheckTask(AppConfig appConfig) {
        this.managerClient = new ManagerClient(appConfig);
        this.alertEvaluator = new AlertEvaluator();
        this.timer = new Timer();
    }

    public void start(long interval) {
        timer.scheduleAtFixedRate(new TimerTask() {
            @Override
            public void run() {
                checkAuditData();
            }
        }, 0, interval);
    }

    private void checkAuditData() {
        List<AlertPolicy> alertPolicies = managerClient.fetchAlertPolicies();
        AuditData auditData = managerClient.fetchAuditData();
        for (AlertPolicy policy : alertPolicies) {
            if (alertEvaluator.shouldTriggerAlert(auditData, policy)) {
                alertEvaluator.triggerAlert(auditData, policy);
            }
        }
    }

    public void stop() {
        timer.cancel();
    }
}