/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.inlong.audit.tool.task;

import org.apache.inlong.audit.tool.config.AppConfig;
import org.apache.inlong.audit.tool.config.ConfigConstants;
import org.apache.inlong.audit.tool.dto.AuditAlertRule;
import org.apache.inlong.audit.tool.entity.AuditMetric;
import org.apache.inlong.audit.tool.evaluator.AlertEvaluator;
import org.apache.inlong.audit.tool.manager.AuditAlertRuleManager;
import org.apache.inlong.audit.tool.service.AuditMetricService;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * AuditCheckTask class: Periodically fetches audit data and evaluates alert policies.
 */
public class AuditCheckTask {

    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
    private final AlertEvaluator alertEvaluator;
    private final AuditAlertRuleManager auditAlertRuleManager;
    private static final Logger LOGGER = LoggerFactory.getLogger(AuditAlertRuleManager.class);
    private final AuditMetricService auditMetricService;
    private Integer executionIntervalTime;
    private Integer intervalTimeMinute;
    private final Integer delayTimeMinute;
    private static final DateTimeFormatter LOGS_FMT = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    private String sourceAuditId;

    public AuditCheckTask(
            AuditAlertRuleManager auditAlertRuleManager, AlertEvaluator alertEvaluator, AppConfig appConfig) {
        this.auditAlertRuleManager = auditAlertRuleManager;
        this.alertEvaluator = alertEvaluator;
        this.auditMetricService = new AuditMetricService();
        try {
            this.executionIntervalTime =
                    Integer.valueOf(appConfig.getProperties().getProperty(ConfigConstants.KEY_DELAY_TIME, "1"));
            this.intervalTimeMinute =
                    Integer.parseInt(appConfig.getProperties().getProperty(ConfigConstants.KEY_INTERVAL_TIME, "1"));
            this.sourceAuditId = appConfig.getProperties().getProperty(ConfigConstants.KEY_SOURCE_AUDIT_ID, "5");
        } catch (Exception e) {
            LOGGER.info(
                    "Failed to read configuration information, default source AuditId is 5, delay execution time is 1, time interval is 1");
            this.executionIntervalTime = 1;
            this.intervalTimeMinute = 1;
            this.sourceAuditId = "5";
        }
        this.delayTimeMinute = executionIntervalTime;
    }

    /**
     * Initiate the audit inspection task
     */
    public void start() {
        scheduler.scheduleAtFixedRate(this::checkAuditData, 0, executionIntervalTime, TimeUnit.MINUTES);
    }

    /**
     * Check audit data and trigger alert evaluation.
     */
    private void checkAuditData() {
        // Obtain auditIds provided by the interface
        List<String> sinkAuditIds = auditAlertRuleManager.getAuditIds();
        if (sinkAuditIds == null) {
            return;
        }

        // Obtain alarm strategy
        List<AuditAlertRule> alertRules = auditAlertRuleManager.getAuditAlertRuleList();

        // Obtain the range of logs that need to be queried
        String startLogTs = getStartLogTs();
        String endLogTs = getEndLogTs();

        // Query the relevant indicator data of auditId source
        List<AuditMetric> sourceAuditMetric =
                auditMetricService.getStorageAuditMetrics(sourceAuditId, startLogTs, endLogTs);
        if (sourceAuditMetric == null) {
            return;
        }

        // Compare the source auditId related indicator data with the sink auditId related indicator data
        for (String sinkAuditId : sinkAuditIds) {
            List<AuditMetric> sinkAuditMetrics =
                    auditMetricService.getStorageAuditMetrics(sinkAuditId, startLogTs, endLogTs);
            if (sinkAuditMetrics == null || sinkAuditMetrics.isEmpty()) {
                continue;
            }
            for (AuditAlertRule alertRule : alertRules) {
                alertEvaluator.evaluateAndReportAlert(sourceAuditMetric, sinkAuditMetrics,
                        alertRule);
            }
        }
    }

    /**
     * Stop the audit inspection task
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

    private String getStartLogTs() {
        return LocalDateTime.now()
                .withSecond(0)
                .minusMinutes(delayTimeMinute)
                .minusMinutes(intervalTimeMinute)
                .format(LOGS_FMT);
    }
    private String getEndLogTs() {
        return LocalDateTime.now()
                .withSecond(0)
                .minusMinutes(delayTimeMinute)
                .format(LOGS_FMT);
    }

}