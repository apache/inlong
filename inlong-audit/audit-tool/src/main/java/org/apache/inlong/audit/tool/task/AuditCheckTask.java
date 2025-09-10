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

import org.apache.inlong.audit.tool.DTO.AlertPolicy;
import org.apache.inlong.audit.tool.DTO.AuditData;
import org.apache.inlong.audit.tool.evaluator.AlertEvaluator;
import org.apache.inlong.audit.tool.manager.ManagerClient;
import org.apache.inlong.audit.tool.reporter.OpenTelemetryReporter;
import org.apache.inlong.audit.tool.reporter.PrometheusReporter;

import lombok.Getter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
    @Getter
    private PrometheusReporter prometheusReporter;
    @Getter
    private OpenTelemetryReporter openTelemetryReporter;
    private final ManagerClient managerClient;
    private static final Logger LOGGER = LoggerFactory.getLogger(ManagerClient.class);

    public AuditCheckTask(PrometheusReporter prometheusReporter, OpenTelemetryReporter openTelemetryReporter,
            ManagerClient managerClient, AlertEvaluator alertEvaluator) {
        this.prometheusReporter = prometheusReporter;
        this.openTelemetryReporter = openTelemetryReporter;
        this.managerClient = managerClient;
        this.alertEvaluator = alertEvaluator;
    }

    /**
     * Initiate the audit inspection task
     */
    public void start() {
        scheduler.scheduleAtFixedRate(this::checkAuditData, 0, 30, TimeUnit.SECONDS);
    }

    /**
     * Check audit data and trigger alert evaluation.
     */
    private void checkAuditData() {
        final long startTime = System.currentTimeMillis();
        final long timeoutMillis = 10 * 60 * 1000; // 10 minutes timeout
        int attempt = 0;
        boolean success = false;

        while (!success && (System.currentTimeMillis() - startTime) < timeoutMillis) {
            attempt++;
            try {
                LOGGER.info("Attempt #{} to check audit data", attempt);

                // Get audit data
                List<AuditData> auditDataList = managerClient.fetchAuditData();

                // Get alert policies
                List<AlertPolicy> policies = managerClient.fetchAlertPolicies();

                // Evaluate each audit data against each policy
                for (AuditData auditData : auditDataList) {
                    for (AlertPolicy policy : policies) {
                        if (alertEvaluator.shouldTriggerAlert(auditData, policy)) {
                            alertEvaluator.triggerAlert(auditData, policy);
                        }
                    }
                }

                // Successfully completed, exit loop
                success = true;
                LOGGER.info("Successfully checked audit data on attempt #{}", attempt);

            } catch (Exception e) {
                // Log error but continue retrying
                LOGGER.error("Error occurred on attempt #{}: {}", attempt, e.getMessage());
                LOGGER.debug("Error details", e);

                // Check if timeout reached
                if ((System.currentTimeMillis() - startTime) >= timeoutMillis) {
                    LOGGER.error("Timeout reached after {} minutes. Terminating thread.", 10);
                    break;
                }

                // Wait 3 seconds before retrying
                try {
                    Thread.sleep(3000);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    LOGGER.warn("Thread interrupted during retry wait");
                    break;
                }
            }
        }

        if (!success) {
            LOGGER.error("Failed to check audit data after {} attempts and {} minutes. Terminating thread.",
                    attempt, 10);
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

}