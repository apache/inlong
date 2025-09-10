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

package org.apache.inlong.audit.tool.evaluator;

import org.apache.inlong.audit.tool.DTO.AlertPolicy;
import org.apache.inlong.audit.tool.DTO.AuditData;
import org.apache.inlong.audit.tool.DTO.MetricData;
import org.apache.inlong.audit.tool.manager.ManagerClient;
import org.apache.inlong.audit.tool.reporter.OpenTelemetryReporter;
import org.apache.inlong.audit.tool.reporter.PrometheusReporter;

import lombok.Getter;

import java.util.ArrayList;
import java.util.List;

public class AlertEvaluator {

    private final PrometheusReporter prometheusReporter;
    private final OpenTelemetryReporter openTelemetryReporter;
    @Getter
    private final ManagerClient managerClient;
    @Getter
    private AuditData auditData;
    @Getter
    private AlertPolicy alertpolicy;

    public AlertEvaluator(PrometheusReporter prometheusReporter, OpenTelemetryReporter openTelemetryReporter,
            ManagerClient managerClient) {
        this.prometheusReporter = prometheusReporter;
        this.openTelemetryReporter = openTelemetryReporter;
        this.managerClient = managerClient;
    }

    private MetricData calculateMetricData(AuditData auditData) {
        return new MetricData(auditData.getGroupId(), auditData.getStreamId(), auditData.getDataLossRate(),
                auditData.getDataLossCount(), auditData.getAuditCount(), auditData.getExpectedCount(),
                auditData.getReceivedCount());
    }

    public List<String> getEnabledPlatforms(AlertPolicy alertPolicy) {
        List<String> enabledPlatforms = new ArrayList<>();
        List<String> targets = alertPolicy.getTargets();
        if (targets != null) {
            for (String target : targets) {
                switch (target.toLowerCase()) {
                    case "prometheus":
                        enabledPlatforms.add("prometheus");
                        break;
                    case "opentelemetry":
                        enabledPlatforms.add("opentelemetry");
                        break;
                    default:
                        System.out.println("Invalid platform");
                        break;
                }
            }
        }
        return enabledPlatforms;
    }

    public boolean shouldTriggerAlert(AuditData auditData, AlertPolicy alertPolicy) {
        this.auditData = auditData;
        this.alertpolicy = alertPolicy;

        double dataLossRate = auditData.getDataLossRate();

        double threshold = alertPolicy.getThreshold();
        String comparisonOperator = alertPolicy.getComparisonOperator();

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
        List<String> enabledPlatforms = getEnabledPlatforms(policy);
        MetricData metricData = calculateMetricData(auditData);
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
                    System.out.println("Invalid platform: " + platform);
                    break;
            }
        }
    }

}