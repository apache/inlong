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

import org.apache.inlong.audit.tool.dto.AuditAlertCondition;
import org.apache.inlong.audit.tool.dto.AuditAlertRule;
import org.apache.inlong.audit.tool.entity.AuditMetric;
import org.apache.inlong.audit.tool.manager.AuditAlertRuleManager;
import org.apache.inlong.audit.tool.reporter.PrometheusReporter;

import lombok.Getter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Objects;

public class AlertEvaluator {

    private final PrometheusReporter prometheusReporter;
    private static final Logger LOGGER = LoggerFactory.getLogger(AlertEvaluator.class);
    @Getter
    private final AuditAlertRuleManager auditAlertRuleManager;

    public AlertEvaluator(PrometheusReporter prometheusReporter,
            AuditAlertRuleManager auditAlertRuleManager) {
        this.prometheusReporter = prometheusReporter;
        this.auditAlertRuleManager = auditAlertRuleManager;
    }

    public void evaluateAndReportAlert(List<AuditMetric> sourceMetrics,
            List<AuditMetric> sinkMetrics,
            AuditAlertRule alertRule) {
        if (sourceMetrics == null || sinkMetrics == null) {
            return;
        }

        AuditAlertCondition condition = alertRule.getCondition();
        double threshold = condition.getValue();
        String op = condition.getOperator();

        for (AuditMetric source : sourceMetrics) {
            if (!Objects.equals(source.getInlongGroupId(), alertRule.getInlongGroupId()) ||
                    !Objects.equals(source.getInlongStreamId(), alertRule.getInlongStreamId())) {
                continue;
            }
            for (AuditMetric sink : sinkMetrics) {
                if (!Objects.equals(source.getInlongGroupId(), sink.getInlongGroupId()) ||
                        !Objects.equals(source.getInlongStreamId(), sink.getInlongStreamId())) {
                    continue;
                }

                if (source.getCount() == 0) {
                    continue;
                }

                double diff = (sink.getCount() - source.getCount()) / (double) source.getCount();

                boolean hit;

                switch (op) {
                    case ">":
                        hit = diff > threshold;
                        break;
                    case ">=":
                        hit = diff >= threshold;
                        break;
                    case "<":
                        hit = diff < threshold;
                        break;
                    case "<=":
                        hit = diff <= threshold;
                        break;
                    case "==":
                        hit = diff == threshold;
                        break;
                    case "!=":
                        hit = diff != threshold;
                        break;
                    default:
                        hit = false;
                }

                if (hit) {
                    LOGGER.error(
                            "[ALERT] groupId={}, streamId={} | sourceCount={}, sinkCount={} | diff={} {} threshold={}",
                            source.getInlongGroupId(), source.getInlongStreamId(),
                            source.getCount(), sink.getCount(), diff, op, threshold);
                    if (prometheusReporter.getAuditMetric() != null) {
                        prometheusReporter.getAuditMetric().updateSourceAndSinkAuditDiffMetric(diff);
                    }
                }
            }
        }
    }

}