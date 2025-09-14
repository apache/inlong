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

package org.apache.inlong.tool.evaluator;

import org.apache.inlong.audit.tool.dto.AuditAlertCondition;
import org.apache.inlong.audit.tool.dto.AuditAlertRule;
import org.apache.inlong.audit.tool.entity.AuditMetric;
import org.apache.inlong.audit.tool.evaluator.AlertEvaluator;
import org.apache.inlong.audit.tool.manager.AuditAlertRuleManager;
import org.apache.inlong.audit.tool.reporter.PrometheusReporter;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Collections;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class AlertEvaluatorTest {

    @Mock
    private PrometheusReporter prometheusReporter;

    @Mock
    private AuditAlertRuleManager auditAlertRuleManager;

    private AlertEvaluator alertEvaluator;

    @BeforeEach
    void setUp() {
        alertEvaluator = new AlertEvaluator(prometheusReporter, auditAlertRuleManager);
    }

    @Test
    void testEvaluateAndReportAlertWithNullMetrics() {
        // Test with null source metrics
        alertEvaluator.evaluateAndReportAlert(null, Collections.singletonList(new AuditMetric()), new AuditAlertRule());

        // Test with null sink metrics
        alertEvaluator.evaluateAndReportAlert(Collections.singletonList(new AuditMetric()), null, new AuditAlertRule());

        // Verify no interaction with prometheus reporter
        verifyNoInteractions(prometheusReporter);
    }

    @Test
    void testEvaluateAndReportAlertWithNonMatchingGroupAndStream() {
        // Setup
        AuditMetric sourceMetric = createAuditMetric("group1", 100L);
        AuditMetric sinkMetric = createAuditMetric("group2", 90L); // Different group

        AuditAlertRule alertRule = createAlertRule(">", 0.1);

        // Execute
        alertEvaluator.evaluateAndReportAlert(
                Collections.singletonList(sourceMetric),
                Collections.singletonList(sinkMetric),
                alertRule);

        // Verify no alert was triggered
        verifyNoInteractions(prometheusReporter);
    }

    @Test
    void testEvaluateAndReportAlertWithZeroSourceCount() {
        // Setup
        AuditMetric sourceMetric = createAuditMetric("group1", 0L); // Zero count
        AuditMetric sinkMetric = createAuditMetric("group1", 90L);

        AuditAlertRule alertRule = createAlertRule(">", 0.1);

        // Execute
        alertEvaluator.evaluateAndReportAlert(
                Collections.singletonList(sourceMetric),
                Collections.singletonList(sinkMetric),
                alertRule);

        // Verify no alert was triggered
        verifyNoInteractions(prometheusReporter);
    }

    @Test
    void testEvaluateAndReportAlertWithGreaterThanCondition() {
        AuditMetric sourceMetric = createAuditMetric("group1", 100L);
        AuditMetric sinkMetric = createAuditMetric("group1", 90L);

        AuditAlertRule alertRule = createAlertRule(">", 0.1);

        // Execute
        alertEvaluator.evaluateAndReportAlert(
                Collections.singletonList(sourceMetric),
                Collections.singletonList(sinkMetric),
                alertRule);

        // Verify no alert was triggered
        verifyNoInteractions(prometheusReporter);

        // Setup - diff = (120-100)/100 = 0.2 which is > 0.1
        AuditMetric sinkMetricWithAlert = createAuditMetric("group1", 120L);

        // Execute
        alertEvaluator.evaluateAndReportAlert(
                Collections.singletonList(sourceMetric),
                Collections.singletonList(sinkMetricWithAlert),
                alertRule);

        // Verify alert was triggered
        verify(prometheusReporter, times(1)).getAuditMetric();
    }

    @Test
    void testEvaluateAndReportAlertWithGreaterThanOrEqualCondition() {
        // Setup
        AuditMetric sourceMetric = createAuditMetric("group1", 100L);
        AuditMetric sinkMetric = createAuditMetric("group1", 110L); // diff = 0.1

        AuditAlertRule alertRule = createAlertRule(">=", 0.1);

        // Execute
        alertEvaluator.evaluateAndReportAlert(
                Collections.singletonList(sourceMetric),
                Collections.singletonList(sinkMetric),
                alertRule);

        // Verify alert was triggered
        verify(prometheusReporter, times(1)).getAuditMetric();
    }

    @Test
    void testEvaluateAndReportAlertWithLessThanCondition() {
        // Setup
        AuditMetric sourceMetric = createAuditMetric("group1", 100L);
        AuditMetric sinkMetric = createAuditMetric("group1", 90L); // diff = -0.1

        AuditAlertRule alertRule = createAlertRule("<", -0.05); // -0.1 < -0.05

        // Execute
        alertEvaluator.evaluateAndReportAlert(
                Collections.singletonList(sourceMetric),
                Collections.singletonList(sinkMetric),
                alertRule);

        // Verify alert was triggered
        verify(prometheusReporter, times(1)).getAuditMetric();
    }

    @Test
    void testEvaluateAndReportAlertWithLessThanOrEqualCondition() {
        // Setup
        AuditMetric sourceMetric = createAuditMetric("group1", 100L);
        AuditMetric sinkMetric = createAuditMetric("group1", 90L); // diff = -0.1

        AuditAlertRule alertRule = createAlertRule("<=", -0.1); // -0.1 <= -0.1

        // Execute
        alertEvaluator.evaluateAndReportAlert(
                Collections.singletonList(sourceMetric),
                Collections.singletonList(sinkMetric),
                alertRule);

        // Verify alert was triggered
        verify(prometheusReporter, times(1)).getAuditMetric();
    }

    @Test
    void testEvaluateAndReportAlertWithEqualCondition() {
        // Setup
        AuditMetric sourceMetric = createAuditMetric("group1", 100L);
        AuditMetric sinkMetric = createAuditMetric("group1", 110L); // diff = 0.1

        AuditAlertRule alertRule = createAlertRule("==", 0.1);

        // Execute
        alertEvaluator.evaluateAndReportAlert(
                Collections.singletonList(sourceMetric),
                Collections.singletonList(sinkMetric),
                alertRule);

        // Verify alert was triggered
        verify(prometheusReporter, times(1)).getAuditMetric();
    }

    @Test
    void testEvaluateAndReportAlertWithNotEqualCondition() {
        // Setup
        AuditMetric sourceMetric = createAuditMetric("group1", 100L);
        AuditMetric sinkMetric = createAuditMetric("group1", 110L); // diff = 0.1

        AuditAlertRule alertRule = createAlertRule("!=", 0.2); // 0.1 != 0.2

        // Execute
        alertEvaluator.evaluateAndReportAlert(
                Collections.singletonList(sourceMetric),
                Collections.singletonList(sinkMetric),
                alertRule);

        // Verify alert was triggered
        verify(prometheusReporter, times(1)).getAuditMetric();
    }

    @Test
    void testEvaluateAndReportAlertWithUnknownOperator() {
        // Setup
        AuditMetric sourceMetric = createAuditMetric("group1", 100L);
        AuditMetric sinkMetric = createAuditMetric("group1", 110L);

        AuditAlertRule alertRule = createAlertRule("unknown", 0.1); // Unknown operator

        // Execute
        alertEvaluator.evaluateAndReportAlert(
                Collections.singletonList(sourceMetric),
                Collections.singletonList(sinkMetric),
                alertRule);

        // Verify no alert was triggered for unknown operator
        verifyNoInteractions(prometheusReporter);
    }

    @Test
    void testGetAuditAlertRuleManager() {
        // Test the getter method
        assertEquals(auditAlertRuleManager, alertEvaluator.getAuditAlertRuleManager());
    }

    // Helper methods to reduce duplication
    private AuditMetric createAuditMetric(String groupId, long count) {
        AuditMetric metric = new AuditMetric();
        metric.setInlongGroupId(groupId);
        metric.setInlongStreamId("stream1");
        metric.setCount(count);
        return metric;
    }

    private AuditAlertRule createAlertRule(String operator, double value) {
        AuditAlertRule rule = new AuditAlertRule();
        rule.setInlongGroupId("group1");
        rule.setInlongStreamId("stream1");

        AuditAlertCondition condition = new AuditAlertCondition();
        condition.setOperator(operator);
        condition.setValue(value);
        rule.setCondition(condition);

        return rule;
    }
}
