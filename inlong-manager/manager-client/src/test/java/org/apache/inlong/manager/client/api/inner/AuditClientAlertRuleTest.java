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

package org.apache.inlong.manager.client.api.inner;

import org.apache.inlong.manager.client.api.inner.client.AuditClient;
import org.apache.inlong.manager.common.exceptions.BusinessException;
import org.apache.inlong.manager.common.util.JsonUtils;
import org.apache.inlong.manager.pojo.audit.AuditAlertCondition;
import org.apache.inlong.manager.pojo.audit.AuditAlertRule;
import org.apache.inlong.manager.pojo.audit.AuditAlertRuleRequest;
import org.apache.inlong.manager.pojo.common.Response;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static com.github.tomakehurst.wiremock.client.WireMock.delete;
import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.okJson;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.client.WireMock.put;
import static com.github.tomakehurst.wiremock.client.WireMock.stubFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlMatching;

/**
 * Tests for {@link AuditClient} audit alert rule functionality
 */
public class AuditClientAlertRuleTest extends ClientFactoryTest {

    private static final AuditClient AUDIT_CLIENT = clientFactory.getAuditClient();

    @Test
    void testCreateAlertRule() {
        // Prepare test data
        AuditAlertRuleRequest inputRule = createTestAlertRuleRequest();
        Integer expectedId = 1;

        // Mock API response
        stubFor(
                post(urlMatching("/inlong/manager/api/audit/alert/rule.*"))
                        .willReturn(
                                okJson(JsonUtils.toJsonString(
                                        Response.success(expectedId)))));

        // Execute test
        Integer result = AUDIT_CLIENT.create(inputRule);

        // Verify result
        Assertions.assertNotNull(result);
        Assertions.assertEquals(1, result.intValue());
    }

    @Test
    void testCreateAlertRuleWithNullInput() {
        // Test null input parameter validation
        Assertions.assertThrows(IllegalArgumentException.class, () -> {
            AUDIT_CLIENT.create((AuditAlertRuleRequest) null);
        });
    }

    @Test
    void testCreateAlertRuleWithEmptyGroupId() {
        // Test empty GroupId parameter validation
        AuditAlertRuleRequest rule = createTestAlertRuleRequest();
        rule.setInlongGroupId("");

        Assertions.assertThrows(BusinessException.class, () -> {
            AUDIT_CLIENT.create(rule);
        });
    }

    @Test
    void testCreateAlertRuleWithEmptyAuditId() {
        // Test empty AuditId parameter validation
        AuditAlertRuleRequest rule = createTestAlertRuleRequest();
        rule.setAuditId("");

        Assertions.assertThrows(BusinessException.class, () -> {
            AUDIT_CLIENT.create(rule);
        });
    }

    @Test
    void testCreateAlertRuleWithEmptyAlertName() {
        // Test empty AlertName parameter validation
        AuditAlertRuleRequest rule = createTestAlertRuleRequest();
        rule.setAlertName("");

        Assertions.assertThrows(BusinessException.class, () -> {
            AUDIT_CLIENT.create(rule);
        });
    }

    @Test
    void testCreateAlertRuleWithEmptyCondition() {
        // Test empty Condition parameter validation
        AuditAlertRuleRequest rule = createTestAlertRuleRequest();
        rule.setCondition(null);

        Assertions.assertThrows(BusinessException.class, () -> {
            AUDIT_CLIENT.create(rule);
        });
    }

    @Test
    void testGetAlertRule() {
        // Prepare test data
        AuditAlertRule expectedRule = createTestAlertRule();
        expectedRule.setId(1);

        // Mock API response
        stubFor(
                get(urlMatching("/inlong/manager/api/audit/alert/rule/1.*"))
                        .willReturn(
                                okJson(JsonUtils.toJsonString(
                                        Response.success(expectedRule)))));

        // Execute test
        AuditAlertRule result = AUDIT_CLIENT.get(1);

        // Verify result
        Assertions.assertNotNull(result);
        Assertions.assertEquals(1, result.getId());
        Assertions.assertEquals("test_group_001", result.getInlongGroupId());
    }

    @Test
    void testGetAlertRuleWithNullId() {
        // Test null ID parameter validation
        Assertions.assertThrows(IllegalArgumentException.class, () -> {
            AUDIT_CLIENT.get(null);
        });
    }

    @Test
    void testListEnabledAlertRules() {
        // Prepare test data
        AuditAlertRule rule1 = createTestAlertRule();
        rule1.setId(1);
        rule1.setEnabled(true);

        AuditAlertRule rule2 = createTestAlertRule();
        rule2.setId(2);
        rule2.setAlertName("High Delay Alert");
        rule2.setEnabled(true);
        List<AuditAlertRule> expectedRules = Arrays.asList(rule1, rule2);

        // Mock API response
        stubFor(
                post(urlMatching("/inlong/manager/api/audit/alert/rule/list.*"))
                        .willReturn(
                                okJson(JsonUtils.toJsonString(
                                        Response.success(expectedRules)))));

        // Execute test - 使用selectByCondition替代listEnabled
        AuditAlertRuleRequest request = new AuditAlertRuleRequest();
        request.setEnabled(true);
        List<AuditAlertRule> result = AUDIT_CLIENT.selectByCondition(request);

        // Verify result
        Assertions.assertNotNull(result);
        Assertions.assertEquals(2, result.size());
        Assertions.assertTrue(result.get(0).getEnabled());
        Assertions.assertTrue(result.get(1).getEnabled());
    }

    @Test
    void testListAlertRulesByGroup() {
        // Prepare test data
        AuditAlertRule rule = createTestAlertRule();
        rule.setId(1);
        List<AuditAlertRule> expectedRules = Arrays.asList(rule);

        // Mock API response
        stubFor(
                post(urlMatching("/inlong/manager/api/audit/alert/rule/list.*"))
                        .willReturn(
                                okJson(JsonUtils.toJsonString(
                                        Response.success(expectedRules)))));

        // Execute test - 使用selectByCondition替代listRules
        AuditAlertRuleRequest request = new AuditAlertRuleRequest();
        request.setInlongGroupId("test_group_001");
        List<AuditAlertRule> result = AUDIT_CLIENT.selectByCondition(request);

        // Verify result
        Assertions.assertNotNull(result);
        Assertions.assertEquals(1, result.size());
        Assertions.assertEquals("test_group_001", result.get(0).getInlongGroupId());
    }

    @Test
    void testListAlertRulesByGroupAndStream() {
        // Prepare test data
        AuditAlertRule rule = createTestAlertRule();
        rule.setId(1);
        List<AuditAlertRule> expectedRules = Arrays.asList(rule);

        // Mock API response
        stubFor(
                post(urlMatching("/inlong/manager/api/audit/alert/rule/list.*"))
                        .willReturn(
                                okJson(JsonUtils.toJsonString(
                                        Response.success(expectedRules)))));

        // Execute test - 使用selectByCondition替代listRules
        AuditAlertRuleRequest request = new AuditAlertRuleRequest();
        request.setInlongGroupId("test_group_001");
        request.setInlongStreamId("test_stream_001");
        List<AuditAlertRule> result = AUDIT_CLIENT.selectByCondition(request);

        // Verify result
        Assertions.assertNotNull(result);
        Assertions.assertEquals(1, result.size());
        Assertions.assertEquals("test_group_001", result.get(0).getInlongGroupId());
        Assertions.assertEquals("test_stream_001", result.get(0).getInlongStreamId());
    }

    @Test
    void testUpdateAlertRule() {
        // Prepare test data
        AuditAlertRule inputRule = createTestAlertRule();
        inputRule.setId(1);
        AuditAlertCondition condition = new AuditAlertCondition();
        condition.setType("count");
        condition.setOperator("<");
        condition.setValue(500);
        inputRule.setCondition(condition);
        inputRule.setLevel("CRITICAL");

        // Mock API response
        stubFor(
                put(urlMatching("/inlong/manager/api/audit/alert/rule.*"))
                        .willReturn(
                                okJson(JsonUtils.toJsonString(
                                        Response.success(inputRule)))));

        // Execute test
        AuditAlertRule result = AUDIT_CLIENT.update(inputRule);

        // Verify result
        Assertions.assertNotNull(result);
        Assertions.assertEquals(1, result.getId());
        Assertions.assertEquals(condition, result.getCondition());
        Assertions.assertEquals("CRITICAL", result.getLevel());
    }

    @Test
    void testUpdateAlertRuleWithNullInput() {
        // Test null input parameter validation
        Assertions.assertThrows(IllegalArgumentException.class, () -> {
            AUDIT_CLIENT.update(null);
        });
    }

    @Test
    void testUpdateAlertRuleWithNullId() {
        // Test null ID parameter validation
        AuditAlertRule rule = createTestAlertRule();
        rule.setId(null);

        Assertions.assertThrows(IllegalArgumentException.class, () -> {
            AUDIT_CLIENT.update(rule);
        });
    }

    @Test
    void testDeleteAlertRule() {
        // Mock API response
        stubFor(
                delete(urlMatching("/inlong/manager/api/audit/delete/1.*"))
                        .willReturn(
                                okJson(JsonUtils.toJsonString(
                                        Response.success(true)))));

        // Execute test
        Boolean result = AUDIT_CLIENT.delete(1);

        // Verify result
        Assertions.assertNotNull(result);
        Assertions.assertTrue(result);
    }

    @Test
    void testDeleteAlertRuleWithNullId() {
        // Test null ID parameter validation
        Assertions.assertThrows(IllegalArgumentException.class, () -> {
            AUDIT_CLIENT.delete(null);
        });
    }

    @Test
    void testDeleteAlertRuleNotFound() {
        // Mock API response - delete non-existent rule
        stubFor(
                delete(urlMatching("/inlong/manager/api/audit/delete/999.*"))
                        .willReturn(
                                okJson(JsonUtils.toJsonString(
                                        Response.success(false)))));

        // Execute test
        Boolean result = AUDIT_CLIENT.delete(999);

        // Verify result
        Assertions.assertNotNull(result);
        Assertions.assertFalse(result);
    }

    /**
     * Create test AuditAlertRule object
     */
    private AuditAlertRule createTestAlertRule() {
        AuditAlertRule rule = new AuditAlertRule();
        rule.setInlongGroupId("test_group_001");
        rule.setInlongStreamId("test_stream_001");
        rule.setAuditId("3");
        rule.setAlertName("Data Loss Alert");
        AuditAlertCondition condition = new AuditAlertCondition();
        condition.setType("data_loss");
        condition.setOperator("<");
        condition.setValue(1000);
        rule.setCondition(condition);
        rule.setLevel("ERROR");
        rule.setNotifyType("EMAIL");
        rule.setReceivers("admin@example.com,monitor@example.com");
        rule.setEnabled(true);
        return rule;
    }

    /**
     * Create test AuditAlertRuleRequest object
     */
    private AuditAlertRuleRequest createTestAlertRuleRequest() {
        AuditAlertRuleRequest request = new AuditAlertRuleRequest();
        request.setInlongGroupId("test_group_001");
        request.setInlongStreamId("test_stream_001");
        request.setAuditId("3");
        request.setAlertName("Data Loss Alert");
        AuditAlertCondition condition = new AuditAlertCondition();
        condition.setType("data_loss");
        condition.setOperator("<");
        condition.setValue(1000);
        request.setCondition(condition);
        request.setLevel("ERROR");
        request.setNotifyType("EMAIL");
        request.setReceivers("admin@example.com,monitor@example.com");
        request.setEnabled(true);
        return request;
    }
}