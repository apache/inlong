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

package org.apache.inlong.manager.client.api.integration;

import org.apache.inlong.manager.client.api.ClientConfiguration;
import org.apache.inlong.manager.client.api.impl.InlongClientImpl;
import org.apache.inlong.manager.client.api.inner.client.AuditClient;
import org.apache.inlong.manager.client.api.inner.client.ClientFactory;
import org.apache.inlong.manager.client.api.util.ClientUtils;
import org.apache.inlong.manager.common.auth.DefaultAuthentication;
import org.apache.inlong.manager.common.util.JsonUtils;
import org.apache.inlong.manager.pojo.audit.AuditAlertRule;
import org.apache.inlong.manager.pojo.common.Response;

import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.client.WireMock;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static com.github.tomakehurst.wiremock.client.WireMock.delete;
import static com.github.tomakehurst.wiremock.client.WireMock.equalToJson;
import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.okJson;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.client.WireMock.put;
import static com.github.tomakehurst.wiremock.client.WireMock.stubFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlMatching;
import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.options;

/**
 * Integration tests for Audit Alert Rule SDK functionality
 * 
 * This test class validates the complete workflow of:
 * 1. Creating an audit alert rule
 * 2. Querying the created rule
 * 3. Listing rules with filters
 * 4. Updating the rule
 * 5. Deleting the rule
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class AuditAlertRuleIntegrationTest {

    private static final int SERVICE_PORT = 8085;
    static ClientFactory clientFactory;
    private static WireMockServer wireMockServer;
    private static AuditClient auditClient;

    // Test data
    private static final String TEST_GROUP_ID = "integration_test_group";
    private static final String TEST_STREAM_ID = "integration_test_stream";
    private static final String TEST_AUDIT_ID = "3";
    private static final String TEST_ALERT_NAME = "Integration Test Alert";
    private static final String TEST_CONDITION = "count < 1000 OR delay > 60000";
    private static final String TEST_LEVEL = "ERROR";
    private static final String TEST_NOTIFY_TYPE = "EMAIL";
    private static final String TEST_RECEIVERS = "integration@test.com";

    private static Integer createdRuleId;

    @BeforeAll
    static void setup() {
        // Start WireMock server
        wireMockServer = new WireMockServer(options().port(SERVICE_PORT));
        wireMockServer.start();
        WireMock.configureFor(wireMockServer.port());

        // Configure client using ClientFactory pattern
        String serviceUrl = "127.0.0.1:" + SERVICE_PORT;
        ClientConfiguration configuration = new ClientConfiguration();
        configuration.setAuthentication(new DefaultAuthentication("admin", "inlong"));
        configuration.setWriteTimeout(10);
        configuration.setReadTimeout(10);
        configuration.setConnectTimeout(10);
        configuration.setTimeUnit(TimeUnit.SECONDS);

        InlongClientImpl inlongClient = new InlongClientImpl(serviceUrl, configuration);
        clientFactory = ClientUtils.getClientFactory(inlongClient.getConfiguration());
        auditClient = clientFactory.getAuditClient();
    }

    @AfterAll
    static void teardown() {
        wireMockServer.stop();
    }

    @Test
    @Order(1)
    void testCreateAlertRuleIntegration() {
        // Prepare test data
        AuditAlertRule inputRule = createTestAlertRule();
        AuditAlertRule expectedRule = createTestAlertRule();
        expectedRule.setId(1);
        createdRuleId = 1;

        String requestBody = JsonUtils.toJsonString(inputRule);
        String responseBody = JsonUtils.toJsonString(Response.success(expectedRule));

        // Mock API response
        stubFor(
                post(urlMatching("/inlong/manager/api/audit/alert/rule.*"))
                        .withRequestBody(equalToJson(requestBody))
                        .willReturn(okJson(responseBody)));

        // Execute test
        AuditAlertRule result = auditClient.createAlertRule(inputRule);

        // Verify result
        Assertions.assertNotNull(result, "Created alert rule should not be null");
        Assertions.assertEquals(1, result.getId(), "Rule ID should be 1");
        Assertions.assertEquals(TEST_GROUP_ID, result.getInlongGroupId(), "Group ID should match");
        Assertions.assertEquals(TEST_STREAM_ID, result.getInlongStreamId(), "Stream ID should match");
        Assertions.assertEquals(TEST_AUDIT_ID, result.getAuditId(), "Audit ID should match");
        Assertions.assertEquals(TEST_ALERT_NAME, result.getAlertName(), "Alert name should match");
        Assertions.assertEquals(TEST_CONDITION, result.getCondition(), "Trigger condition should match");
        Assertions.assertEquals(TEST_LEVEL, result.getLevel(), "Alert level should match");
        Assertions.assertEquals(TEST_NOTIFY_TYPE, result.getNotifyType(), "Notification type should match");
        Assertions.assertEquals(TEST_RECEIVERS, result.getReceivers(), "Receivers should match");
        Assertions.assertTrue(result.getEnabled(), "Rule should be enabled");

        System.out.println("✓ Successfully created alert rule, ID: " + result.getId());
    }

    @Test
    @Order(2)
    void testGetAlertRuleIntegration() {
        // Prepare test data
        AuditAlertRule expectedRule = createTestAlertRule();
        expectedRule.setId(createdRuleId);

        String responseBody = JsonUtils.toJsonString(Response.success(expectedRule));

        // Mock API response
        stubFor(
                get(urlMatching("/inlong/manager/api/audit/alert/rule/" + createdRuleId + ".*"))
                        .willReturn(okJson(responseBody)));

        // Execute test
        AuditAlertRule result = auditClient.getAlertRule(createdRuleId);

        // Verify result
        Assertions.assertNotNull(result, "Queried alert rule should not be null");
        Assertions.assertEquals(createdRuleId, result.getId(), "Rule ID should match");
        Assertions.assertEquals(TEST_GROUP_ID, result.getInlongGroupId(), "Group ID should match");
        Assertions.assertEquals(TEST_ALERT_NAME, result.getAlertName(), "Alert name should match");

        System.out.println("✓ Successfully queried alert rule, name: " + result.getAlertName());
    }

    @Test
    @Order(3)
    void testListEnabledAlertRulesIntegration() {
        // Prepare test data
        AuditAlertRule rule1 = createTestAlertRule();
        rule1.setId(1);
        rule1.setEnabled(true);

        AuditAlertRule rule2 = createTestAlertRule();
        rule2.setId(2);
        rule2.setAlertName("Second Alert Rule");
        rule2.setEnabled(true);

        List<AuditAlertRule> expectedRules = Arrays.asList(rule1, rule2);
        String responseBody = JsonUtils.toJsonString(Response.success(expectedRules));

        // Mock API response
        stubFor(
                get(urlMatching("/inlong/manager/api/audit/alert/rule/enabled.*"))
                        .willReturn(okJson(responseBody)));

        // Execute test
        List<AuditAlertRule> result = auditClient.listEnabledAlertRules();

        // Verify result
        Assertions.assertNotNull(result, "Enabled alert rules list should not be null");
        Assertions.assertEquals(2, result.size(), "Should return 2 enabled rules");
        Assertions.assertTrue(result.get(0).getEnabled(), "First rule should be enabled");
        Assertions.assertTrue(result.get(1).getEnabled(), "Second rule should be enabled");

        System.out.println("✓ Successfully queried enabled alert rules, count: " + result.size());
    }

    @Test
    @Order(4)
    void testListAlertRulesByGroupIntegration() {
        // Prepare test data
        AuditAlertRule rule = createTestAlertRule();
        rule.setId(createdRuleId);
        List<AuditAlertRule> expectedRules = Arrays.asList(rule);

        String responseBody = JsonUtils.toJsonString(Response.success(expectedRules));

        // Mock API response
        stubFor(
                get(urlMatching("/inlong/manager/api/audit/alert/rule/list\\?inlongGroupId=" + TEST_GROUP_ID + ".*"))
                        .willReturn(okJson(responseBody)));

        // Execute test
        List<AuditAlertRule> result = auditClient.listAlertRules(TEST_GROUP_ID, null);

        // Verify result
        Assertions.assertNotNull(result, "Alert rules list by group should not be null");
        Assertions.assertEquals(1, result.size(), "Should return 1 rule");
        Assertions.assertEquals(TEST_GROUP_ID, result.get(0).getInlongGroupId(), "Group ID should match");

        System.out.println("✓ Successfully queried alert rules by group, group ID: " + TEST_GROUP_ID);
    }

    @Test
    @Order(5)
    void testListAlertRulesByGroupAndStreamIntegration() {
        // Prepare test data
        AuditAlertRule rule = createTestAlertRule();
        rule.setId(createdRuleId);
        List<AuditAlertRule> expectedRules = Arrays.asList(rule);

        String responseBody = JsonUtils.toJsonString(Response.success(expectedRules));

        // Mock API response
        stubFor(
                get(urlMatching("/inlong/manager/api/audit/alert/rule/list\\?inlongGroupId=" + TEST_GROUP_ID
                        + "&inlongStreamId="
                        + TEST_STREAM_ID + ".*"))
                                .willReturn(okJson(responseBody)));

        // Execute test
        List<AuditAlertRule> result = auditClient.listAlertRules(TEST_GROUP_ID, TEST_STREAM_ID);

        // Verify result
        Assertions.assertNotNull(result, "Alert rules list by group and stream should not be null");
        Assertions.assertEquals(1, result.size(), "Should return 1 rule");
        Assertions.assertEquals(TEST_GROUP_ID, result.get(0).getInlongGroupId(), "Group ID should match");
        Assertions.assertEquals(TEST_STREAM_ID, result.get(0).getInlongStreamId(), "Stream ID should match");

        System.out.println("✓ Successfully queried alert rules by group and stream, group ID: " + TEST_GROUP_ID
                + ", stream ID: " + TEST_STREAM_ID);
    }

    @Test
    @Order(6)
    void testUpdateAlertRuleIntegration() {
        // Prepare test data - modify condition and level
        AuditAlertRule inputRule = createTestAlertRule();
        inputRule.setId(createdRuleId);
        inputRule.setCondition("count < 500 OR delay > 120000");
        inputRule.setLevel("CRITICAL");
        inputRule.setAlertName("Updated Integration Test Alert");

        String requestBody = JsonUtils.toJsonString(inputRule);
        String responseBody = JsonUtils.toJsonString(Response.success(inputRule));

        // Mock API response
        stubFor(
                put(urlMatching("/inlong/manager/api/audit/alert/rule.*"))
                        .withRequestBody(equalToJson(requestBody))
                        .willReturn(okJson(responseBody)));

        // Execute test
        AuditAlertRule result = auditClient.updateAlertRule(inputRule);

        // Verify result
        Assertions.assertNotNull(result, "Updated alert rule should not be null");
        Assertions.assertEquals(createdRuleId, result.getId(), "Rule ID should match");
        Assertions.assertEquals("count < 500 OR delay > 120000", result.getCondition(),
                "Updated condition should match");
        Assertions.assertEquals("CRITICAL", result.getLevel(), "Updated level should match");
        Assertions.assertEquals("Updated Integration Test Alert", result.getAlertName(), "Updated name should match");

        System.out.println("✓ Successfully updated alert rule, new condition: " + result.getCondition());
    }

    @Test
    @Order(7)
    void testDeleteAlertRuleIntegration() {
        // Prepare test data
        String responseBody = JsonUtils.toJsonString(Response.success(true));

        // Mock API response
        stubFor(
                delete(urlMatching("/inlong/manager/api/audit/alert/rule/" + createdRuleId + ".*"))
                        .willReturn(okJson(responseBody)));

        // Execute test
        Boolean result = auditClient.deleteAlertRule(createdRuleId);

        // Verify result
        Assertions.assertNotNull(result, "Delete result should not be null");
        Assertions.assertTrue(result, "Delete should be successful");

        System.out.println("✓ Successfully deleted alert rule, ID: " + createdRuleId);
    }

    @Test
    @Order(8)
    void testCompleteWorkflowIntegration() {
        // Test complete workflow: create -> query -> update -> delete

        // 1. Create rule
        AuditAlertRule createRule = createTestAlertRule();
        createRule.setAlertName("Workflow Test Alert");

        AuditAlertRule createdRule = createTestAlertRule();
        createdRule.setId(100);
        createdRule.setAlertName("Workflow Test Alert");

        stubFor(
                post(urlMatching("/inlong/manager/api/audit/alert/rule.*"))
                        .willReturn(okJson(JsonUtils.toJsonString(Response.success(createdRule)))));

        AuditAlertRule created = auditClient.createAlertRule(createRule);
        Assertions.assertEquals(100, created.getId());

        // 2. Query rule
        stubFor(
                get(urlMatching("/inlong/manager/api/audit/alert/rule/100.*"))
                        .willReturn(okJson(JsonUtils.toJsonString(Response.success(createdRule)))));

        AuditAlertRule retrieved = auditClient.getAlertRule(100);
        Assertions.assertEquals("Workflow Test Alert", retrieved.getAlertName());

        // 3. Update rule
        createdRule.setLevel("WARN");
        stubFor(
                put(urlMatching("/inlong/manager/api/audit/alert/rule.*"))
                        .willReturn(okJson(JsonUtils.toJsonString(Response.success(createdRule)))));

        AuditAlertRule updated = auditClient.updateAlertRule(createdRule);
        Assertions.assertEquals("WARN", updated.getLevel());

        // 4. Delete rule
        stubFor(
                delete(urlMatching("/inlong/manager/api/audit/alert/rule/100.*"))
                        .willReturn(okJson(JsonUtils.toJsonString(Response.success(true)))));

        Boolean deleted = auditClient.deleteAlertRule(100);
        Assertions.assertTrue(deleted);

        System.out.println("✓ Complete workflow test successful: create -> query -> update -> delete");
    }

    @Test
    @Order(9)
    void testErrorHandlingIntegration() {
        // Test error handling scenarios

        // 1. Test querying non-existent rule
        stubFor(
                get(urlMatching("/inlong/manager/api/audit/alert/rule/999.*"))
                        .willReturn(WireMock.aResponse()
                                .withStatus(404)
                                .withHeader("Content-Type", "application/json")
                                .withBody(JsonUtils.toJsonString(Response.fail("Rule not found")))));

        try {
            auditClient.getAlertRule(999);
            Assertions.fail("Exception should be thrown");
        } catch (Exception e) {
            System.out.println("✓ Correctly handled non-existent rule query error");
        }

        // 2. Test deleting non-existent rule
        stubFor(
                delete(urlMatching("/inlong/manager/api/audit/alert/rule/999.*"))
                        .willReturn(okJson(JsonUtils.toJsonString(Response.success(false)))));

        Boolean deleteResult = auditClient.deleteAlertRule(999);
        Assertions.assertFalse(deleteResult);
        System.out.println("✓ Correctly handled non-existent rule delete");
    }

    @Test
    @Order(10)
    void testConcurrentOperationsIntegration() throws InterruptedException {
        // Test concurrent operations scenario

        // Prepare multiple test rules
        for (int i = 1; i <= 3; i++) {
            AuditAlertRule rule = createTestAlertRule();
            rule.setId(200 + i);
            rule.setAlertName("Concurrent Test Alert " + i);

            stubFor(
                    get(urlMatching("/inlong/manager/api/audit/alert/rule/" + (200 + i) + ".*"))
                            .willReturn(okJson(JsonUtils.toJsonString(Response.success(rule)))));
        }

        // Concurrently query multiple rules
        Thread[] threads = new Thread[3];
        final Boolean[] results = new Boolean[3];

        for (int i = 0; i < 3; i++) {
            final int index = i;
            threads[i] = new Thread(() -> {
                try {
                    AuditAlertRule result = auditClient.getAlertRule(201 + index);
                    results[index] = result != null && result.getAlertName().contains("Concurrent Test Alert");
                } catch (Exception e) {
                    results[index] = false;
                }
            });
            threads[i].start();
        }

        // Wait for all threads to complete
        for (Thread thread : threads) {
            thread.join();
        }

        // Verify all concurrent operations were successful
        for (int i = 0; i < 3; i++) {
            Assertions.assertTrue(results[i], "Concurrent operation " + (i + 1) + " should be successful");
        }

        System.out.println("✓ Concurrent operations test successful");
    }

    /**
     * Create test AuditAlertRule object
     */
    private AuditAlertRule createTestAlertRule() {
        AuditAlertRule rule = new AuditAlertRule();
        rule.setInlongGroupId(TEST_GROUP_ID);
        rule.setInlongStreamId(TEST_STREAM_ID);
        rule.setAuditId(TEST_AUDIT_ID);
        rule.setAlertName(TEST_ALERT_NAME);
        rule.setCondition(TEST_CONDITION);
        rule.setLevel(TEST_LEVEL);
        rule.setNotifyType(TEST_NOTIFY_TYPE);
        rule.setReceivers(TEST_RECEIVERS);
        rule.setEnabled(true);
        return rule;
    }
}