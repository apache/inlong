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
import org.apache.inlong.manager.pojo.audit.AuditAlertCondition;
import org.apache.inlong.manager.pojo.audit.AuditAlertRule;
import org.apache.inlong.manager.pojo.audit.AuditAlertRuleRequest;
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
    private static final AuditAlertCondition TEST_CONDITION = createTestCondition();
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
        AuditAlertRuleRequest inputRule = createTestAlertRuleRequest();
        Integer expectedId = 1;
        createdRuleId = 1;

        String requestBody = JsonUtils.toJsonString(inputRule);
        String responseBody = JsonUtils.toJsonString(Response.success(expectedId));

        // Mock API response
        stubFor(
                post(urlMatching("/inlong/manager/api/audit/alert/rule.*"))
                        .withRequestBody(equalToJson(requestBody))
                        .willReturn(okJson(responseBody)));

        // Execute test - Fix: use auditClient.create() instead of auditClient.createAlertRule()
        Integer result = auditClient.create(inputRule);

        // Verify result
        Assertions.assertNotNull(result, "Created alert rule ID should not be null");
        Assertions.assertEquals(1, result.intValue(), "Rule ID should be 1");

        System.out.println("✓ Successfully created alert rule, ID: " + result);
    }

    @Test
    @Order(2)
    void testGetAlertRuleIntegration() {
        // Prepare test data
        AuditAlertRule expectedRule = createTestAlertRule();
        expectedRule.setId(createdRuleId);
        expectedRule.setVersion(1); // Set version to 1
        expectedRule.setIsDeleted(0); // Set isDeleted to 0

        String responseBody = JsonUtils.toJsonString(Response.success(expectedRule));

        // Mock API response
        stubFor(
                get(urlMatching("/inlong/manager/api/audit/alert/rule/" + createdRuleId + ".*"))
                        .willReturn(okJson(responseBody)));

        // Execute test
        AuditAlertRule result = auditClient.get(createdRuleId);

        // Verify result
        Assertions.assertNotNull(result, "Queried alert rule should not be null");
        Assertions.assertEquals(createdRuleId, result.getId(), "Rule ID should match");
        Assertions.assertEquals(TEST_GROUP_ID, result.getInlongGroupId(), "Group ID should match");
        Assertions.assertEquals(TEST_ALERT_NAME, result.getAlertName(), "Alert name should match");
        Assertions.assertEquals(0, result.getIsDeleted().intValue(), "Rule should not be deleted");

        System.out.println("✓ Successfully queried alert rule, name: " + result.getAlertName());
    }

    @Test
    @Order(3)
    void testListEnabledAlertRulesIntegration() {
        // Prepare test data
        AuditAlertRule rule1 = createTestAlertRule();
        rule1.setId(1);
        rule1.setEnabled(true);
        rule1.setVersion(1); // Set version to 1
        rule1.setIsDeleted(0); // Set isDeleted to 0

        AuditAlertRule rule2 = createTestAlertRule();
        rule2.setId(2);
        rule2.setAlertName("Second Alert Rule");
        rule2.setEnabled(true);
        rule2.setVersion(1); // Set version to 1
        rule2.setIsDeleted(0); // Set isDeleted to 0

        List<AuditAlertRule> expectedRules = Arrays.asList(rule1, rule2);
        String responseBody = JsonUtils.toJsonString(Response.success(expectedRules));

        // Mock API response
        stubFor(
                get(urlMatching("/inlong/manager/api/audit/alert/rule/enabled.*"))
                        .willReturn(okJson(responseBody)));

        // Execute test
        List<AuditAlertRule> result = auditClient.listEnabled();

        // Verify result
        Assertions.assertNotNull(result, "Enabled alert rules list should not be null");
        Assertions.assertEquals(2, result.size(), "Should return 2 enabled rules");
        Assertions.assertTrue(result.get(0).getEnabled(), "First rule should be enabled");
        Assertions.assertTrue(result.get(1).getEnabled(), "Second rule should be enabled");
        // Verify isDeleted for both rules
        Assertions.assertEquals(0, result.get(0).getIsDeleted().intValue(), "First rule should not be deleted");
        Assertions.assertEquals(0, result.get(1).getIsDeleted().intValue(), "Second rule should not be deleted");

        System.out.println("✓ Successfully queried enabled alert rules, count: " + result.size());
    }

    @Test
    @Order(4)
    void testListAlertRulesByGroupIntegration() {
        // Prepare test data
        AuditAlertRule rule = createTestAlertRule();
        rule.setId(createdRuleId);
        rule.setVersion(1); // Set version to 1
        rule.setIsDeleted(0); // Set isDeleted to 0
        List<AuditAlertRule> expectedRules = Arrays.asList(rule);

        String responseBody = JsonUtils.toJsonString(Response.success(expectedRules));

        // Mock API response
        stubFor(
                get(urlMatching("/inlong/manager/api/audit/alert/rule/list\\?inlongGroupId=" + TEST_GROUP_ID + ".*"))
                        .willReturn(okJson(responseBody)));

        // Execute test
        List<AuditAlertRule> result = auditClient.listRules(TEST_GROUP_ID, null);

        // Verify result
        Assertions.assertNotNull(result, "Alert rules list by group should not be null");
        Assertions.assertEquals(1, result.size(), "Should return 1 rule");
        Assertions.assertEquals(TEST_GROUP_ID, result.get(0).getInlongGroupId(), "Group ID should match");
        Assertions.assertEquals(0, result.get(0).getIsDeleted().intValue(), "Rule should not be deleted");

        System.out.println("✓ Successfully queried alert rules by group, group ID: " + TEST_GROUP_ID);
    }

    @Test
    @Order(5)
    void testListAlertRulesByGroupAndStreamIntegration() {
        // Prepare test data
        AuditAlertRule rule = createTestAlertRule();
        rule.setId(createdRuleId);
        rule.setVersion(1); // Set version to 1
        rule.setIsDeleted(0); // Set isDeleted to 0
        List<AuditAlertRule> expectedRules = Arrays.asList(rule);

        String responseBody = JsonUtils.toJsonString(Response.success(expectedRules));

        // Mock API response
        stubFor(
                get(urlMatching("/inlong/manager/api/audit/alert/rule/list\\?inlongGroupId=" + TEST_GROUP_ID
                        + "&inlongStreamId="
                        + TEST_STREAM_ID + ".*"))
                                .willReturn(okJson(responseBody)));

        // Execute test
        List<AuditAlertRule> result = auditClient.listRules(TEST_GROUP_ID, TEST_STREAM_ID);

        // Verify result
        Assertions.assertNotNull(result, "Alert rules list by group and stream should not be null");
        Assertions.assertEquals(1, result.size(), "Should return 1 rule");
        Assertions.assertEquals(TEST_GROUP_ID, result.get(0).getInlongGroupId(), "Group ID should match");
        Assertions.assertEquals(TEST_STREAM_ID, result.get(0).getInlongStreamId(), "Stream ID should match");
        Assertions.assertEquals(0, result.get(0).getIsDeleted().intValue(), "Rule should not be deleted");

        System.out.println("✓ Successfully queried alert rules by group and stream, group ID: " + TEST_GROUP_ID
                + ", stream ID: " + TEST_STREAM_ID);
    }

    @Test
    @Order(6)
    void testUpdateAlertRuleIntegration() {
        // Prepare test data - modify condition and level
        AuditAlertRule inputRule = createTestAlertRule();
        inputRule.setId(createdRuleId);
        // Update condition
        AuditAlertCondition updatedCondition = new AuditAlertCondition();
        updatedCondition.setType("count");
        updatedCondition.setOperator("<");
        updatedCondition.setValue(500);
        inputRule.setCondition(updatedCondition);
        inputRule.setLevel("CRITICAL");
        inputRule.setAlertName("Updated Integration Test Alert");
        inputRule.setVersion(2); // Set version for update
        inputRule.setIsDeleted(0); // Set isDeleted to 0

        String requestBody = JsonUtils.toJsonString(inputRule);
        String responseBody = JsonUtils.toJsonString(Response.success(inputRule));

        // Mock API response
        stubFor(
                put(urlMatching("/inlong/manager/api/audit/alert/rule.*"))
                        .withRequestBody(equalToJson(requestBody))
                        .willReturn(okJson(responseBody)));

        // Execute test
        AuditAlertRule result = auditClient.update(inputRule);

        // Verify result
        Assertions.assertNotNull(result, "Updated alert rule should not be null");
        Assertions.assertEquals(createdRuleId, result.getId(), "Rule ID should match");
        // Update assertion to check Condition object properties
        Assertions.assertEquals("count", result.getCondition().getType(), "Updated condition type should match");
        Assertions.assertEquals("<", result.getCondition().getOperator(), "Updated condition operator should match");
        Assertions.assertEquals(500, result.getCondition().getValue(), "Updated condition value should match");
        Assertions.assertEquals("CRITICAL", result.getLevel(), "Updated level should match");
        Assertions.assertEquals("Updated Integration Test Alert", result.getAlertName(), "Updated name should match");
        Assertions.assertEquals(2, result.getVersion().intValue(), "Version should be updated");

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
        Boolean result = auditClient.delete(createdRuleId);

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
        AuditAlertRuleRequest createRuleRequest = createTestAlertRuleRequest();
        createRuleRequest.setAlertName("Workflow Test Alert");

        AuditAlertRule createdRule = createTestAlertRule();
        createdRule.setId(100);
        createdRule.setAlertName("Workflow Test Alert");
        createdRule.setVersion(1); // Set version to 1
        createdRule.setIsDeleted(0); // Set isDeleted to 0

        stubFor(
                post(urlMatching("/inlong/manager/api/audit/alert/rule.*"))
                        .willReturn(okJson(JsonUtils.toJsonString(Response.success(100)))));

        // Fix: use auditClient.create() instead of auditClient.createAlertRule()
        Integer createdId = auditClient.create(createRuleRequest);
        Assertions.assertEquals(100, createdId.intValue());

        // 2. Query rule
        stubFor(
                get(urlMatching("/inlong/manager/api/audit/alert/rule/100.*"))
                        .willReturn(okJson(JsonUtils.toJsonString(Response.success(createdRule)))));

        AuditAlertRule queriedRule = auditClient.get(100);
        Assertions.assertNotNull(queriedRule);
        Assertions.assertEquals("Workflow Test Alert", queriedRule.getAlertName());
        Assertions.assertEquals(0, queriedRule.getIsDeleted().intValue(), "Rule should not be deleted");

        // 3. Update rule
        AuditAlertRule updateRule = createTestAlertRule();
        updateRule.setId(100);
        updateRule.setAlertName("Updated Workflow Test Alert");
        updateRule.setLevel("CRITICAL");
        updateRule.setVersion(2); // Set version for update
        updateRule.setIsDeleted(0); // Set isDeleted to 0

        stubFor(
                put(urlMatching("/inlong/manager/api/audit/alert/rule.*"))
                        .willReturn(okJson(JsonUtils.toJsonString(Response.success(updateRule)))));

        AuditAlertRule updatedRule = auditClient.update(updateRule);
        Assertions.assertNotNull(updatedRule);
        Assertions.assertEquals("Updated Workflow Test Alert", updatedRule.getAlertName());
        Assertions.assertEquals("CRITICAL", updatedRule.getLevel());
        Assertions.assertEquals(2, updatedRule.getVersion().intValue(), "Version should be updated");

        // 4. Delete rule
        stubFor(
                delete(urlMatching("/inlong/manager/api/audit/alert/rule/100.*"))
                        .willReturn(okJson(JsonUtils.toJsonString(Response.success(true)))));

        Boolean deleteResult = auditClient.delete(100);
        Assertions.assertTrue(deleteResult);

        System.out.println("✓ Complete workflow test successful");
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
            auditClient.get(999);
            Assertions.fail("Exception should be thrown");
        } catch (Exception e) {
            System.out.println("✓ Correctly handled non-existent rule query error");
        }

        // 2. Test deleting non-existent rule
        stubFor(
                delete(urlMatching("/inlong/manager/api/audit/alert/rule/999.*"))
                        .willReturn(okJson(JsonUtils.toJsonString(Response.success(false)))));

        Boolean deleteResult = auditClient.delete(999);
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
            rule.setVersion(1); // Set version to 1
            rule.setIsDeleted(0); // Set isDeleted to 0

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
                    AuditAlertRule result = auditClient.get(201 + index);
                    results[index] = result != null && result.getAlertName().contains("Concurrent Test Alert");
                    // Verify isDeleted
                    if (result != null) {
                        Assertions.assertEquals(0, result.getIsDeleted().intValue(), "Rule should not be deleted");
                    }
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
        rule.setIsDeleted(0); // Set default isDeleted to 0
        rule.setVersion(1); // Set default version to 1
        return rule;
    }

    /**
     * Create test AuditAlertRuleRequest object
     */
    private AuditAlertRuleRequest createTestAlertRuleRequest() {
        AuditAlertRuleRequest request = new AuditAlertRuleRequest();
        request.setInlongGroupId(TEST_GROUP_ID);
        request.setInlongStreamId(TEST_STREAM_ID);
        request.setAuditId(TEST_AUDIT_ID);
        request.setAlertName(TEST_ALERT_NAME);
        request.setCondition(TEST_CONDITION);
        request.setLevel(TEST_LEVEL);
        request.setNotifyType(TEST_NOTIFY_TYPE);
        request.setReceivers(TEST_RECEIVERS);
        request.setEnabled(true);
        return request;
    }

    /**
     * Create test AuditAlertCondition object
     */
    private static AuditAlertCondition createTestCondition() {
        AuditAlertCondition condition = new AuditAlertCondition();
        condition.setType("count");
        condition.setOperator("<");
        condition.setValue(1000);
        return condition;
    }
}