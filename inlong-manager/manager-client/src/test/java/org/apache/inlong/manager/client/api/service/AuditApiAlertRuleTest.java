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

package org.apache.inlong.manager.client.api.service;

import org.apache.inlong.manager.client.api.ClientConfiguration;
import org.apache.inlong.manager.client.api.impl.InlongClientImpl;
import org.apache.inlong.manager.client.api.inner.client.ClientFactory;
import org.apache.inlong.manager.client.api.util.ClientUtils;
import org.apache.inlong.manager.common.auth.DefaultAuthentication;
import org.apache.inlong.manager.common.util.JsonUtils;
import org.apache.inlong.manager.pojo.audit.AuditAlertRule;
import org.apache.inlong.manager.pojo.audit.AuditAlertRuleRequest;
import org.apache.inlong.manager.pojo.audit.Condition;
import org.apache.inlong.manager.pojo.common.Response;

import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.client.WireMock;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import retrofit2.Call;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

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
 * Tests for {@link AuditApi} audit alert rule functionality
 */
public class AuditApiAlertRuleTest {

    private static final int SERVICE_PORT = 8085;
    static ClientFactory clientFactory;
    private static WireMockServer wireMockServer;
    private static AuditApi auditApi;

    @BeforeAll
    static void setup() {
        wireMockServer = new WireMockServer(options().port(SERVICE_PORT));
        wireMockServer.start();
        WireMock.configureFor(wireMockServer.port());

        String serviceUrl = "127.0.0.1:" + SERVICE_PORT;
        ClientConfiguration configuration = new ClientConfiguration();
        configuration.setAuthentication(new DefaultAuthentication("admin", "inlong"));
        InlongClientImpl inlongClient = new InlongClientImpl(serviceUrl, configuration);
        clientFactory = ClientUtils.getClientFactory(inlongClient.getConfiguration());

        auditApi = ClientUtils.createRetrofit(configuration).create(AuditApi.class);
    }

    @AfterAll
    static void teardown() {
        wireMockServer.stop();
    }

    @Test
    void testCreateAlertRuleApi() throws IOException {
        // Prepare test data
        AuditAlertRuleRequest inputRule = createTestAlertRuleRequest();
        Integer expectedId = 1;

        String requestBody = JsonUtils.toJsonString(inputRule);
        String responseBody = JsonUtils.toJsonString(Response.success(expectedId));

        // Mock API response
        stubFor(
                post(urlMatching("/inlong/manager/api/audit/alert/rule.*"))
                        .withRequestBody(equalToJson(requestBody))
                        .willReturn(okJson(responseBody)));

        // Execute test
        Call<Response<Integer>> call = auditApi.createAlertRule(inputRule);
        Response<Integer> response = call.execute().body();

        // Verify result
        Assertions.assertNotNull(response);
        Assertions.assertTrue(response.isSuccess());
        Assertions.assertNotNull(response.getData());
        Assertions.assertEquals(1, response.getData().intValue());
    }

    @Test
    void testGetAlertRuleApi() throws IOException {
        // Prepare test data
        AuditAlertRule expectedRule = createTestAlertRule();
        expectedRule.setId(1);
        expectedRule.setVersion(1); // Set version to 1
        expectedRule.setIsDeleted(0); // Set isDeleted to 0

        String responseBody = JsonUtils.toJsonString(Response.success(expectedRule));

        // Mock API response
        stubFor(
                get(urlMatching("/inlong/manager/api/audit/alert/rule/1.*"))
                        .willReturn(okJson(responseBody)));

        // Execute test
        Call<Response<AuditAlertRule>> call = auditApi.getAlertRule(1);
        Response<AuditAlertRule> response = call.execute().body();

        // Verify result
        Assertions.assertNotNull(response);
        Assertions.assertTrue(response.isSuccess());
        Assertions.assertNotNull(response.getData());
        Assertions.assertEquals(1, response.getData().getId());
        Assertions.assertEquals("test_group_001", response.getData().getInlongGroupId());
        Assertions.assertEquals(0, response.getData().getIsDeleted().intValue()); // Verify isDeleted
    }

    @Test
    void testListEnabledAlertRulesApi() throws IOException {
        // Prepare test data
        AuditAlertRule rule1 = createTestAlertRule();
        rule1.setId(1);
        rule1.setEnabled(true);
        rule1.setVersion(1); // Set version to 1
        rule1.setIsDeleted(0); // Set isDeleted to 0

        AuditAlertRule rule2 = createTestAlertRule();
        rule2.setId(2);
        rule2.setAlertName("High Delay Alert");
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
        Call<Response<List<AuditAlertRule>>> call = auditApi.listEnabledAlertRules();
        Response<List<AuditAlertRule>> response = call.execute().body();

        // Verify result
        Assertions.assertNotNull(response);
        Assertions.assertTrue(response.isSuccess());
        Assertions.assertNotNull(response.getData());
        Assertions.assertEquals(2, response.getData().size());
        Assertions.assertTrue(response.getData().get(0).getEnabled());
        Assertions.assertTrue(response.getData().get(1).getEnabled());
        // Verify isDeleted for both rules
        Assertions.assertEquals(0, response.getData().get(0).getIsDeleted().intValue());
        Assertions.assertEquals(0, response.getData().get(1).getIsDeleted().intValue());
    }

    @Test
    void testListAlertRulesWithParametersApi() throws IOException {
        // Prepare test data
        AuditAlertRule rule = createTestAlertRule();
        rule.setId(1);
        rule.setVersion(1); // Set version to 1
        rule.setIsDeleted(0); // Set isDeleted to 0
        List<AuditAlertRule> expectedRules = Arrays.asList(rule);

        String responseBody = JsonUtils.toJsonString(Response.success(expectedRules));

        // Mock API response
        stubFor(
                get(urlMatching(
                        "/inlong/manager/api/audit/alert/rule/list\\?inlongGroupId=test_group_001&inlongStreamId=test_stream_001.*"))
                                .willReturn(okJson(responseBody)));

        // Execute test
        Call<Response<List<AuditAlertRule>>> call = auditApi.listAlertRules("test_group_001", "test_stream_001");
        Response<List<AuditAlertRule>> response = call.execute().body();

        // Verify result
        Assertions.assertNotNull(response);
        Assertions.assertTrue(response.isSuccess());
        Assertions.assertNotNull(response.getData());
        Assertions.assertEquals(1, response.getData().size());
        Assertions.assertEquals("test_group_001", response.getData().get(0).getInlongGroupId());
        Assertions.assertEquals("test_stream_001", response.getData().get(0).getInlongStreamId());
        Assertions.assertEquals(0, response.getData().get(0).getIsDeleted().intValue()); // Verify isDeleted
    }

    @Test
    void testListAlertRulesWithNullParametersApi() throws IOException {
        // Prepare test data
        AuditAlertRule rule = createTestAlertRule();
        rule.setId(1);
        rule.setVersion(1); // Set version to 1
        rule.setIsDeleted(0); // Set isDeleted to 0
        List<AuditAlertRule> expectedRules = Arrays.asList(rule);

        String responseBody = JsonUtils.toJsonString(Response.success(expectedRules));

        // Mock API response - test null parameters case
        stubFor(
                get(urlMatching("/inlong/manager/api/audit/alert/rule/list.*"))
                        .willReturn(okJson(responseBody)));

        // Execute test
        Call<Response<List<AuditAlertRule>>> call = auditApi.listAlertRules(null, null);
        Response<List<AuditAlertRule>> response = call.execute().body();

        // Verify result
        Assertions.assertNotNull(response);
        Assertions.assertTrue(response.isSuccess());
        Assertions.assertNotNull(response.getData());
        Assertions.assertEquals(1, response.getData().size());
        Assertions.assertEquals(0, response.getData().get(0).getIsDeleted().intValue()); // Verify isDeleted
    }

    @Test
    void testUpdateAlertRuleApi() throws IOException {
        // Prepare test data
        AuditAlertRule inputRule = createTestAlertRule();
        inputRule.setId(1);
        // Use Condition object instead of string
        Condition condition = new Condition();
        condition.setType("count");
        condition.setOperator("<");
        condition.setValue(500);
        inputRule.setCondition(condition);
        inputRule.setLevel("CRITICAL");
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
        Call<Response<AuditAlertRule>> call = auditApi.updateAlertRule(inputRule);
        Response<AuditAlertRule> response = call.execute().body();

        // Verify result
        Assertions.assertNotNull(response);
        Assertions.assertTrue(response.isSuccess());
        Assertions.assertNotNull(response.getData());
        Assertions.assertEquals(1, response.getData().getId());
        Assertions.assertEquals(condition, response.getData().getCondition());
        Assertions.assertEquals("CRITICAL", response.getData().getLevel());
        Assertions.assertEquals(2, response.getData().getVersion().intValue()); // Verify version is incremented
    }

    @Test
    void testDeleteAlertRuleApi() throws IOException {
        // Prepare test data
        String responseBody = JsonUtils.toJsonString(Response.success(true));

        // Mock API response
        stubFor(
                delete(urlMatching("/inlong/manager/api/audit/alert/rule/1.*"))
                        .willReturn(okJson(responseBody)));

        // Execute test
        Call<Response<Boolean>> call = auditApi.deleteAlertRule(1);
        Response<Boolean> response = call.execute().body();

        // Verify result
        Assertions.assertNotNull(response);
        Assertions.assertTrue(response.isSuccess());
        Assertions.assertNotNull(response.getData());
        Assertions.assertTrue(response.getData());
    }

    @Test
    void testDeleteNonExistentAlertRuleApi() throws IOException {
        // Prepare test data - delete non-existent rule
        String responseBody = JsonUtils.toJsonString(Response.success(false));

        // Mock API response
        stubFor(
                delete(urlMatching("/inlong/manager/api/audit/alert/rule/999.*"))
                        .willReturn(okJson(responseBody)));

        // Execute test
        Call<Response<Boolean>> call = auditApi.deleteAlertRule(999);
        Response<Boolean> response = call.execute().body();

        // Verify result
        Assertions.assertNotNull(response);
        Assertions.assertTrue(response.isSuccess());
        Assertions.assertNotNull(response.getData());
        Assertions.assertFalse(response.getData());
    }

    @Test
    void testApiErrorHandling() throws IOException {
        // Mock API error response
        stubFor(
                get(urlMatching("/inlong/manager/api/audit/alert/rule/1.*"))
                        .willReturn(WireMock.aResponse()
                                .withStatus(500)
                                .withHeader("Content-Type", "application/json")
                                .withBody(JsonUtils.toJsonString(Response.fail("Internal Server Error")))));

        // Execute test
        Call<Response<AuditAlertRule>> call = auditApi.getAlertRule(1);
        retrofit2.Response<Response<AuditAlertRule>> response = call.execute();

        // Verify error handling
        Assertions.assertEquals(500, response.code());
        // For 500 status code, response body might be null, so we just check the status code
        if (response.body() != null) {
            Assertions.assertFalse(response.body().isSuccess());
        }
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
        // Use Condition object
        Condition condition = new Condition();
        condition.setType("data_loss");
        condition.setOperator("<");
        condition.setValue(1000);
        rule.setCondition(condition);
        rule.setLevel("ERROR");
        rule.setNotifyType("EMAIL");
        rule.setReceivers("admin@example.com,monitor@example.com");
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
        request.setInlongGroupId("test_group_001");
        request.setInlongStreamId("test_stream_001");
        request.setAuditId("3");
        request.setAlertName("Data Loss Alert");
        // Use Condition object
        Condition condition = new Condition();
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