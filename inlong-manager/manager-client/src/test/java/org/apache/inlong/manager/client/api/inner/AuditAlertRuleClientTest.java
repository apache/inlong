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

import org.apache.inlong.manager.client.api.ClientConfiguration;
import org.apache.inlong.manager.client.api.impl.InlongClientImpl;
import org.apache.inlong.manager.client.api.inner.client.AuditAlertRuleClient;
import org.apache.inlong.manager.client.api.inner.client.ClientFactory;
import org.apache.inlong.manager.client.api.util.ClientUtils;
import org.apache.inlong.manager.common.auth.DefaultAuthentication;
import org.apache.inlong.manager.common.util.JsonUtils;
import org.apache.inlong.manager.pojo.audit.AuditAlertCondition;
import org.apache.inlong.manager.pojo.audit.AuditAlertRule;
import org.apache.inlong.manager.pojo.audit.AuditAlertRulePageRequest;
import org.apache.inlong.manager.pojo.audit.AuditAlertRuleRequest;
import org.apache.inlong.manager.pojo.common.PageResult;
import org.apache.inlong.manager.pojo.common.Response;

import com.github.tomakehurst.wiremock.WireMockServer;
import com.google.common.collect.Lists;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.Date;
import java.util.List;

import static com.github.tomakehurst.wiremock.client.WireMock.delete;
import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.okJson;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.client.WireMock.put;
import static com.github.tomakehurst.wiremock.client.WireMock.stubFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlMatching;
import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.options;

/** Unit test for {@link org.apache.inlong.manager.client.api.inner.AuditAlertRuleClient} */
public class AuditAlertRuleClientTest {

    private static final int SERVICE_PORT = 8085;
    private static WireMockServer wireMockServer;
    private static AuditAlertRuleClient auditAlertRuleClient;

    @BeforeAll
    static void setup() {
        wireMockServer = new WireMockServer(options().port(SERVICE_PORT));
        wireMockServer.start();
        com.github.tomakehurst.wiremock.client.WireMock.configureFor(wireMockServer.port());

        String serviceUrl = "127.0.0.1:" + SERVICE_PORT;
        ClientConfiguration configuration = new ClientConfiguration();
        configuration.setAuthentication(new DefaultAuthentication("admin", "inlong"));
        InlongClientImpl inlongClient = new InlongClientImpl(serviceUrl, configuration);
        ClientFactory clientFactory = ClientUtils.getClientFactory(inlongClient.getConfiguration());

        auditAlertRuleClient = clientFactory.getAuditAlertRuleClient();
    }

    @AfterAll
    static void teardown() {
        wireMockServer.stop();
    }

    @Test
    void testCreate() {
        AuditAlertRuleRequest request = new AuditAlertRuleRequest();
        request.setInlongGroupId("test_group");
        request.setAuditId("test_audit");
        request.setAlertName("test_alert");
        request.setCondition(new AuditAlertCondition());
        request.setEnabled(true);

        stubFor(
                post(urlMatching("/inlong/manager/api/audit/alert/rule.*"))
                        .willReturn(
                                okJson(JsonUtils.toJsonString(
                                        Response.success(1)))));

        Integer result = auditAlertRuleClient.create(request);
        Assertions.assertEquals(1, result);
    }

    @Test
    void testGet() {
        AuditAlertRule rule = new AuditAlertRule();
        rule.setId(1);
        rule.setInlongGroupId("test_group");
        rule.setAuditId("test_audit");
        rule.setAlertName("test_alert");
        rule.setCondition(new AuditAlertCondition());
        rule.setEnabled(true);
        rule.setCreateTime(new Date());
        rule.setModifyTime(new Date());

        stubFor(
                get(urlMatching("/inlong/manager/api/audit/alert/rule/get/1.*"))
                        .willReturn(
                                okJson(JsonUtils.toJsonString(
                                        Response.success(rule)))));

        AuditAlertRule result = auditAlertRuleClient.get(1);
        Assertions.assertEquals(1, result.getId());
        Assertions.assertEquals("test_group", result.getInlongGroupId());
        Assertions.assertEquals("test_audit", result.getAuditId());
        Assertions.assertEquals("test_alert", result.getAlertName());
    }

    @Test
    void testListByCondition() {
        AuditAlertRule rule = new AuditAlertRule();
        rule.setId(1);
        rule.setInlongGroupId("test_group");
        rule.setAuditId("test_audit");
        rule.setAlertName("test_alert");
        rule.setCondition(new AuditAlertCondition());
        rule.setEnabled(true);
        rule.setCreateTime(new Date());
        rule.setModifyTime(new Date());

        List<AuditAlertRule> rules = Lists.newArrayList(rule);
        PageResult<AuditAlertRule> pageResult = new PageResult<>(rules, 1L, 1, 10);

        stubFor(
                post(urlMatching("/inlong/manager/api/audit/alert/rule/list.*"))
                        .willReturn(
                                okJson(JsonUtils.toJsonString(
                                        Response.success(pageResult)))));

        AuditAlertRulePageRequest request = new AuditAlertRulePageRequest();
        request.setInlongGroupId("test_group");
        PageResult<AuditAlertRule> result = auditAlertRuleClient.listByCondition(request);
        Assertions.assertEquals(1, result.getList().size());
        Assertions.assertEquals(1L, result.getTotal());
    }

    @Test
    void testUpdate() {
        AuditAlertRuleRequest request = new AuditAlertRuleRequest();
        request.setId(1);
        request.setInlongGroupId("test_group");
        request.setAuditId("test_audit");
        request.setAlertName("test_alert");
        request.setCondition(new AuditAlertCondition());
        request.setEnabled(true);
        request.setVersion(1);

        stubFor(
                put(urlMatching("/inlong/manager/api/audit/alert/rule/update.*"))
                        .willReturn(
                                okJson(JsonUtils.toJsonString(
                                        Response.success(true)))));

        Boolean result = auditAlertRuleClient.update(request);
        Assertions.assertTrue(result);
    }

    @Test
    void testDelete() {
        stubFor(
                delete(urlMatching("/inlong/manager/api/audit/delete/1.*"))
                        .willReturn(
                                okJson(JsonUtils.toJsonString(
                                        Response.success(true)))));

        Boolean result = auditAlertRuleClient.delete(1);
        Assertions.assertTrue(result);
    }
}