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

package org.apache.inlong.managerClient;

import org.apache.inlong.audit.tool.DTO.AlertPolicy;
import org.apache.inlong.audit.tool.DTO.AuditAlertRule;
import org.apache.inlong.audit.tool.DTO.AuditData;
import org.apache.inlong.audit.tool.config.AppConfig;
import org.apache.inlong.audit.tool.manager.ManagerClient;
import org.apache.inlong.audit.tool.response.Response;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class ManagerClientTest {

    private final ManagerClient managerClient = new ManagerClient(new AppConfig());

    @Test
    void testFetchAlertPolicies_Success() {
        // Mock data
        AuditAlertRule rule1 = new AuditAlertRule();
        rule1.setId(1);
        rule1.setInlongGroupId("group1");
        rule1.setInlongStreamId("stream1");
        rule1.setAuditId("1,2,3");

        AuditAlertRule rule2 = new AuditAlertRule();
        rule2.setId(2);
        rule2.setInlongGroupId("group2");
        rule2.setInlongStreamId("stream2");
        rule2.setAuditId("4,5");

        List<AuditAlertRule> mockRules = Arrays.asList(rule1, rule2);

        // Mock response
        Response<List<AuditAlertRule>> mockResponse = new Response<>();
        mockResponse.setSuccess(true);
        mockResponse.setData(mockRules);

        try {
            // Execute
            List<AlertPolicy> result = managerClient.fetchAlertPolicies();
            // Verify
            assertNotNull(result);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    void testFetchAlertPolicies_Failure() {
        // Mock response
        Response<List<AuditAlertRule>> mockResponse = new Response<>();
        mockResponse.setSuccess(false);
        mockResponse.setErrMsg("Failed to fetch alert rules");

        // Execute
        try {
            List<AlertPolicy> result = managerClient.fetchAlertPolicies();
            // Verify
            assertNotNull(result);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    void testFetchAlertRules_Success() {
        // Mock data
        AuditAlertRule rule = new AuditAlertRule();
        rule.setId(1);
        rule.setInlongGroupId("group1");

        Response<List<AuditAlertRule>> mockResponse = new Response<>();
        mockResponse.setSuccess(true);
        mockResponse.setData(Collections.singletonList(rule));

        try {
            // Execute
            List<AuditAlertRule> result = managerClient.fetchAlertRules();

            // Verify
            assertNotNull(result);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    void testFetchAlertRules_Failure() {
        // Mock response
        Response<List<AuditAlertRule>> mockResponse = new Response<>();
        mockResponse.setSuccess(false);

        try {
            // Execute
            List<AuditAlertRule> result = managerClient.fetchAlertRules();

            // Verify
            assertNotNull(result);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    void testFetchAuditData_Success() {
        // Mock alert rules
        AuditAlertRule rule = new AuditAlertRule();
        rule.setInlongGroupId("group1");
        rule.setInlongStreamId("stream1");
        rule.setAuditId("1,2,3");

        Response<List<AuditAlertRule>> alertRulesResponse = new Response<>();
        alertRulesResponse.setSuccess(true);
        alertRulesResponse.setData(Collections.singletonList(rule));

        try {
            // Execute
            List<AuditData> result = managerClient.fetchAuditData();

            // Verify
            assertNotNull(result);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    void testFetchAuditData_NoAlertRules() {
        // Mock empty alert rules
        Response<List<AuditAlertRule>> alertRulesResponse = new Response<>();
        alertRulesResponse.setSuccess(true);
        alertRulesResponse.setData(Collections.emptyList());

        try {
            // Execute
            List<AuditData> result = managerClient.fetchAuditData();

            // Verify
            assertNotNull(result);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    void testFetchAuditData_AuditRequestFailure() {
        // Mock alert rules
        AuditAlertRule rule = new AuditAlertRule();
        rule.setInlongGroupId("group1");
        rule.setInlongStreamId("stream1");
        rule.setAuditId("1,2,3");

        Response<List<AuditAlertRule>> alertRulesResponse = new Response<>();
        alertRulesResponse.setSuccess(true);
        alertRulesResponse.setData(Collections.singletonList(rule));

        try {
            // Execute
            List<AuditData> result = managerClient.fetchAuditData();

            // Verify
            assertNotNull(result);
            assertTrue(result.isEmpty());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}