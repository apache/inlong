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

package org.apache.inlong.tool.AuditAlertRuleManager;

import org.apache.inlong.audit.tool.config.AppConfig;
import org.apache.inlong.audit.tool.dto.AuditAlertRule;
import org.apache.inlong.audit.tool.manager.AuditAlertRuleManager;
import org.apache.inlong.audit.tool.response.Response;

import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class AuditAlertRuleManagerTest {

    private final AuditAlertRuleManager auditAlertRuleManager = AuditAlertRuleManager.getInstance();

    @Test
    void testFetchAlertRulesFromManager() {
        // Mock data
        AuditAlertRule rule = new AuditAlertRule();
        rule.setId(1);
        rule.setInlongGroupId("group1");

        Response<List<AuditAlertRule>> mockResponse = new Response<>();
        mockResponse.setSuccess(true);
        mockResponse.setData(Collections.singletonList(rule));

        try {
            // Execute
            auditAlertRuleManager.init(new AppConfig());
            List<AuditAlertRule> result = auditAlertRuleManager.fetchAlertRulesFromManager();

            // Verify
            assertNotNull(result);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    void testFetchAuditIds() {
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
            auditAlertRuleManager.init(new AppConfig());
            List<String> result = auditAlertRuleManager.getAuditIds();

            // Verify
            assertNotNull(result);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}