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

package org.apache.inlong.manager.service.core;

import org.apache.inlong.manager.pojo.audit.alert.AuditAlertRule;
import org.apache.inlong.manager.service.core.impl.AuditServiceImpl;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.TestPropertySource;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Test cases for Audit Alert Rule functionality.
 */
@SpringBootTest(classes = org.apache.inlong.manager.service.TestApplication.class)
@TestPropertySource(locations = "classpath:application-test.properties")
@Transactional
public class AuditAlertRuleTest {

    @Autowired
    private AuditService auditService;

    @Test
    public void testCreateAndQueryAlertRule() {
        // Create test rule
        AuditAlertRule rule = new AuditAlertRule();
        rule.setInlongGroupId("test_group");
        rule.setInlongStreamId("test_stream");
        rule.setAuditId("3");
        rule.setAlertName("Test Alert Rule");
        rule.setCondition("count > 10000");
        rule.setLevel("WARN");
        rule.setNotifyType("EMAIL");
        rule.setReceivers("admin@example.com");
        rule.setEnabled(true);

        // Test creation
        AuditAlertRule created = auditService.createAlertRule(rule, "test_user");
        assertNotNull(created);
        assertNotNull(created.getId());
        assertEquals("test_group", created.getInlongGroupId());
        assertEquals("Test Alert Rule", created.getAlertName());

        // Test query by ID
        AuditAlertRule queried = auditService.getAlertRule(created.getId());
        assertNotNull(queried);
        assertEquals(created.getId(), queried.getId());
        assertEquals("test_group", queried.getInlongGroupId());

        // Test list rules
        List<AuditAlertRule> rules = auditService.listAlertRules("test_group", "test_stream");
        assertNotNull(rules);
        assertTrue(rules.size() > 0);
        assertTrue(rules.stream().anyMatch(r -> r.getId().equals(created.getId())));

        // Test update
        queried.setAlertName("Updated Alert Rule");
        queried.setLevel("ERROR");
        AuditAlertRule updated = auditService.updateAlertRule(queried, "test_user");
        assertNotNull(updated);
        assertEquals("Updated Alert Rule", updated.getAlertName());
        assertEquals("ERROR", updated.getLevel());

        // Test delete
        Boolean deleted = auditService.deleteAlertRule(created.getId());
        assertTrue(deleted);

        // Verify deletion
        assertThrows(Exception.class, () -> {
            auditService.getAlertRule(created.getId());
        });
    }

    @Test
    public void testValidation() {
        // Test null group ID
        AuditAlertRule rule = new AuditAlertRule();
        rule.setAuditId("3");
        rule.setAlertName("Test");
        rule.setCondition("count > 1000");

        assertThrows(Exception.class, () -> {
            auditService.createAlertRule(rule, "test_user");
        });

        // Test null audit ID
        rule.setInlongGroupId("test_group");
        rule.setAuditId(null);
        assertThrows(Exception.class, () -> {
            auditService.createAlertRule(rule, "test_user");
        });
    }

    @Test
    public void testListByGroupAndStream() {
        // Create multiple rules for different groups/streams
        AuditAlertRule rule1 = createTestRule("group1", "stream1", "Rule 1");
        AuditAlertRule rule2 = createTestRule("group1", "stream2", "Rule 2");
        AuditAlertRule rule3 = createTestRule("group2", "stream1", "Rule 3");

        AuditAlertRule created1 = auditService.createAlertRule(rule1, "test_user");
        AuditAlertRule created2 = auditService.createAlertRule(rule2, "test_user");
        AuditAlertRule created3 = auditService.createAlertRule(rule3, "test_user");

        // Test list by group only
        List<AuditAlertRule> group1Rules = auditService.listAlertRules("group1", null);
        assertEquals(2, group1Rules.size());

        // Test list by group and stream
        List<AuditAlertRule> stream1Rules = auditService.listAlertRules("group1", "stream1");
        assertEquals(1, stream1Rules.size());
        assertEquals("Rule 1", stream1Rules.get(0).getAlertName());

        // Clean up
        auditService.deleteAlertRule(created1.getId());
        auditService.deleteAlertRule(created2.getId());
        auditService.deleteAlertRule(created3.getId());
    }

    private AuditAlertRule createTestRule(String groupId, String streamId, String alertName) {
        AuditAlertRule rule = new AuditAlertRule();
        rule.setInlongGroupId(groupId);
        rule.setInlongStreamId(streamId);
        rule.setAuditId("3");
        rule.setAlertName(alertName);
        rule.setCondition("count > 5000");
        rule.setLevel("WARN");
        rule.setNotifyType("EMAIL");
        rule.setReceivers("test@example.com");
        rule.setEnabled(true);
        return rule;
    }
}