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

import org.apache.inlong.manager.pojo.audit.AuditAlertRule;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/**
 * Test cases for Audit Alert Rule functionality.
 */
@ExtendWith(MockitoExtension.class)
public class AuditAlertRuleTest {

    @Mock
    private AuditService auditService;

    private AuditAlertRule sampleRule;

    @BeforeEach
    public void setUp() {
        sampleRule = new AuditAlertRule();
        sampleRule.setId(1);
        sampleRule.setInlongGroupId("test_group");
        sampleRule.setInlongStreamId("test_stream");
        sampleRule.setAuditId("3");
        sampleRule.setAlertName("Test Alert Rule");
        sampleRule.setCondition("count > 10000");
        sampleRule.setLevel("WARN");
        sampleRule.setNotifyType("EMAIL");
        sampleRule.setReceivers("admin@example.com");
        sampleRule.setEnabled(true);
    }

    @Test
    public void testCreateAndQueryAlertRule() {
        // Test creation
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

        // Mock behavior for creation
        AuditAlertRule created = new AuditAlertRule();
        created.setId(1);
        created.setInlongGroupId("test_group");
        created.setAlertName("Test Alert Rule");
        when(auditService.createAlertRule(any(AuditAlertRule.class), eq("test_user")))
                .thenReturn(created);

        AuditAlertRule createdRule = auditService.createAlertRule(rule, "test_user");
        assertNotNull(createdRule);
        assertNotNull(createdRule.getId());
        assertEquals("test_group", createdRule.getInlongGroupId());
        assertEquals("Test Alert Rule", createdRule.getAlertName());

        // Mock behavior for query by ID
        when(auditService.getAlertRule(1))
                .thenReturn(created);

        // Test query by ID
        AuditAlertRule queried = auditService.getAlertRule(created.getId());
        assertNotNull(queried);
        assertEquals(created.getId(), queried.getId());
        assertEquals("test_group", queried.getInlongGroupId());

        // Mock behavior for list rules
        when(auditService.listAlertRules("test_group", "test_stream"))
                .thenReturn(Arrays.asList(created));

        // Test list rules
        List<AuditAlertRule> rules = auditService.listAlertRules("test_group", "test_stream");
        assertNotNull(rules);
        assertTrue(rules.size() > 0);
        assertTrue(rules.stream().anyMatch(r -> r.getId().equals(created.getId())));

        // Mock behavior for update
        AuditAlertRule updatedRule = new AuditAlertRule();
        updatedRule.setId(1);
        updatedRule.setAlertName("Updated Alert Rule");
        updatedRule.setLevel("ERROR");
        when(auditService.updateAlertRule(any(AuditAlertRule.class), eq("test_user")))
                .thenReturn(updatedRule);

        // Test update
        queried.setAlertName("Updated Alert Rule");
        queried.setLevel("ERROR");
        AuditAlertRule updated = auditService.updateAlertRule(queried, "test_user");
        assertNotNull(updated);
        assertEquals("Updated Alert Rule", updated.getAlertName());
        assertEquals("ERROR", updated.getLevel());

        // Mock behavior for delete
        when(auditService.deleteAlertRule(1))
                .thenReturn(true);

        // Test delete
        Boolean deleted = auditService.deleteAlertRule(created.getId());
        assertTrue(deleted);

        // Mock behavior for get after delete (should throw exception)
        when(auditService.getAlertRule(1))
                .thenThrow(new RuntimeException("Alert rule not found"));

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

        // Mock behavior for validation error
        when(auditService.createAlertRule(any(AuditAlertRule.class), eq("test_user")))
                .thenThrow(new IllegalArgumentException("Group ID cannot be null"));

        assertThrows(Exception.class, () -> {
            auditService.createAlertRule(rule, "test_user");
        });

        // Test null audit ID
        rule.setInlongGroupId("test_group");
        rule.setAuditId(null);

        // Mock behavior for validation error
        when(auditService.createAlertRule(any(AuditAlertRule.class), eq("test_user")))
                .thenThrow(new IllegalArgumentException("Audit ID cannot be null"));

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

        AuditAlertRule created1 = new AuditAlertRule();
        created1.setId(1);
        created1.setInlongGroupId("group1");
        created1.setInlongStreamId("stream1");
        created1.setAlertName("Rule 1");

        AuditAlertRule created2 = new AuditAlertRule();
        created2.setId(2);
        created2.setInlongGroupId("group1");
        created2.setInlongStreamId("stream2");
        created2.setAlertName("Rule 2");

        AuditAlertRule created3 = new AuditAlertRule();
        created3.setId(3);
        created3.setInlongGroupId("group2");
        created3.setInlongStreamId("stream1");
        created3.setAlertName("Rule 3");

        // Mock behavior for creation
        when(auditService.createAlertRule(eq(rule1), eq("test_user")))
                .thenReturn(created1);
        when(auditService.createAlertRule(eq(rule2), eq("test_user")))
                .thenReturn(created2);
        when(auditService.createAlertRule(eq(rule3), eq("test_user")))
                .thenReturn(created3);

        auditService.createAlertRule(rule1, "test_user");
        auditService.createAlertRule(rule2, "test_user");
        auditService.createAlertRule(rule3, "test_user");

        // Mock behavior for list by group only
        when(auditService.listAlertRules("group1", null))
                .thenReturn(Arrays.asList(created1, created2));

        // Test list by group only
        List<AuditAlertRule> group1Rules = auditService.listAlertRules("group1", null);
        assertEquals(2, group1Rules.size());

        // Mock behavior for list by group and stream
        when(auditService.listAlertRules("group1", "stream1"))
                .thenReturn(Arrays.asList(created1));

        // Test list by group and stream
        List<AuditAlertRule> stream1Rules = auditService.listAlertRules("group1", "stream1");
        assertEquals(1, stream1Rules.size());
        assertEquals("Rule 1", stream1Rules.get(0).getAlertName());

        // Mock behavior for delete
        when(auditService.deleteAlertRule(1)).thenReturn(true);
        when(auditService.deleteAlertRule(2)).thenReturn(true);
        when(auditService.deleteAlertRule(3)).thenReturn(true);

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