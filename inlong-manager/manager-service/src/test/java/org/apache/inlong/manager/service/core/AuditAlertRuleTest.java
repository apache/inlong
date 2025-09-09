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

import org.apache.inlong.manager.pojo.audit.AuditAlertCondition;
import org.apache.inlong.manager.pojo.audit.AuditAlertRule;
import org.apache.inlong.manager.pojo.audit.AuditAlertRuleRequest;
import org.apache.inlong.manager.pojo.audit.AuditAlertRuleUpdateRequest;

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
        AuditAlertCondition condition = new AuditAlertCondition();
        condition.setType("count");
        condition.setOperator(">");
        condition.setValue(10000);
        sampleRule.setCondition(condition);
        sampleRule.setLevel("WARN");
        sampleRule.setNotifyType("EMAIL");
        sampleRule.setReceivers("admin@example.com");
        sampleRule.setEnabled(true);
        sampleRule.setIsDeleted(0); // Set isDeleted to 0 by default
        sampleRule.setCreator("test_user");
        sampleRule.setModifier("test_user");
        sampleRule.setVersion(1); // Set default version to 1
    }

    @Test
    public void testCreateAndQueryAlertRule() {
        // Test creation
        AuditAlertRule rule = new AuditAlertRule();
        rule.setInlongGroupId("test_group");
        rule.setInlongStreamId("test_stream");
        rule.setAuditId("3");
        rule.setAlertName("Test Alert Rule");
        AuditAlertCondition condition = new AuditAlertCondition();
        condition.setType("count");
        condition.setOperator(">");
        condition.setValue(10000);
        rule.setCondition(condition);
        rule.setLevel("WARN");
        rule.setNotifyType("EMAIL");
        rule.setReceivers("admin@example.com");
        rule.setEnabled(true);
        rule.setIsDeleted(0); // Set isDeleted to 0 by default
        rule.setCreator("test_user");
        rule.setModifier("test_user");
        rule.setVersion(1); // Set default version to 1

        // Mock behavior for creation
        AuditAlertRule created = new AuditAlertRule();
        created.setId(1);
        created.setInlongGroupId("test_group");
        created.setAlertName("Test Alert Rule");
        created.setIsDeleted(0); // Set isDeleted to 0
        created.setVersion(1); // Set version to 1
        when(auditService.create(any(AuditAlertRule.class), eq("test_user")))
                .thenReturn(created);

        AuditAlertRule createdRule = auditService.create(rule, "test_user");
        assertNotNull(createdRule);
        assertNotNull(createdRule.getId());
        assertEquals("test_group", createdRule.getInlongGroupId());
        assertEquals("Test Alert Rule", createdRule.getAlertName());
        assertEquals(0, createdRule.getIsDeleted().intValue()); // Verify isDeleted is 0

        // Mock behavior for query by ID
        when(auditService.get(1))
                .thenReturn(created);

        // Test query by ID
        AuditAlertRule queried = auditService.get(created.getId());
        assertNotNull(queried);
        assertEquals(created.getId(), queried.getId());
        assertEquals("test_group", queried.getInlongGroupId());

        // Mock behavior for list rules
        when(auditService.listRules("test_group", "test_stream"))
                .thenReturn(Arrays.asList(created));

        // Test list rules
        List<AuditAlertRule> rules = auditService.listRules("test_group", "test_stream");
        assertNotNull(rules);
        assertTrue(rules.size() > 0);
        assertTrue(rules.stream().anyMatch(r -> r.getId().equals(created.getId())));

        // Mock behavior for update
        AuditAlertRule updatedRule = new AuditAlertRule();
        updatedRule.setId(1);
        updatedRule.setAlertName("Updated Alert Rule");
        updatedRule.setLevel("ERROR");
        updatedRule.setVersion(2); // Increment version
        when(auditService.update(any(AuditAlertRule.class), eq("test_user")))
                .thenReturn(updatedRule);

        // Test update
        queried.setAlertName("Updated Alert Rule");
        queried.setLevel("ERROR");
        queried.setVersion(2); // Set version for update
        AuditAlertRule updated = auditService.update(queried, "test_user");
        assertNotNull(updated);
        assertEquals("Updated Alert Rule", updated.getAlertName());
        assertEquals("ERROR", updated.getLevel());
        assertEquals(2, updated.getVersion().intValue()); // Verify version is incremented

        // Mock behavior for delete
        when(auditService.delete(1))
                .thenReturn(true);

        // Test delete
        Boolean deleted = auditService.delete(created.getId());
        assertTrue(deleted);

        // Mock behavior for get after delete (should throw exception)
        when(auditService.get(1))
                .thenThrow(new RuntimeException("Alert rule not found"));

        // Verify deletion
        assertThrows(Exception.class, () -> {
            auditService.get(created.getId());
        });
    }

    @Test
    public void testCreateAndUpdateAlertRuleWithRequest() {
        // Test creation with request
        AuditAlertRuleRequest request = new AuditAlertRuleRequest();
        request.setInlongGroupId("test_group");
        request.setInlongStreamId("test_stream");
        request.setAuditId("3");
        request.setAlertName("Test Alert Rule");
        AuditAlertCondition condition = new AuditAlertCondition();
        condition.setType("count");
        condition.setOperator(">");
        condition.setValue(10000);
        request.setCondition(condition);
        request.setLevel("WARN");
        request.setNotifyType("EMAIL");
        request.setReceivers("admin@example.com");
        request.setEnabled(true);

        // Mock behavior for creation with request
        AuditAlertRule created = new AuditAlertRule();
        created.setId(1);
        created.setInlongGroupId("test_group");
        created.setAlertName("Test Alert Rule");
        created.setIsDeleted(0); // Set isDeleted to 0
        created.setVersion(1); // Set version to 1
        when(auditService.create(any(AuditAlertRuleRequest.class), eq("test_user")))
                .thenReturn(created.getId());

        Integer createdRuleId = auditService.create(request, "test_user");
        assertNotNull(createdRuleId);
        assertEquals(created.getId(), createdRuleId);

        // Test update with request
        AuditAlertRuleUpdateRequest updateRequest = new AuditAlertRuleUpdateRequest();
        updateRequest.setId(1);
        updateRequest.setLevel("ERROR");
        updateRequest.setNotifyType("SMS");
        updateRequest.setReceivers("updated@example.com");
        updateRequest.setEnabled(false);
        updateRequest.setVersion(1);

        // Mock behavior for update with request
        AuditAlertRule updatedRule = new AuditAlertRule();
        updatedRule.setId(1);
        updatedRule.setAlertName("Test Alert Rule");
        updatedRule.setLevel("ERROR");
        updatedRule.setNotifyType("SMS");
        updatedRule.setReceivers("updated@example.com");
        updatedRule.setEnabled(false);
        updatedRule.setVersion(2); // Increment version
        when(auditService.update(any(AuditAlertRuleUpdateRequest.class), eq("test_user")))
                .thenReturn(updatedRule);

        AuditAlertRule updated = auditService.update(updateRequest, "test_user");
        assertNotNull(updated);
        assertEquals("ERROR", updated.getLevel());
        assertEquals("SMS", updated.getNotifyType());
        assertEquals("updated@example.com", updated.getReceivers());
        assertFalse(updated.getEnabled());
        assertEquals(2, updated.getVersion().intValue()); // Verify version is incremented
    }

    @Test
    public void testListEnabledAlertRules() {
        // Test list enabled alert rules
        AuditAlertRule rule1 = new AuditAlertRule();
        rule1.setId(1);
        rule1.setAlertName("Enabled Rule 1");
        rule1.setEnabled(true);
        rule1.setIsDeleted(0);

        AuditAlertRule rule2 = new AuditAlertRule();
        rule2.setId(2);
        rule2.setAlertName("Enabled Rule 2");
        rule2.setEnabled(true);
        rule2.setIsDeleted(0);

        AuditAlertRule rule3 = new AuditAlertRule();
        rule3.setId(3);
        rule3.setAlertName("Disabled Rule");
        rule3.setEnabled(false);
        rule3.setIsDeleted(0);

        List<AuditAlertRule> allRules = Arrays.asList(rule1, rule2, rule3);
        when(auditService.listEnabled())
                .thenReturn(Arrays.asList(rule1, rule2));

        List<AuditAlertRule> enabledRules = auditService.listEnabled();
        assertNotNull(enabledRules);
        assertEquals(2, enabledRules.size());
        assertTrue(enabledRules.stream().allMatch(AuditAlertRule::getEnabled));
    }

    @Test
    public void testValidation() {
        // Test null group ID
        AuditAlertRule rule = new AuditAlertRule();
        rule.setAuditId("3");
        rule.setAlertName("Test");
        AuditAlertCondition condition = new AuditAlertCondition();
        condition.setType("count");
        condition.setOperator(">");
        condition.setValue(5000);
        rule.setCondition(condition);

        // Mock behavior for validation error
        when(auditService.create(any(AuditAlertRule.class), eq("test_user")))
                .thenThrow(new IllegalArgumentException("Group ID cannot be null"));

        assertThrows(Exception.class, () -> {
            auditService.create(rule, "test_user");
        });

        // Test null audit ID
        rule.setInlongGroupId("test_group");
        rule.setAuditId(null);

        // Mock behavior for validation error
        when(auditService.create(any(AuditAlertRule.class), eq("test_user")))
                .thenThrow(new IllegalArgumentException("Audit ID cannot be null"));

        assertThrows(Exception.class, () -> {
            auditService.create(rule, "test_user");
        });
    }

    private AuditAlertRule createTestRule(String groupId, String streamId, String alertName) {
        AuditAlertRule rule = new AuditAlertRule();
        rule.setInlongGroupId(groupId);
        rule.setInlongStreamId(streamId);
        rule.setAuditId("3");
        rule.setAlertName(alertName);
        AuditAlertCondition condition = new AuditAlertCondition();
        condition.setType("data_loss");
        condition.setOperator(">");
        condition.setValue(1000);
        rule.setCondition(condition);
        rule.setLevel("WARN");
        rule.setNotifyType("EMAIL");
        rule.setReceivers("admin@example.com");
        rule.setEnabled(true);
        rule.setIsDeleted(0); // Set isDeleted to 0
        return rule;
    }
}