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

package org.apache.inlong.manager.service.core.impl;

import org.apache.inlong.manager.dao.entity.AuditAlertRuleEntity;
import org.apache.inlong.manager.dao.mapper.AuditAlertRuleEntityMapper;
import org.apache.inlong.manager.pojo.audit.AuditAlertCondition;
import org.apache.inlong.manager.pojo.audit.AuditAlertRule;
import org.apache.inlong.manager.pojo.audit.AuditAlertRuleRequest;
import org.apache.inlong.manager.service.ServiceBaseTest;
import org.apache.inlong.manager.service.core.AuditAlertRuleService;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.Date;
import java.util.List;

/**
 * Test cases for {@link AuditServiceImpl} audit alert rule functionality
 */
class AuditServiceImplTest extends ServiceBaseTest {

    @Autowired
    private AuditAlertRuleService auditAlertRuleService;

    @Autowired
    private AuditAlertRuleEntityMapper auditAlertRuleMapper;

    private AuditAlertRuleEntity insertTestEntity() {
        AuditAlertRuleEntity entity = new AuditAlertRuleEntity();
        entity.setInlongGroupId("test_group_service");
        entity.setInlongStreamId("test_stream_service");
        entity.setAuditId("3");
        entity.setAlertName("Service Test Alert");
        entity.setCondition("{\"type\": \"data_loss\", \"operator\": \">\", \"value\": 5}");
        entity.setLevel("WARN");
        entity.setNotifyType("EMAIL");
        entity.setReceivers("service@test.com");
        entity.setEnabled(true);
        entity.setIsDeleted(0);
        entity.setCreator("test_user");
        entity.setModifier("test_user");
        entity.setCreateTime(new Date());
        entity.setModifyTime(new Date());
        entity.setVersion(1);

        auditAlertRuleMapper.insert(entity);
        return entity;
    }

    @Test
    void testUpdateAlertRule() {
        // Insert test data
        AuditAlertRuleEntity entity = insertTestEntity();

        // Retrieve the entity to get the current version
        AuditAlertRuleEntity freshEntity = auditAlertRuleMapper.selectById(entity.getId());

        // Create update request
        AuditAlertRule rule = new AuditAlertRule();
        rule.setId(freshEntity.getId());
        rule.setLevel("CRITICAL");
        rule.setNotifyType("SMS");
        rule.setReceivers("updated_service@test.com");
        rule.setEnabled(false);
        rule.setVersion(freshEntity.getVersion());

        // Update the rule
        AuditAlertRule updatedRule = auditAlertRuleService.update(rule, "test_user");

        // Verify the update
        Assertions.assertNotNull(updatedRule);
        Assertions.assertEquals("CRITICAL", updatedRule.getLevel());
        Assertions.assertEquals("SMS", updatedRule.getNotifyType());
        Assertions.assertEquals("updated_service@test.com", updatedRule.getReceivers());
        Assertions.assertFalse(updatedRule.getEnabled());
        Assertions.assertEquals(freshEntity.getVersion() + 1, updatedRule.getVersion().intValue());

        // Verify in database
        AuditAlertRuleEntity updatedEntity = auditAlertRuleMapper.selectById(entity.getId());
        Assertions.assertNotNull(updatedEntity);
        Assertions.assertEquals("CRITICAL", updatedEntity.getLevel());
        Assertions.assertEquals("SMS", updatedEntity.getNotifyType());
        Assertions.assertEquals("updated_service@test.com", updatedEntity.getReceivers());
        Assertions.assertFalse(updatedEntity.getEnabled());
        Assertions.assertEquals(freshEntity.getVersion() + 1, updatedEntity.getVersion().intValue());
    }

    @Test
    void testCreateAlertRuleWithRequest() {
        // Create test alert rule request
        AuditAlertRuleRequest request = new AuditAlertRuleRequest();
        request.setInlongGroupId("test_group_service_request");
        request.setInlongStreamId("test_stream_service_request");
        request.setAuditId("3");
        request.setAlertName("Service Test Alert From Request");
        AuditAlertCondition condition = new AuditAlertCondition();
        condition.setType("data_loss");
        condition.setOperator(">");
        condition.setValue(5);
        request.setCondition(condition);
        request.setLevel("WARN");
        request.setNotifyType("EMAIL");
        request.setReceivers("service_request@test.com");
        request.setEnabled(true);
        request.setVersion(1);

        // Create the rule
        Integer ruleId = auditAlertRuleService.create(request, "test_user");

        // Verify the creation
        Assertions.assertNotNull(ruleId);

        // Verify in database
        AuditAlertRuleEntity entity = auditAlertRuleMapper.selectById(ruleId);
        Assertions.assertNotNull(entity);
        Assertions.assertEquals("test_group_service_request", entity.getInlongGroupId());
        Assertions.assertEquals("Service Test Alert From Request", entity.getAlertName());
        Assertions.assertEquals("WARN", entity.getLevel());
        Assertions.assertEquals("EMAIL", entity.getNotifyType());
        Assertions.assertEquals("service_request@test.com", entity.getReceivers());
        Assertions.assertTrue(entity.getEnabled());
        Assertions.assertEquals(0, entity.getIsDeleted().intValue());
        Assertions.assertEquals(1, entity.getVersion().intValue());
    }

    @Test
    void testUpdateAlertRuleWithRequest() {
        // Insert test data
        AuditAlertRuleEntity entity = insertTestEntity();

        // Retrieve the entity to get the current version
        AuditAlertRuleEntity freshEntity = auditAlertRuleMapper.selectById(entity.getId());

        // Create update request
        AuditAlertRuleRequest updateRequest = new AuditAlertRuleRequest();
        updateRequest.setId(freshEntity.getId());
        updateRequest.setLevel("CRITICAL");
        updateRequest.setNotifyType("SMS");
        updateRequest.setReceivers("updated_service_request@test.com");
        updateRequest.setEnabled(false);
        updateRequest.setVersion(freshEntity.getVersion());

        // Update the rule
        AuditAlertRule updatedRule = auditAlertRuleService.update(updateRequest, "test_user");

        // Verify the update
        Assertions.assertNotNull(updatedRule);
        Assertions.assertEquals("CRITICAL", updatedRule.getLevel());
        Assertions.assertEquals("SMS", updatedRule.getNotifyType());
        Assertions.assertEquals("updated_service_request@test.com", updatedRule.getReceivers());
        Assertions.assertFalse(updatedRule.getEnabled());
        Assertions.assertEquals(freshEntity.getVersion() + 1, updatedRule.getVersion().intValue());

        // Verify in database
        AuditAlertRuleEntity updatedEntity = auditAlertRuleMapper.selectById(entity.getId());
        Assertions.assertNotNull(updatedEntity);
        Assertions.assertEquals("CRITICAL", updatedEntity.getLevel());
        Assertions.assertEquals("SMS", updatedEntity.getNotifyType());
        Assertions.assertEquals("updated_service_request@test.com", updatedEntity.getReceivers());
        Assertions.assertFalse(updatedEntity.getEnabled());
        Assertions.assertEquals(freshEntity.getVersion() + 1, updatedEntity.getVersion().intValue());
    }

    @Test
    void testListEnabledAlertRules() {
        // Insert test data
        AuditAlertRuleEntity entity1 = insertTestEntity();

        // Create enabled rule
        AuditAlertRuleEntity entity2 = new AuditAlertRuleEntity();
        entity2.setInlongGroupId("test_group_service_enabled");
        entity2.setAuditId("4");
        entity2.setAlertName("Enabled Rule");
        entity2.setCondition("{\"type\": \"count\", \"operator\": \">\", \"value\": 100}");
        entity2.setLevel("INFO");
        entity2.setNotifyType("EMAIL");
        entity2.setReceivers("enabled@test.com");
        entity2.setEnabled(true); // Enabled
        entity2.setIsDeleted(0);
        entity2.setCreator("test_user");
        entity2.setModifier("test_user");
        entity2.setCreateTime(new Date());
        entity2.setModifyTime(new Date());
        entity2.setVersion(1);
        auditAlertRuleMapper.insert(entity2);

        // Create disabled rule
        AuditAlertRuleEntity entity3 = new AuditAlertRuleEntity();
        entity3.setInlongGroupId("test_group_service_disabled");
        entity3.setAuditId("5");
        entity3.setAlertName("Disabled Rule");
        entity3.setCondition("{\"type\": \"delay\", \"operator\": \">\", \"value\": 1000}");
        entity3.setLevel("ERROR");
        entity3.setNotifyType("SMS");
        entity3.setReceivers("disabled@test.com");
        entity3.setEnabled(false); // Disabled
        entity3.setIsDeleted(0);
        entity3.setCreator("test_user");
        entity3.setModifier("test_user");
        entity3.setCreateTime(new Date());
        entity3.setModifyTime(new Date());
        entity3.setVersion(1);
        auditAlertRuleMapper.insert(entity3);

        // List enabled rules
        List<AuditAlertRule> enabledRules = auditAlertRuleService.listEnabled();

        // Verify the result
        Assertions.assertNotNull(enabledRules);
        // Should have at least the two enabled rules
        Assertions.assertTrue(enabledRules.size() >= 2);
        // All rules should be enabled
        Assertions.assertTrue(enabledRules.stream().allMatch(AuditAlertRule::getEnabled));
        // All rules should not be deleted
        Assertions.assertTrue(enabledRules.stream().allMatch(rule -> rule.getIsDeleted() == 0));
        // Should contain our enabled rule
        boolean foundEnabledRule = enabledRules.stream()
                .anyMatch(rule -> "Enabled Rule".equals(rule.getAlertName()));
        Assertions.assertTrue(foundEnabledRule);
        // Should not contain our disabled rule
        boolean foundDisabledRule = enabledRules.stream()
                .anyMatch(rule -> "Disabled Rule".equals(rule.getAlertName()));
        Assertions.assertFalse(foundDisabledRule);
    }
}