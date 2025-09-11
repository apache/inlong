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
        AuditAlertRuleRequest request = new AuditAlertRuleRequest();
        request.setId(freshEntity.getId());
        request.setLevel("CRITICAL");
        request.setNotifyType("SMS");
        request.setReceivers("updated_service@test.com");
        request.setEnabled(false);
        request.setVersion(freshEntity.getVersion());

        // Update the rule
        AuditAlertRule updatedRule = auditAlertRuleService.update(request, "test_user");

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

        // Test select by condition for enabled rules (replacing listEnabled)
        AuditAlertRulePageRequest request = new AuditAlertRulePageRequest();
        request.setEnabled(true);
        PageResult<AuditAlertRule> pageResult = auditAlertRuleService.selectByCondition(request);
        List<AuditAlertRule> enabledRules = pageResult.getList();

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
    }

    @Test
    void testSelectByCondition() {
        // Insert test data
        AuditAlertRuleEntity entity1 = insertTestEntity();

        // Create another rule with different group ID
        AuditAlertRuleEntity entity2 = new AuditAlertRuleEntity();
        entity2.setInlongGroupId("test_group_service_select");
        entity2.setInlongStreamId("test_stream_service_select");
        entity2.setAuditId("4");
        entity2.setAlertName("Select Test Rule");
        entity2.setCondition("{\"type\": \"count\", \"operator\": \">\", \"value\": 100}");
        entity2.setLevel("INFO");
        entity2.setNotifyType("EMAIL");
        entity2.setReceivers("select@test.com");
        entity2.setEnabled(true);
        entity2.setIsDeleted(0);
        entity2.setCreator("test_user");
        entity2.setModifier("test_user");
        entity2.setCreateTime(new Date());
        entity2.setModifyTime(new Date());
        entity2.setVersion(1);
        auditAlertRuleMapper.insert(entity2);

        // Test select by condition - filter by group ID
        AuditAlertRulePageRequest request1 = new AuditAlertRulePageRequest();
        request1.setInlongGroupId("test_group_service_select");
        PageResult<AuditAlertRule> pageResult1 = auditAlertRuleService.selectByCondition(request1);
        List<AuditAlertRule> rules1 = pageResult1.getList();
        Assertions.assertNotNull(rules1);
        Assertions.assertEquals(1, rules1.size());
        Assertions.assertEquals("test_group_service_select", rules1.get(0).getInlongGroupId());
        Assertions.assertEquals("Select Test Rule", rules1.get(0).getAlertName());

        // Test select by condition - filter by alert name
        AuditAlertRulePageRequest request2 = new AuditAlertRulePageRequest();
        request2.setAlertName("Service Test Alert");
        PageResult<AuditAlertRule> pageResult2 = auditAlertRuleService.selectByCondition(request2);
        List<AuditAlertRule> rules2 = pageResult2.getList();
        Assertions.assertNotNull(rules2);
        Assertions.assertEquals(1, rules2.size());
        Assertions.assertEquals("test_group_service", rules2.get(0).getInlongGroupId());
        Assertions.assertEquals("Service Test Alert", rules2.get(0).getAlertName());

        // Test select by condition - no filter (should return all non-deleted rules)
        AuditAlertRulePageRequest request3 = new AuditAlertRulePageRequest();
        PageResult<AuditAlertRule> pageResult3 = auditAlertRuleService.selectByCondition(request3);
        List<AuditAlertRule> rules3 = pageResult3.getList();
        Assertions.assertNotNull(rules3);
        // Should have at least the two rules we inserted
        Assertions.assertTrue(rules3.size() >= 2);
        // All rules should not be deleted
        Assertions.assertTrue(rules3.stream().allMatch(rule -> rule.getIsDeleted() == 0));
    }
}