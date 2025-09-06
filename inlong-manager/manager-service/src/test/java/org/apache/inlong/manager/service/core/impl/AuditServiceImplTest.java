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
import org.apache.inlong.manager.pojo.audit.AuditAlertRule;
import org.apache.inlong.manager.service.ServiceBaseTest;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.Date;

/**
 * Test cases for {@link AuditServiceImpl} audit alert rule functionality
 */
class AuditServiceImplTest extends ServiceBaseTest {

    @Autowired
    private AuditServiceImpl auditService;

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
        AuditAlertRule updatedRule = auditService.updateAlertRule(rule, "test_user");

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
}