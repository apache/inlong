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

package org.apache.inlong.manager.dao.mapper;

import org.apache.inlong.manager.dao.DaoBaseTest;
import org.apache.inlong.manager.dao.entity.AuditAlertRuleEntity;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.Date;
import java.util.List;

/**
 * Test cases for {@link AuditAlertRuleEntityMapper}
 */
public class AuditAlertRuleEntityMapperTest extends DaoBaseTest {

    @Autowired
    private AuditAlertRuleEntityMapper auditAlertRuleMapper;

    private AuditAlertRuleEntity createTestEntity() {
        AuditAlertRuleEntity entity = new AuditAlertRuleEntity();
        entity.setInlongGroupId("test_group_mapper");
        entity.setInlongStreamId("test_stream_mapper");
        entity.setAuditId("3");
        entity.setAlertName("Mapper Test Alert");
        entity.setCondition("{\"type\": \"data_loss\", \"operator\": \">\", \"value\": 10}");
        entity.setLevel("WARN");
        entity.setNotifyType("EMAIL");
        entity.setReceivers("mapper@test.com");
        entity.setEnabled(true);
        entity.setCreator(ADMIN);
        entity.setModifier(ADMIN);
        entity.setCreateTime(new Date());
        entity.setUpdateTime(new Date());
        return entity;
    }

    @Test
    public void testInsert() {
        // Test insert operation
        AuditAlertRuleEntity entity = createTestEntity();

        int result = auditAlertRuleMapper.insert(entity);

        Assertions.assertEquals(1, result);
        Assertions.assertNotNull(entity.getId());
        Assertions.assertTrue(entity.getId() > 0);
    }

    @Test
    public void testSelectById() {
        // Insert test data
        AuditAlertRuleEntity entity = createTestEntity();
        auditAlertRuleMapper.insert(entity);

        // Test select by id
        AuditAlertRuleEntity retrieved = auditAlertRuleMapper.selectById(entity.getId());

        Assertions.assertNotNull(retrieved);
        Assertions.assertEquals(entity.getId(), retrieved.getId());
        Assertions.assertEquals("test_group_mapper", retrieved.getInlongGroupId());
        Assertions.assertEquals("test_stream_mapper", retrieved.getInlongStreamId());
        Assertions.assertEquals("Mapper Test Alert", retrieved.getAlertName());
        Assertions.assertEquals("WARN", retrieved.getLevel());
        Assertions.assertTrue(retrieved.getEnabled());
    }

    @Test
    public void testSelectByIdNotFound() {
        // Test select with non-existent id
        AuditAlertRuleEntity retrieved = auditAlertRuleMapper.selectById(99999);

        Assertions.assertNull(retrieved);
    }

    @Test
    public void testSelectByGroupAndStream() {
        // Insert test data with specific group and stream
        AuditAlertRuleEntity entity1 = createTestEntity();
        entity1.setInlongGroupId("group_1");
        entity1.setInlongStreamId("stream_1");
        auditAlertRuleMapper.insert(entity1);

        AuditAlertRuleEntity entity2 = createTestEntity();
        entity2.setInlongGroupId("group_1");
        entity2.setInlongStreamId("stream_2");
        auditAlertRuleMapper.insert(entity2);

        AuditAlertRuleEntity entity3 = createTestEntity();
        entity3.setInlongGroupId("group_2");
        entity3.setInlongStreamId("stream_1");
        auditAlertRuleMapper.insert(entity3);

        // Test select by group and stream
        List<AuditAlertRuleEntity> results = auditAlertRuleMapper.selectByGroupAndStream("group_1", "stream_1");

        Assertions.assertNotNull(results);
        Assertions.assertEquals(1, results.size());
        Assertions.assertEquals("group_1", results.get(0).getInlongGroupId());
        Assertions.assertEquals("stream_1", results.get(0).getInlongStreamId());
    }

    @Test
    public void testSelectByGroupOnly() {
        // Insert test data
        AuditAlertRuleEntity entity1 = createTestEntity();
        entity1.setInlongGroupId("group_test");
        entity1.setInlongStreamId("stream_1");
        auditAlertRuleMapper.insert(entity1);

        AuditAlertRuleEntity entity2 = createTestEntity();
        entity2.setInlongGroupId("group_test");
        entity2.setInlongStreamId("stream_2");
        auditAlertRuleMapper.insert(entity2);

        // Test select by group only (stream = null)
        List<AuditAlertRuleEntity> results = auditAlertRuleMapper.selectByGroupAndStream("group_test", null);

        Assertions.assertNotNull(results);
        Assertions.assertEquals(2, results.size());
        for (AuditAlertRuleEntity entity : results) {
            Assertions.assertEquals("group_test", entity.getInlongGroupId());
        }
    }

    @Test
    public void testSelectByGroupAndStreamWithNullParameters() {
        // Insert test data
        AuditAlertRuleEntity entity = createTestEntity();
        auditAlertRuleMapper.insert(entity);

        // Test select with null parameters (should return all)
        List<AuditAlertRuleEntity> results = auditAlertRuleMapper.selectByGroupAndStream(null, null);

        Assertions.assertNotNull(results);
        Assertions.assertTrue(results.size() >= 1);
    }

    @Test
    public void testSelectEnabledRules() {
        // Insert enabled rule
        AuditAlertRuleEntity enabledEntity = createTestEntity();
        enabledEntity.setEnabled(true);
        enabledEntity.setAlertName("Enabled Rule");
        auditAlertRuleMapper.insert(enabledEntity);

        // Insert disabled rule
        AuditAlertRuleEntity disabledEntity = createTestEntity();
        disabledEntity.setEnabled(false);
        disabledEntity.setAlertName("Disabled Rule");
        auditAlertRuleMapper.insert(disabledEntity);

        // Test select enabled rules
        List<AuditAlertRuleEntity> results = auditAlertRuleMapper.selectEnabledRules();

        Assertions.assertNotNull(results);
        for (AuditAlertRuleEntity entity : results) {
            Assertions.assertTrue(entity.getEnabled());
        }

        // Verify our enabled rule is in the results
        boolean foundEnabledRule = results.stream()
                .anyMatch(entity -> "Enabled Rule".equals(entity.getAlertName()));
        Assertions.assertTrue(foundEnabledRule);
    }

    @Test
    public void testUpdateById() {
        // Insert test data
        AuditAlertRuleEntity entity = createTestEntity();
        auditAlertRuleMapper.insert(entity);

        // Update entity
        entity.setAlertName("Updated Alert Name");
        entity.setLevel("ERROR");
        entity.setEnabled(false);
        entity.setReceivers("updated@test.com");
        entity.setModifier("updated_user");

        int result = auditAlertRuleMapper.updateById(entity);

        Assertions.assertEquals(1, result);

        // Verify update
        AuditAlertRuleEntity updated = auditAlertRuleMapper.selectById(entity.getId());
        Assertions.assertNotNull(updated);
        Assertions.assertEquals("Updated Alert Name", updated.getAlertName());
        Assertions.assertEquals("ERROR", updated.getLevel());
        Assertions.assertFalse(updated.getEnabled());
        Assertions.assertEquals("updated@test.com", updated.getReceivers());
        Assertions.assertEquals("updated_user", updated.getModifier());
    }

    @Test
    public void testUpdateByIdNotFound() {
        // Test update with non-existent id
        AuditAlertRuleEntity entity = createTestEntity();
        entity.setId(99999);

        int result = auditAlertRuleMapper.updateById(entity);

        Assertions.assertEquals(0, result);
    }

    @Test
    public void testDeleteById() {
        // Insert test data
        AuditAlertRuleEntity entity = createTestEntity();
        auditAlertRuleMapper.insert(entity);
        Integer entityId = entity.getId();

        // Verify entity exists
        Assertions.assertNotNull(auditAlertRuleMapper.selectById(entityId));

        // Delete entity
        int result = auditAlertRuleMapper.deleteById(entityId);

        Assertions.assertEquals(1, result);

        // Verify entity is deleted
        Assertions.assertNull(auditAlertRuleMapper.selectById(entityId));
    }

    @Test
    public void testDeleteByIdNotFound() {
        // Test delete with non-existent id
        int result = auditAlertRuleMapper.deleteById(99999);

        Assertions.assertEquals(0, result);
    }

    @Test
    public void testOrderingInSelectByGroupAndStream() {
        // Insert multiple entities with different create times
        AuditAlertRuleEntity entity1 = createTestEntity();
        entity1.setInlongGroupId("order_test");
        entity1.setAlertName("First Alert");
        auditAlertRuleMapper.insert(entity1);

        // Wait a moment to ensure different timestamps
        try {
            Thread.sleep(10);
        } catch (InterruptedException e) {
            // Ignore
        }

        AuditAlertRuleEntity entity2 = createTestEntity();
        entity2.setInlongGroupId("order_test");
        entity2.setAlertName("Second Alert");
        auditAlertRuleMapper.insert(entity2);

        // Test ordering (should be desc by create_time)
        List<AuditAlertRuleEntity> results = auditAlertRuleMapper.selectByGroupAndStream("order_test", null);

        Assertions.assertNotNull(results);
        Assertions.assertEquals(2, results.size());

        // First result should be the latest (Second Alert)
        Assertions.assertEquals("Second Alert", results.get(0).getAlertName());
        Assertions.assertEquals("First Alert", results.get(1).getAlertName());
    }

    @Test
    public void testEmptyStringParametersInSelectByGroupAndStream() {
        // Insert test data
        AuditAlertRuleEntity entity = createTestEntity();
        entity.setInlongGroupId("empty_test");
        auditAlertRuleMapper.insert(entity);

        // Test with empty strings (should be treated as null)
        List<AuditAlertRuleEntity> results = auditAlertRuleMapper.selectByGroupAndStream("", "");

        Assertions.assertNotNull(results);
        // Should return all records when empty strings are passed
    }
}