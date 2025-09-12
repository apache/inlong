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

import org.apache.inlong.manager.common.enums.NotifyType;
import org.apache.inlong.manager.dao.DaoBaseTest;
import org.apache.inlong.manager.dao.entity.AuditAlertRuleEntity;
import org.apache.inlong.manager.pojo.audit.AuditAlertCondition;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
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

    private static final ObjectMapper objectMapper = new ObjectMapper();

    private AuditAlertRuleEntity createTestEntity() {
        AuditAlertRuleEntity entity = new AuditAlertRuleEntity();
        entity.setInlongGroupId("test_group_mapper");
        entity.setInlongStreamId("test_stream_mapper");
        entity.setAuditId("3");
        entity.setAlertName("Mapper Test Alert");
        // Convert AuditAlertCondition object to JSON string for entity
        try {
            AuditAlertCondition condition = new AuditAlertCondition();
            condition.setType("data_loss");
            condition.setOperator(">");
            condition.setValue(10);
            entity.setCondition(objectMapper.writeValueAsString(condition));
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Failed to serialize condition", e);
        }
        entity.setLevel("WARN");
        entity.setNotifyType(NotifyType.EMAIL.name());
        entity.setReceivers("mapper@test.com");
        entity.setEnabled(true);
        entity.setIsDeleted(0); // Initialize isDeleted field
        entity.setCreator(ADMIN);
        entity.setModifier(ADMIN);
        entity.setCreateTime(new Date());
        entity.setModifyTime(new Date());
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
        // Verify isDeleted is properly set
        Assertions.assertEquals(0, entity.getIsDeleted().intValue());
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
    public void testUpdateById() {
        // Insert test data
        AuditAlertRuleEntity entity = createTestEntity();
        auditAlertRuleMapper.insert(entity);

        // Retrieve the entity to get the current version
        AuditAlertRuleEntity retrieved = auditAlertRuleMapper.selectById(entity.getId());

        // Store the original version
        Integer originalVersion = retrieved.getVersion();

        // Update entity
        retrieved.setAlertName("Updated Alert Name");
        retrieved.setLevel("ERROR");
        retrieved.setEnabled(false);
        retrieved.setReceivers("updated@test.com");
        retrieved.setModifier("updated_user");
        // Version will be automatically incremented by the mapper

        int result = auditAlertRuleMapper.updateById(retrieved);

        Assertions.assertEquals(1, result);

        // Verify update
        AuditAlertRuleEntity updated = auditAlertRuleMapper.selectById(retrieved.getId());
        Assertions.assertNotNull(updated);
        Assertions.assertEquals("Updated Alert Name", updated.getAlertName());
        Assertions.assertEquals("ERROR", updated.getLevel());
        Assertions.assertFalse(updated.getEnabled());
        Assertions.assertEquals("updated@test.com", updated.getReceivers());
        Assertions.assertEquals("updated_user", updated.getModifier());
        // Verify version is incremented
        Assertions.assertEquals(originalVersion + 1, updated.getVersion().intValue());
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

        // Verify entity exists and is not marked as deleted
        AuditAlertRuleEntity retrieved = auditAlertRuleMapper.selectById(entityId);
        Assertions.assertNotNull(retrieved);
        Assertions.assertEquals(0, retrieved.getIsDeleted().intValue());

        // Delete entity (soft delete)
        int result = auditAlertRuleMapper.deleteById(entityId);

        Assertions.assertEquals(1, result);

        // Verify entity is marked as deleted and not returned by selectById
        AuditAlertRuleEntity deleted = auditAlertRuleMapper.selectById(entityId);
        Assertions.assertNull(deleted);

        // But it still exists in database with is_deleted = 1
        // We would need a special method to retrieve deleted entities for full verification
    }

    @Test
    public void testDeleteByIdNotFound() {
        // Test delete with non-existent id
        int result = auditAlertRuleMapper.deleteById(99999);

        Assertions.assertEquals(0, result);
    }

    @Test
    public void testSoftDeleteBehavior() {
        // Insert test data
        AuditAlertRuleEntity entity = createTestEntity();
        auditAlertRuleMapper.insert(entity);
        Integer entityId = entity.getId();

        // Verify entity exists and is not marked as deleted
        AuditAlertRuleEntity retrieved = auditAlertRuleMapper.selectById(entityId);
        Assertions.assertNotNull(retrieved);
        Assertions.assertEquals(0, retrieved.getIsDeleted().intValue());

        // Delete entity (soft delete)
        int result = auditAlertRuleMapper.deleteById(entityId);
        Assertions.assertEquals(1, result);

        // Verify entity is marked as deleted and not returned by selectById
        AuditAlertRuleEntity deleted = auditAlertRuleMapper.selectById(entityId);
        Assertions.assertNull(deleted);

        // Test that deleted entities are not returned by selectByCondition
        AuditAlertRuleEntity condition = new AuditAlertRuleEntity();
        condition.setInlongGroupId("test_group_mapper");
        List<AuditAlertRuleEntity> results = auditAlertRuleMapper.selectByCondition(condition);
        Assertions.assertNotNull(results);
        boolean foundDeleted = results.stream().anyMatch(e -> e.getId().equals(entityId));
        Assertions.assertFalse(foundDeleted, "Deleted entity should not appear in selectByCondition results");
    }

    @Test
    public void testSelectByConditionWithId() {
        // Insert test data
        AuditAlertRuleEntity entity = createTestEntity();
        entity.setInlongGroupId("condition_test");
        entity.setAlertName("Condition Test Alert");
        auditAlertRuleMapper.insert(entity);

        // Test select by id condition
        AuditAlertRuleEntity condition = new AuditAlertRuleEntity();
        condition.setId(entity.getId());

        List<AuditAlertRuleEntity> results = auditAlertRuleMapper.selectByCondition(condition);

        Assertions.assertNotNull(results);
        Assertions.assertEquals(1, results.size());
        Assertions.assertEquals(entity.getId(), results.get(0).getId());
        Assertions.assertEquals("Condition Test Alert", results.get(0).getAlertName());
    }

    @Test
    public void testSelectByConditionWithGroupId() {
        // Insert test data
        AuditAlertRuleEntity entity1 = createTestEntity();
        entity1.setInlongGroupId("condition_group_1");
        entity1.setAlertName("Alert 1");
        auditAlertRuleMapper.insert(entity1);

        AuditAlertRuleEntity entity2 = createTestEntity();
        entity2.setInlongGroupId("condition_group_1");
        entity2.setAlertName("Alert 2");
        auditAlertRuleMapper.insert(entity2);

        AuditAlertRuleEntity entity3 = createTestEntity();
        entity3.setInlongGroupId("condition_group_2");
        entity3.setAlertName("Alert 3");
        auditAlertRuleMapper.insert(entity3);

        // Test select by group id condition
        AuditAlertRuleEntity condition = new AuditAlertRuleEntity();
        condition.setInlongGroupId("condition_group_1");

        List<AuditAlertRuleEntity> results = auditAlertRuleMapper.selectByCondition(condition);

        Assertions.assertNotNull(results);
        Assertions.assertEquals(2, results.size());
        for (AuditAlertRuleEntity result : results) {
            Assertions.assertEquals("condition_group_1", result.getInlongGroupId());
        }
    }

    @Test
    public void testSelectByConditionWithStreamId() {
        // Insert test data
        AuditAlertRuleEntity entity1 = createTestEntity();
        entity1.setInlongStreamId("condition_stream_1");
        entity1.setAlertName("Stream Alert 1");
        auditAlertRuleMapper.insert(entity1);

        AuditAlertRuleEntity entity2 = createTestEntity();
        entity2.setInlongStreamId("condition_stream_2");
        entity2.setAlertName("Stream Alert 2");
        auditAlertRuleMapper.insert(entity2);

        // Test select by stream id condition
        AuditAlertRuleEntity condition = new AuditAlertRuleEntity();
        condition.setInlongStreamId("condition_stream_1");

        List<AuditAlertRuleEntity> results = auditAlertRuleMapper.selectByCondition(condition);

        Assertions.assertNotNull(results);
        Assertions.assertEquals(1, results.size());
        Assertions.assertEquals("condition_stream_1", results.get(0).getInlongStreamId());
        Assertions.assertEquals("Stream Alert 1", results.get(0).getAlertName());
    }

    @Test
    public void testSelectByConditionWithAlertName() {
        // Insert test data
        AuditAlertRuleEntity entity1 = createTestEntity();
        entity1.setAlertName("Critical Alert");
        auditAlertRuleMapper.insert(entity1);

        AuditAlertRuleEntity entity2 = createTestEntity();
        entity2.setAlertName("Warning Alert");
        auditAlertRuleMapper.insert(entity2);

        AuditAlertRuleEntity entity3 = createTestEntity();
        entity3.setAlertName("Critical System Alert");
        auditAlertRuleMapper.insert(entity3);

        // Test select by alert name condition (fuzzy match)
        AuditAlertRuleEntity condition = new AuditAlertRuleEntity();
        condition.setAlertName("Critical");

        List<AuditAlertRuleEntity> results = auditAlertRuleMapper.selectByCondition(condition);

        Assertions.assertNotNull(results);
        Assertions.assertEquals(2, results.size());
        for (AuditAlertRuleEntity result : results) {
            Assertions.assertTrue(result.getAlertName().contains("Critical"));
        }
    }

    @Test
    public void testSelectByConditionWithLevel() {
        // Insert test data
        AuditAlertRuleEntity entity1 = createTestEntity();
        entity1.setLevel("ERROR");
        entity1.setAlertName("Error Alert");
        auditAlertRuleMapper.insert(entity1);

        AuditAlertRuleEntity entity2 = createTestEntity();
        entity2.setLevel("WARN");
        entity2.setAlertName("Warning Alert");
        auditAlertRuleMapper.insert(entity2);

        AuditAlertRuleEntity entity3 = createTestEntity();
        entity3.setLevel("ERROR");
        entity3.setAlertName("Another Error Alert");
        auditAlertRuleMapper.insert(entity3);

        // Test select by level condition
        AuditAlertRuleEntity condition = new AuditAlertRuleEntity();
        condition.setLevel("ERROR");

        List<AuditAlertRuleEntity> results = auditAlertRuleMapper.selectByCondition(condition);

        Assertions.assertNotNull(results);
        Assertions.assertEquals(2, results.size());
        for (AuditAlertRuleEntity result : results) {
            Assertions.assertEquals("ERROR", result.getLevel());
        }
    }

    @Test
    public void testSelectByConditionWithEnabled() {
        // Insert test data
        AuditAlertRuleEntity entity1 = createTestEntity();
        entity1.setEnabled(true);
        entity1.setAlertName("Enabled Alert");
        auditAlertRuleMapper.insert(entity1);

        AuditAlertRuleEntity entity2 = createTestEntity();
        entity2.setEnabled(false);
        entity2.setAlertName("Disabled Alert");
        auditAlertRuleMapper.insert(entity2);

        AuditAlertRuleEntity entity3 = createTestEntity();
        entity3.setEnabled(true);
        entity3.setAlertName("Another Enabled Alert");
        auditAlertRuleMapper.insert(entity3);

        // Test select by enabled condition
        AuditAlertRuleEntity condition = new AuditAlertRuleEntity();
        condition.setEnabled(true);

        List<AuditAlertRuleEntity> results = auditAlertRuleMapper.selectByCondition(condition);

        Assertions.assertNotNull(results);
        for (AuditAlertRuleEntity result : results) {
            Assertions.assertTrue(result.getEnabled());
        }

        // Verify our enabled alerts are in the results
        boolean foundEnabledAlert = results.stream()
                .anyMatch(entity -> "Enabled Alert".equals(entity.getAlertName()));
        Assertions.assertTrue(foundEnabledAlert);

        boolean foundAnotherEnabledAlert = results.stream()
                .anyMatch(entity -> "Another Enabled Alert".equals(entity.getAlertName()));
        Assertions.assertTrue(foundAnotherEnabledAlert);
    }

    @Test
    public void testSelectByConditionWithMultipleConditions() {
        // Insert test data
        AuditAlertRuleEntity entity1 = createTestEntity();
        entity1.setInlongGroupId("multi_test");
        entity1.setLevel("ERROR");
        entity1.setEnabled(true);
        entity1.setAlertName("Multi Condition Alert");
        auditAlertRuleMapper.insert(entity1);

        AuditAlertRuleEntity entity2 = createTestEntity();
        entity2.setInlongGroupId("multi_test");
        entity2.setLevel("WARN");
        entity2.setEnabled(true);
        entity2.setAlertName("Different Level Alert");
        auditAlertRuleMapper.insert(entity2);

        AuditAlertRuleEntity entity3 = createTestEntity();
        entity3.setInlongGroupId("multi_test");
        entity3.setLevel("ERROR");
        entity3.setEnabled(false);
        entity3.setAlertName("Disabled Error Alert");
        auditAlertRuleMapper.insert(entity3);

        // Test select with multiple conditions
        AuditAlertRuleEntity condition = new AuditAlertRuleEntity();
        condition.setInlongGroupId("multi_test");
        condition.setLevel("ERROR");
        condition.setEnabled(true);

        List<AuditAlertRuleEntity> results = auditAlertRuleMapper.selectByCondition(condition);

        Assertions.assertNotNull(results);
        Assertions.assertEquals(1, results.size());
        Assertions.assertEquals("multi_test", results.get(0).getInlongGroupId());
        Assertions.assertEquals("ERROR", results.get(0).getLevel());
        Assertions.assertTrue(results.get(0).getEnabled());
        Assertions.assertEquals("Multi Condition Alert", results.get(0).getAlertName());
    }

    @Test
    public void testSelectByConditionWithCreator() {
        // Insert test data
        AuditAlertRuleEntity entity1 = createTestEntity();
        entity1.setCreator("test_user_1");
        entity1.setAlertName("User 1 Alert");
        auditAlertRuleMapper.insert(entity1);

        AuditAlertRuleEntity entity2 = createTestEntity();
        entity2.setCreator("test_user_2");
        entity2.setAlertName("User 2 Alert");
        auditAlertRuleMapper.insert(entity2);

        // Test select by creator condition
        AuditAlertRuleEntity condition = new AuditAlertRuleEntity();
        condition.setCreator("test_user_1");

        List<AuditAlertRuleEntity> results = auditAlertRuleMapper.selectByCondition(condition);

        Assertions.assertNotNull(results);
        Assertions.assertEquals(1, results.size());
        Assertions.assertEquals("test_user_1", results.get(0).getCreator());
        Assertions.assertEquals("User 1 Alert", results.get(0).getAlertName());
    }

    @Test
    public void testSelectByConditionWithEmptyCondition() {
        // Insert test data
        AuditAlertRuleEntity entity1 = createTestEntity();
        entity1.setAlertName("Empty Condition Test 1");
        auditAlertRuleMapper.insert(entity1);

        AuditAlertRuleEntity entity2 = createTestEntity();
        entity2.setAlertName("Empty Condition Test 2");
        auditAlertRuleMapper.insert(entity2);

        // Test select with empty condition (should return all non-deleted records)
        AuditAlertRuleEntity condition = new AuditAlertRuleEntity();

        List<AuditAlertRuleEntity> results = auditAlertRuleMapper.selectByCondition(condition);

        Assertions.assertNotNull(results);
        Assertions.assertTrue(results.size() >= 2);

        // Verify our test entities are in the results
        boolean foundTest1 = results.stream()
                .anyMatch(entity -> "Empty Condition Test 1".equals(entity.getAlertName()));
        Assertions.assertTrue(foundTest1);

        boolean foundTest2 = results.stream()
                .anyMatch(entity -> "Empty Condition Test 2".equals(entity.getAlertName()));
        Assertions.assertTrue(foundTest2);
    }

    @Test
    public void testSelectByConditionExcludesDeletedRecords() {
        // Insert test data
        AuditAlertRuleEntity entity = createTestEntity();
        entity.setAlertName("Deleted Test Alert");
        auditAlertRuleMapper.insert(entity);
        Integer entityId = entity.getId();

        // Verify entity exists
        List<AuditAlertRuleEntity> results = auditAlertRuleMapper.selectByCondition(new AuditAlertRuleEntity());
        boolean foundBeforeDelete = results.stream()
                .anyMatch(e -> e.getId().equals(entityId));
        Assertions.assertTrue(foundBeforeDelete);

        // Delete entity (soft delete)
        auditAlertRuleMapper.deleteById(entityId);

        // Test that deleted entity is not returned by selectByCondition
        results = auditAlertRuleMapper.selectByCondition(new AuditAlertRuleEntity());
        boolean foundAfterDelete = results.stream()
                .anyMatch(e -> e.getId().equals(entityId));
        Assertions.assertFalse(foundAfterDelete, "Deleted entity should not appear in selectByCondition results");
    }

    @Test
    public void testSelectByConditionOrdering() {
        // Insert multiple entities with different create times
        AuditAlertRuleEntity entity1 = createTestEntity();
        entity1.setInlongGroupId("order_condition_test");
        entity1.setAlertName("First Condition Alert");
        auditAlertRuleMapper.insert(entity1);

        // Wait a moment to ensure different timestamps
        try {
            Thread.sleep(10);
        } catch (InterruptedException e) {
            // Ignore
        }

        AuditAlertRuleEntity entity2 = createTestEntity();
        entity2.setInlongGroupId("order_condition_test");
        entity2.setAlertName("Second Condition Alert");
        auditAlertRuleMapper.insert(entity2);

        // Test ordering (should be desc by create_time)
        AuditAlertRuleEntity condition = new AuditAlertRuleEntity();
        condition.setInlongGroupId("order_condition_test");

        List<AuditAlertRuleEntity> results = auditAlertRuleMapper.selectByCondition(condition);

        Assertions.assertNotNull(results);
        Assertions.assertEquals(2, results.size());

        // First result should be the latest (Second Condition Alert)
        Assertions.assertEquals("Second Condition Alert", results.get(0).getAlertName());
        Assertions.assertEquals("First Condition Alert", results.get(1).getAlertName());
    }
}