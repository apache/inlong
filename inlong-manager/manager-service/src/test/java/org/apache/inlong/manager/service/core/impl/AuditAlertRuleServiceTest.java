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

import org.apache.inlong.manager.common.enums.NotifyType;
import org.apache.inlong.manager.common.exceptions.BusinessException;
import org.apache.inlong.manager.dao.entity.AuditAlertRuleEntity;
import org.apache.inlong.manager.dao.mapper.AuditAlertRuleEntityMapper;
import org.apache.inlong.manager.pojo.audit.AuditAlertCondition;
import org.apache.inlong.manager.pojo.audit.AuditAlertRule;
import org.apache.inlong.manager.pojo.audit.AuditAlertRulePageRequest;
import org.apache.inlong.manager.pojo.audit.AuditAlertRuleRequest;
import org.apache.inlong.manager.pojo.common.PageResult;
import org.apache.inlong.manager.test.BaseTest;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.test.context.SpringBootTest;

import java.util.Date;

/**
 * Audit alert rule service test without mock.
 */
@SpringBootApplication
@SpringBootTest(classes = {AuditAlertRuleServiceTest.class, AuditAlertRuleServiceImpl.class,
        AuditAlertRuleEntityMapper.class})
class AuditAlertRuleServiceTest extends BaseTest {

    private static final String GLOBAL_OPERATOR = "admin";
    private static final Integer TEST_RULE_ID = 1;
    private static final String TEST_GROUP_ID = "test_group";
    private static final String TEST_STREAM_ID = "test_stream";
    private static final String TEST_AUDIT_ID = "audit_id_1";
    private static final String TEST_ALERT_NAME = "Test Alert Rule";
    private static final String TEST_LEVEL = "HIGH";
    private static final String TEST_RECEIVERS = "test@example.com";

    @Autowired
    private AuditAlertRuleServiceImpl auditAlertRuleService;

    @Autowired
    private AuditAlertRuleEntityMapper alertRuleMapper;

    private final ObjectMapper objectMapper = new ObjectMapper();

    @BeforeEach
    public void setUp() {
        // Clean up any existing data before each test
        if (alertRuleMapper.selectById(TEST_RULE_ID) != null) {
            alertRuleMapper.deleteById(TEST_RULE_ID);
        }
    }

    /**
     * Create a test AuditAlertRuleRequest
     */
    private AuditAlertRuleRequest createTestRequest() {
        AuditAlertRuleRequest request = new AuditAlertRuleRequest();
        request.setInlongGroupId(TEST_GROUP_ID);
        request.setInlongStreamId(TEST_STREAM_ID);
        request.setAuditId(TEST_AUDIT_ID);
        request.setAlertName(TEST_ALERT_NAME);
        request.setLevel(TEST_LEVEL);
        request.setReceivers(TEST_RECEIVERS);
        request.setEnabled(true);
        request.setNotifyType(NotifyType.EMAIL);

        AuditAlertCondition condition = new AuditAlertCondition();
        condition.setType("data_loss");
        condition.setOperator(">");
        condition.setValue(100.0);
        request.setCondition(condition);

        return request;
    }

    /**
     * Create a test AuditAlertRuleEntity
     */
    private AuditAlertRuleEntity createTestEntity() {
        AuditAlertRuleEntity entity = new AuditAlertRuleEntity();
        entity.setId(TEST_RULE_ID);
        entity.setInlongGroupId(TEST_GROUP_ID);
        entity.setInlongStreamId(TEST_STREAM_ID);
        entity.setAuditId(TEST_AUDIT_ID);
        entity.setAlertName(TEST_ALERT_NAME);
        entity.setLevel(TEST_LEVEL);
        entity.setReceivers(TEST_RECEIVERS);
        entity.setEnabled(true);
        entity.setNotifyType(NotifyType.EMAIL.name());
        entity.setCreator(GLOBAL_OPERATOR);
        entity.setModifier(GLOBAL_OPERATOR);
        entity.setCreateTime(new Date());
        entity.setModifyTime(new Date());
        entity.setIsDeleted(0);
        entity.setVersion(1);

        AuditAlertCondition condition = new AuditAlertCondition();
        condition.setType("data_loss");
        condition.setOperator(">");
        condition.setValue(100.0);

        try {
            entity.setCondition(objectMapper.writeValueAsString(condition));
        } catch (JsonProcessingException e) {
            // Ignore for test
        }

        return entity;
    }

    @Test
    void testCreateSuccess() {
        // Prepare test data
        AuditAlertRuleRequest request = createTestRequest();

        // Execute the method under test
        Integer result = auditAlertRuleService.create(request, GLOBAL_OPERATOR);

        // Verify the result
        Assertions.assertNotNull(result);
        Assertions.assertTrue(result > 0);

        // Verify the entity was actually inserted into the database
        AuditAlertRuleEntity insertedEntity = alertRuleMapper.selectById(result);
        Assertions.assertNotNull(insertedEntity);
        Assertions.assertEquals(TEST_GROUP_ID, insertedEntity.getInlongGroupId());
        Assertions.assertEquals(TEST_STREAM_ID, insertedEntity.getInlongStreamId());
        Assertions.assertEquals(TEST_AUDIT_ID, insertedEntity.getAuditId());
        Assertions.assertEquals(TEST_ALERT_NAME, insertedEntity.getAlertName());
        Assertions.assertEquals(TEST_LEVEL, insertedEntity.getLevel());
        Assertions.assertEquals(TEST_RECEIVERS, insertedEntity.getReceivers());
        Assertions.assertEquals(NotifyType.EMAIL.name(), insertedEntity.getNotifyType());
        Assertions.assertEquals(GLOBAL_OPERATOR, insertedEntity.getCreator());
        Assertions.assertEquals(GLOBAL_OPERATOR, insertedEntity.getModifier());
        Assertions.assertNotNull(insertedEntity.getCondition());
    }

    @Test
    void testCreateWithNullRequest() {
        // Execute and verify exception
        BusinessException exception = Assertions.assertThrows(BusinessException.class, () -> {
            auditAlertRuleService.create(null, GLOBAL_OPERATOR);
        });

        // Verify the exception message
        Assertions.assertEquals("request cannot be null", exception.getMessage());
    }

    @Test
    void testGetSuccess() {
        // Prepare test data - insert an entity first
        AuditAlertRuleEntity entity = createTestEntity();
        alertRuleMapper.insert(entity);

        // Get the actual ID of the inserted entity
        AuditAlertRuleEntity insertedEntity = alertRuleMapper.selectById(entity.getId());

        // Execute the method under test
        AuditAlertRule result = auditAlertRuleService.get(insertedEntity.getId());

        // Verify the result
        Assertions.assertNotNull(result);
        Assertions.assertEquals(insertedEntity.getId(), result.getId());
        Assertions.assertEquals(TEST_ALERT_NAME, result.getAlertName());
        Assertions.assertEquals(TEST_LEVEL, result.getLevel());
        Assertions.assertEquals(TEST_RECEIVERS, result.getReceivers());
        Assertions.assertEquals(NotifyType.EMAIL, result.getNotifyType());
        Assertions.assertNotNull(result.getCondition());
        Assertions.assertEquals("data_loss", result.getCondition().getType());
    }

    @Test
    void testGetWithNullId() {
        // Execute and verify exception
        BusinessException exception = Assertions.assertThrows(BusinessException.class, () -> {
            auditAlertRuleService.get(null);
        });

        // Verify the exception message
        Assertions.assertEquals("id cannot be null", exception.getMessage());
    }

    @Test
    void testGetWithNonExistentId() {
        // Execute and verify exception
        BusinessException exception = Assertions.assertThrows(BusinessException.class, () -> {
            auditAlertRuleService.get(999999);
        });

        // Verify the exception message
        Assertions.assertEquals("Audit alert rule not found with id: 999999", exception.getMessage());
    }

    @Test
    void testUpdateSuccess() {
        // Prepare test data - insert an entity first
        AuditAlertRuleEntity existingEntity = createTestEntity();
        int insertResult = alertRuleMapper.insert(existingEntity);
        Assertions.assertTrue(insertResult > 0);

        // Get the inserted entity to have the correct ID
        AuditAlertRuleEntity insertedEntity = alertRuleMapper.selectById(existingEntity.getId());
        Assertions.assertNotNull(insertedEntity);

        // Create update request
        AuditAlertRuleRequest request = createTestRequest();
        request.setId(insertedEntity.getId());
        request.setVersion(insertedEntity.getVersion());
        request.setAlertName("Updated Alert Name");

        // Execute the method under test
        Boolean result = auditAlertRuleService.update(request, GLOBAL_OPERATOR);

        // Verify the result
        Assertions.assertTrue(result);

        // Verify the entity was actually updated in the database
        AuditAlertRuleEntity updatedEntity = alertRuleMapper.selectById(insertedEntity.getId());
        Assertions.assertNotNull(updatedEntity);
        Assertions.assertEquals("Updated Alert Name", updatedEntity.getAlertName());
        Assertions.assertEquals(GLOBAL_OPERATOR, updatedEntity.getModifier());
        Assertions.assertEquals(2, updatedEntity.getVersion().intValue()); // Version should be incremented
    }

    @Test
    void testUpdateWithNullRequest() {
        // Execute and verify exception
        BusinessException exception = Assertions.assertThrows(BusinessException.class, () -> {
            auditAlertRuleService.update(null, GLOBAL_OPERATOR);
        });

        // Verify the exception message
        Assertions.assertEquals("request cannot be null", exception.getMessage());
    }

    @Test
    void testUpdateWithNullId() {
        // Prepare test data
        AuditAlertRuleRequest request = createTestRequest();
        request.setId(null); // ID is null

        // Execute and verify exception
        BusinessException exception = Assertions.assertThrows(BusinessException.class, () -> {
            auditAlertRuleService.update(request, GLOBAL_OPERATOR);
        });

        // Verify the exception message
        Assertions.assertEquals("rule id cannot be null", exception.getMessage());
    }

    @Test
    void testUpdateWithNonExistentId() {
        // Prepare test data
        AuditAlertRuleRequest request = createTestRequest();
        request.setId(999999);

        // Execute and verify exception
        BusinessException exception = Assertions.assertThrows(BusinessException.class, () -> {
            auditAlertRuleService.update(request, GLOBAL_OPERATOR);
        });

        // Verify the exception message
        Assertions.assertEquals("Audit alert rule not found with id: 999999", exception.getMessage());
    }

    @Test
    void testDeleteSuccess() {
        // Prepare test data - insert an entity first
        AuditAlertRuleEntity existingEntity = createTestEntity();
        int insertResult = alertRuleMapper.insert(existingEntity);
        Assertions.assertTrue(insertResult > 0);

        // Get the inserted entity to have the correct ID
        AuditAlertRuleEntity insertedEntity = alertRuleMapper.selectById(existingEntity.getId());
        Assertions.assertNotNull(insertedEntity);

        // Execute the method under test
        Boolean result = auditAlertRuleService.delete(insertedEntity.getId());

        // Verify the result
        Assertions.assertTrue(result);

        // Verify the entity was marked as deleted in the database
        AuditAlertRuleEntity deletedEntity = alertRuleMapper.selectById(insertedEntity.getId());
        Assertions.assertNull(deletedEntity); // Should not be found as it's marked as deleted
    }

    @Test
    void testDeleteWithNullId() {
        // Execute and verify exception
        BusinessException exception = Assertions.assertThrows(BusinessException.class, () -> {
            auditAlertRuleService.delete(null);
        });

        // Verify the exception message
        Assertions.assertEquals("id cannot be null", exception.getMessage());
    }

    @Test
    void testDeleteWithNonExistentId() {
        // Execute and verify exception
        BusinessException exception = Assertions.assertThrows(BusinessException.class, () -> {
            auditAlertRuleService.delete(999999);
        });

        // Verify the exception message
        Assertions.assertEquals("Audit alert rule not found with id: 999999", exception.getMessage());
    }

    @Test
    void testSelectByConditionSuccess() {
        // Prepare test data - insert an entity first
        AuditAlertRuleEntity entity = createTestEntity();
        int insertResult = alertRuleMapper.insert(entity);
        Assertions.assertTrue(insertResult > 0);

        // Get the inserted entity to have the correct ID
        AuditAlertRuleEntity insertedEntity = alertRuleMapper.selectById(entity.getId());
        Assertions.assertNotNull(insertedEntity);

        // Prepare request
        AuditAlertRulePageRequest request = new AuditAlertRulePageRequest();
        request.setPageNum(1);
        request.setPageSize(10);
        request.setOrderField("id");
        request.setOrderType("asc");

        // Execute the method under test
        PageResult<AuditAlertRule> result = auditAlertRuleService.selectByCondition(request);

        // Verify the result
        Assertions.assertNotNull(result);
        Assertions.assertTrue(result.getList().size() >= 1);
        Assertions.assertTrue(result.getTotal() >= 1);

        // Find our inserted entity in the result list
        boolean found = false;
        for (AuditAlertRule rule : result.getList()) {
            if (rule.getId().equals(insertedEntity.getId())) {
                found = true;
                Assertions.assertEquals(TEST_ALERT_NAME, rule.getAlertName());
                Assertions.assertEquals(TEST_LEVEL, rule.getLevel());
                Assertions.assertEquals(TEST_RECEIVERS, rule.getReceivers());
                Assertions.assertEquals(NotifyType.EMAIL, rule.getNotifyType());
                break;
            }
        }
        Assertions.assertTrue(found, "Inserted entity should be found in the result list");
    }
}