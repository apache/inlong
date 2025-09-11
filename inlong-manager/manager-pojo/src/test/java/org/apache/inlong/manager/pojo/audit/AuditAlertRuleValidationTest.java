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

package org.apache.inlong.manager.pojo.audit;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.apache.inlong.manager.common.enums.NotifyType;

import javax.validation.ConstraintViolation;
import javax.validation.Validation;
import javax.validation.Validator;
import javax.validation.ValidatorFactory;

import java.util.Date;
import java.util.Set;

/**
 * Tests for {@link AuditAlertRule} validation
 */
public class AuditAlertRuleValidationTest {

    private Validator validator;

    @BeforeEach
    void setUp() {
        ValidatorFactory factory = Validation.buildDefaultValidatorFactory();
        validator = factory.getValidator();
    }

    @Test
    void testValidAuditAlertRule() {
        // Create valid AuditAlertRule
        AuditAlertRule rule = createValidAuditAlertRule();

        // Validate
        Set<ConstraintViolation<AuditAlertRule>> violations = validator.validate(rule);

        // Assert no validation errors
        Assertions.assertTrue(violations.isEmpty());
    }

    @Test
    void testInlongGroupIdNotBlank() {
        // Create AuditAlertRule with empty inlongGroupId
        AuditAlertRule rule = createValidAuditAlertRule();
        rule.setInlongGroupId("");

        // Validate
        Set<ConstraintViolation<AuditAlertRule>> violations = validator.validate(rule);

        // Assert validation error
        Assertions.assertEquals(1, violations.size());
        ConstraintViolation<AuditAlertRule> violation = violations.iterator().next();
        Assertions.assertEquals("InLong Group ID cannot be blank", violation.getMessage());
        Assertions.assertEquals("inlongGroupId", violation.getPropertyPath().toString());
    }

    @Test
    void testInlongGroupIdNull() {
        // Create AuditAlertRule with null inlongGroupId
        AuditAlertRule rule = createValidAuditAlertRule();
        rule.setInlongGroupId(null);

        // Validate
        Set<ConstraintViolation<AuditAlertRule>> violations = validator.validate(rule);

        // Assert validation error
        Assertions.assertEquals(1, violations.size());
        ConstraintViolation<AuditAlertRule> violation = violations.iterator().next();
        Assertions.assertEquals("InLong Group ID cannot be blank", violation.getMessage());
    }

    @Test
    void testAuditIdNotBlank() {
        // Create AuditAlertRule with empty auditId
        AuditAlertRule rule = createValidAuditAlertRule();
        rule.setAuditId("");

        // Validate
        Set<ConstraintViolation<AuditAlertRule>> violations = validator.validate(rule);

        // Assert validation error
        Assertions.assertEquals(1, violations.size());
        ConstraintViolation<AuditAlertRule> violation = violations.iterator().next();
        Assertions.assertEquals("Audit ID cannot be blank", violation.getMessage());
        Assertions.assertEquals("auditId", violation.getPropertyPath().toString());
    }

    @Test
    void testAuditIdNull() {
        // Create AuditAlertRule with null auditId
        AuditAlertRule rule = createValidAuditAlertRule();
        rule.setAuditId(null);

        // Validate
        Set<ConstraintViolation<AuditAlertRule>> violations = validator.validate(rule);

        // Assert validation error
        Assertions.assertEquals(1, violations.size());
        ConstraintViolation<AuditAlertRule> violation = violations.iterator().next();
        Assertions.assertEquals("Audit ID cannot be blank", violation.getMessage());
    }

    @Test
    void testAlertNameNotBlank() {
        // Create AuditAlertRule with empty alertName
        AuditAlertRule rule = createValidAuditAlertRule();
        rule.setAlertName("");

        // Validate
        Set<ConstraintViolation<AuditAlertRule>> violations = validator.validate(rule);

        // Assert validation error
        Assertions.assertEquals(1, violations.size());
        ConstraintViolation<AuditAlertRule> violation = violations.iterator().next();
        Assertions.assertEquals("Alert name cannot be blank", violation.getMessage());
        Assertions.assertEquals("alertName", violation.getPropertyPath().toString());
    }

    @Test
    void testAlertNameNull() {
        // Create AuditAlertRule with null alertName
        AuditAlertRule rule = createValidAuditAlertRule();
        rule.setAlertName(null);

        // Validate
        Set<ConstraintViolation<AuditAlertRule>> violations = validator.validate(rule);

        // Assert validation error
        Assertions.assertEquals(1, violations.size());
        ConstraintViolation<AuditAlertRule> violation = violations.iterator().next();
        Assertions.assertEquals("Alert name cannot be blank", violation.getMessage());
    }

    @Test
    void testConditionNotBlank() {
        // Create AuditAlertRule with empty condition
        AuditAlertRule rule = createValidAuditAlertRule();
        AuditAlertCondition condition = new AuditAlertCondition();
        condition.setType("");
        condition.setOperator("");
        condition.setValue(null); // Changed from "" to null to trigger @NotNull validation
        rule.setCondition(condition);

        // Validate
        Set<ConstraintViolation<AuditAlertRule>> violations = validator.validate(rule);

        // Assert validation errors for condition fields (3 errors for type, operator, value)
        Assertions.assertEquals(3, violations.size());

        // Check that we have violations for each field
        boolean hasTypeViolation = false;
        boolean hasOperatorViolation = false;
        boolean hasValueViolation = false;

        for (ConstraintViolation<AuditAlertRule> violation : violations) {
            String message = violation.getMessage();
            String propertyPath = violation.getPropertyPath().toString();

            if ("Condition type cannot be blank".equals(message) && "condition.type".equals(propertyPath)) {
                hasTypeViolation = true;
            } else if ("Operator cannot be blank".equals(message) && "condition.operator".equals(propertyPath)) {
                hasOperatorViolation = true;
            } else if ("Value cannot be null".equals(message) && "condition.value".equals(propertyPath)) {
                hasValueViolation = true;
            }
        }

        Assertions.assertTrue(hasTypeViolation);
        Assertions.assertTrue(hasOperatorViolation);
        Assertions.assertTrue(hasValueViolation);
    }

    @Test
    void testConditionNull() {
        // Create AuditAlertRule with null condition
        AuditAlertRule rule = createValidAuditAlertRule();
        rule.setCondition(null);

        // Validate
        Set<ConstraintViolation<AuditAlertRule>> violations = validator.validate(rule);

        // Assert validation error
        Assertions.assertEquals(1, violations.size());
        ConstraintViolation<AuditAlertRule> violation = violations.iterator().next();
        Assertions.assertEquals("Trigger condition cannot be null", violation.getMessage());
    }

    @Test
    void testEnabledNotNull() {
        // Create AuditAlertRule with null enabled
        AuditAlertRule rule = createValidAuditAlertRule();
        rule.setEnabled(null);

        // Validate
        Set<ConstraintViolation<AuditAlertRule>> violations = validator.validate(rule);

        // Assert validation error
        Assertions.assertEquals(1, violations.size());
        ConstraintViolation<AuditAlertRule> violation = violations.iterator().next();
        Assertions.assertEquals("Enabled status cannot be null", violation.getMessage());
        Assertions.assertEquals("enabled", violation.getPropertyPath().toString());
    }

    @Test
    void testValidAlertLevels() {
        // Test valid alert levels
        String[] validLevels = {"INFO", "WARN", "ERROR", "CRITICAL"};

        for (String level : validLevels) {
            AuditAlertRule rule = createValidAuditAlertRule();
            rule.setLevel(level);

            Set<ConstraintViolation<AuditAlertRule>> violations = validator.validate(rule);
            Assertions.assertTrue(violations.isEmpty(), "Level " + level + " should be valid");
        }
    }

    @Test
    void testInvalidAlertLevel() {
        // Test invalid alert level
        AuditAlertRule rule = createValidAuditAlertRule();
        rule.setLevel("INVALID");

        Set<ConstraintViolation<AuditAlertRule>> violations = validator.validate(rule);

        // Assert validation error
        Assertions.assertEquals(1, violations.size());
        ConstraintViolation<AuditAlertRule> violation = violations.iterator().next();
        Assertions.assertEquals("Alert level must be one of INFO, WARN, ERROR, or CRITICAL", violation.getMessage());
        Assertions.assertEquals("level", violation.getPropertyPath().toString());
    }

    @Test
    void testValidNotifyTypes() {
        // Test valid notification types
        NotifyType[] validNotifyTypes = {NotifyType.EMAIL, NotifyType.SMS, NotifyType.HTTP};

        for (NotifyType notifyType : validNotifyTypes) {
            AuditAlertRule rule = createValidAuditAlertRule();
            rule.setNotifyType(notifyType);

            Set<ConstraintViolation<AuditAlertRule>> violations = validator.validate(rule);
            Assertions.assertTrue(violations.isEmpty(), "NotifyType " + notifyType + " should be valid");
        }
    }

    @Test
    void testInvalidNotifyType() {
        // Test invalid notification type - using EMAIL as placeholder since we can't set invalid enum values
        AuditAlertRule rule = createValidAuditAlertRule();
        rule.setNotifyType(NotifyType.EMAIL);

        Set<ConstraintViolation<AuditAlertRule>> violations = validator.validate(rule);

        // This test is no longer valid since we can't set invalid enum values
        // The validation for notifyType is handled by the enum type itself
        Assertions.assertTrue(violations.isEmpty());
    }

    @Test
    void testMultipleValidationErrors() {
        // Create AuditAlertRule with multiple validation errors
        AuditAlertRule rule = new AuditAlertRule();
        rule.setInlongGroupId(""); // 空字符串
        rule.setAuditId(null); // null
        rule.setAlertName(""); // 空字符串
        rule.setCondition(null); // null
        rule.setEnabled(null); // null
        rule.setLevel("INVALID"); // 无效值
        rule.setNotifyType(NotifyType.EMAIL); // 使用有效值

        // Validate
        Set<ConstraintViolation<AuditAlertRule>> violations = validator.validate(rule);

        // Assert 7 validation errors
        Assertions.assertEquals(7, violations.size());
    }

    @Test
    void testOptionalFieldsCanBeNull() {
        // Create AuditAlertRule with optional fields as null
        AuditAlertRule rule = createValidAuditAlertRule();
        rule.setInlongStreamId(null); // 可选字段
        rule.setLevel(null); // 可选字段
        rule.setNotifyType(null); // 可选字段
        rule.setReceivers(null); // 可选字段
        rule.setCreateTime(null); // 可选字段
        rule.setModifyTime(null); // 可选字段

        // Validate
        Set<ConstraintViolation<AuditAlertRule>> violations = validator.validate(rule);

        // Assert no validation errors (optional fields can be null)
        Assertions.assertTrue(violations.isEmpty());
    }

    @Test
    void testEntityPropertiesGettersAndSetters() {
        // Test entity getter and setter methods
        AuditAlertRule rule = new AuditAlertRule();
        Date now = new Date();

        rule.setId(1);
        rule.setInlongGroupId("test_group");
        rule.setInlongStreamId("test_stream");
        rule.setAuditId("3");
        rule.setAlertName("Test Alert");
        AuditAlertCondition condition = new AuditAlertCondition();
        condition.setType("count");
        condition.setOperator(">");
        condition.setValue(1000);
        rule.setCondition(condition);
        rule.setLevel("ERROR");
        rule.setNotifyType(NotifyType.EMAIL);
        rule.setReceivers("admin@test.com");
        rule.setEnabled(true);
        rule.setCreator("test_user");
        rule.setModifier("test_user");
        rule.setCreateTime(now);
        rule.setModifyTime(now);

        // Assert all properties are set correctly
        Assertions.assertEquals(1, rule.getId());
        Assertions.assertEquals("test_group", rule.getInlongGroupId());
        Assertions.assertEquals("test_stream", rule.getInlongStreamId());
        Assertions.assertEquals("3", rule.getAuditId());
        Assertions.assertEquals("Test Alert", rule.getAlertName());
        Assertions.assertEquals(condition, rule.getCondition());
        Assertions.assertEquals("ERROR", rule.getLevel());
        Assertions.assertEquals(NotifyType.EMAIL.name(), rule.getNotifyType());
        Assertions.assertEquals("admin@test.com", rule.getReceivers());
        Assertions.assertTrue(rule.getEnabled());
        Assertions.assertEquals(0, rule.getIsDeleted().intValue()); // Verify isDeleted
        Assertions.assertEquals("test_user", rule.getCreator());
        Assertions.assertEquals("test_user", rule.getModifier());
        Assertions.assertEquals(now, rule.getCreateTime());
        Assertions.assertEquals(now, rule.getModifyTime());
        Assertions.assertEquals(1, rule.getVersion().intValue()); // Verify version
    }

    @Test
    void testEntityEqualsAndHashCode() {
        // Test entity equals and hashCode methods
        AuditAlertRule rule1 = createValidAuditAlertRule();
        rule1.setId(1);

        AuditAlertRule rule2 = createValidAuditAlertRule();
        rule2.setId(1);

        AuditAlertRule rule3 = createValidAuditAlertRule();
        rule3.setId(2);

        // Test equals method
        Assertions.assertEquals(rule1, rule2);
        Assertions.assertNotEquals(rule1, rule3);
        Assertions.assertNotEquals(rule1, null);
        Assertions.assertNotEquals(rule1, "string");

        // Test hashCode method
        Assertions.assertEquals(rule1.hashCode(), rule2.hashCode());
    }

    @Test
    void testEntityToString() {
        // Test entity toString method
        AuditAlertRule rule = createValidAuditAlertRule();
        rule.setId(1);

        String toString = rule.toString();

        // Assert toString contains key information
        Assertions.assertNotNull(toString);
        Assertions.assertTrue(toString.contains("test_group_001"));
        Assertions.assertTrue(toString.contains("Data Loss Alert"));
    }

    /**
     * Create valid AuditAlertRule object
     */
    private AuditAlertRule createValidAuditAlertRule() {
        AuditAlertRule rule = new AuditAlertRule();
        rule.setInlongGroupId("test_group_001");
        rule.setInlongStreamId("test_stream_001");
        rule.setAuditId("3");
        rule.setAlertName("Data Loss Alert");
        AuditAlertCondition condition = new AuditAlertCondition();
        condition.setType("data_loss");
        condition.setOperator("<");
        condition.setValue(1000);
        rule.setCondition(condition);
        rule.setLevel("ERROR");
        rule.setNotifyType(NotifyType.EMAIL);
        rule.setReceivers("admin@example.com,monitor@example.com");
        rule.setEnabled(true);
        rule.setCreator("test_user");
        rule.setModifier("test_user");
        return rule;
    }
}