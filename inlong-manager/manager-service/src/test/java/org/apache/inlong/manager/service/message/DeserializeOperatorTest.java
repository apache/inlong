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

package org.apache.inlong.manager.service.message;

import org.apache.inlong.common.enums.MessageWrapType;
import org.apache.inlong.manager.pojo.consume.BriefMQMessage.FieldInfo;
import org.apache.inlong.manager.pojo.stream.QueryMessageRequest;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for {@link DeserializeOperator#checkIfFilter(QueryMessageRequest, List)} method.
 * This test class verifies the filtering logic for different operation types: =, !=, and like.
 */
public class DeserializeOperatorTest {

    private DeserializeOperator operator;
    private List<FieldInfo> fieldList;

    @BeforeEach
    void setUp() {
        // Create a simple implementation for testing the default method
        operator = new DeserializeOperator() {

            @Override
            public boolean accept(MessageWrapType type) {
                return false;
            }
        };

        // Initialize test field list
        fieldList = Arrays.asList(
                FieldInfo.builder().fieldName("name").fieldValue("John").build(),
                FieldInfo.builder().fieldName("age").fieldValue("25").build(),
                FieldInfo.builder().fieldName("email").fieldValue("john@example.com").build(),
                FieldInfo.builder().fieldName("nullField").fieldValue(null).build());
    }

    // ==================== Tests for precondition checks ====================

    @Test
    void testCheckIfFilter_whenFieldNameIsNull_shouldReturnFalse() {
        QueryMessageRequest request = QueryMessageRequest.builder()
                .fieldName(null)
                .operationType("=")
                .targetValue("John")
                .build();

        boolean result = operator.checkIfFilter(request, fieldList);

        // Should not filter when fieldName is null
        assertFalse(result);
    }

    @Test
    void testCheckIfFilter_whenFieldNameIsBlank_shouldReturnFalse() {
        QueryMessageRequest request = QueryMessageRequest.builder()
                .fieldName("  ")
                .operationType("=")
                .targetValue("John")
                .build();

        boolean result = operator.checkIfFilter(request, fieldList);

        // Should not filter when fieldName is blank
        assertFalse(result);
    }

    @Test
    void testCheckIfFilter_whenOperationTypeIsNull_shouldReturnFalse() {
        QueryMessageRequest request = QueryMessageRequest.builder()
                .fieldName("name")
                .operationType(null)
                .targetValue("John")
                .build();

        boolean result = operator.checkIfFilter(request, fieldList);

        // Should not filter when operationType is null
        assertFalse(result);
    }

    @Test
    void testCheckIfFilter_whenOperationTypeIsBlank_shouldReturnFalse() {
        QueryMessageRequest request = QueryMessageRequest.builder()
                .fieldName("name")
                .operationType("")
                .targetValue("John")
                .build();

        boolean result = operator.checkIfFilter(request, fieldList);

        // Should not filter when operationType is blank
        assertFalse(result);
    }

    @Test
    void testCheckIfFilter_whenFieldNotFound_shouldReturnFalse() {
        QueryMessageRequest request = QueryMessageRequest.builder()
                .fieldName("nonExistentField")
                .operationType("=")
                .targetValue("value")
                .build();

        boolean result = operator.checkIfFilter(request, fieldList);

        // Should not filter when field is not found in the list
        assertFalse(result);
    }

    @Test
    void testCheckIfFilter_whenFieldListIsEmpty_shouldReturnFalse() {
        QueryMessageRequest request = QueryMessageRequest.builder()
                .fieldName("name")
                .operationType("=")
                .targetValue("John")
                .build();

        boolean result = operator.checkIfFilter(request, Collections.emptyList());

        // Should not filter when field list is empty
        assertFalse(result);
    }

    // ==================== Tests for equals (=) operation ====================

    @Test
    void testCheckIfFilter_equalsOperation_whenValuesMatch_shouldReturnFalse() {
        QueryMessageRequest request = QueryMessageRequest.builder()
                .fieldName("name")
                .operationType("=")
                .targetValue("John")
                .build();

        boolean result = operator.checkIfFilter(request, fieldList);

        // targetValue equals fieldValue, so record should NOT be filtered
        assertFalse(result);
    }

    @Test
    void testCheckIfFilter_equalsOperation_whenValuesNotMatch_shouldReturnTrue() {
        QueryMessageRequest request = QueryMessageRequest.builder()
                .fieldName("name")
                .operationType("=")
                .targetValue("Jane")
                .build();

        boolean result = operator.checkIfFilter(request, fieldList);

        // targetValue does not equal fieldValue, so record should be filtered
        assertTrue(result);
    }

    @Test
    void testCheckIfFilter_equalsOperation_whenTargetValueIsNull_andFieldValueIsNull_shouldReturnFalse() {
        QueryMessageRequest request = QueryMessageRequest.builder()
                .fieldName("nullField")
                .operationType("=")
                .targetValue(null)
                .build();

        boolean result = operator.checkIfFilter(request, fieldList);

        // Both are null, `!Objects.equals` returns false, so record should NOT be filtered
        assertFalse(result);
    }

    @Test
    void testCheckIfFilter_equalsOperation_whenTargetValueIsNull_andFieldValueIsNotNull_shouldReturnTrue() {
        QueryMessageRequest request = QueryMessageRequest.builder()
                .fieldName("name")
                .operationType("=")
                .targetValue(null)
                .build();

        boolean result = operator.checkIfFilter(request, fieldList);

        // targetValue is null but fieldValue is "John", so record should be filtered
        assertTrue(result);
    }

    @Test
    void testCheckIfFilter_equalsOperation_whenTargetValueIsNotNull_andFieldValueIsNull_shouldReturnTrue() {
        QueryMessageRequest request = QueryMessageRequest.builder()
                .fieldName("nullField")
                .operationType("=")
                .targetValue("someValue")
                .build();

        boolean result = operator.checkIfFilter(request, fieldList);

        // targetValue is not null but fieldValue is null, so record should be filtered
        assertTrue(result);
    }

    // ==================== Tests for not equals (!=) operation ====================

    @Test
    void testCheckIfFilter_notEqualsOperation_whenValuesMatch_shouldReturnTrue() {
        QueryMessageRequest request = QueryMessageRequest.builder()
                .fieldName("name")
                .operationType("!=")
                .targetValue("John")
                .build();

        boolean result = operator.checkIfFilter(request, fieldList);

        // targetValue equals fieldValue, so record should be filtered (we want records that are NOT equal)
        assertTrue(result);
    }

    @Test
    void testCheckIfFilter_notEqualsOperation_whenValuesNotMatch_shouldReturnFalse() {
        QueryMessageRequest request = QueryMessageRequest.builder()
                .fieldName("name")
                .operationType("!=")
                .targetValue("Jane")
                .build();

        boolean result = operator.checkIfFilter(request, fieldList);

        // targetValue does not equal fieldValue, so record should NOT be filtered
        assertFalse(result);
    }

    @Test
    void testCheckIfFilter_notEqualsOperation_whenBothValuesAreNull_shouldReturnTrue() {
        QueryMessageRequest request = QueryMessageRequest.builder()
                .fieldName("nullField")
                .operationType("!=")
                .targetValue(null)
                .build();

        boolean result = operator.checkIfFilter(request, fieldList);

        // Both are null, Objects.equals returns true, so record should be filtered
        assertTrue(result);
    }

    @Test
    void testCheckIfFilter_notEqualsOperation_whenTargetValueIsNull_andFieldValueIsNotNull_shouldReturnFalse() {
        QueryMessageRequest request = QueryMessageRequest.builder()
                .fieldName("name")
                .operationType("!=")
                .targetValue(null)
                .build();

        boolean result = operator.checkIfFilter(request, fieldList);

        // targetValue is null but fieldValue is "John", they are not equal, so record should NOT be filtered
        assertFalse(result);
    }

    // ==================== Tests for like operation ====================

    @Test
    void testCheckIfFilter_likeOperation_whenFieldValueContainsTargetValue_shouldReturnFalse() {
        QueryMessageRequest request = QueryMessageRequest.builder()
                .fieldName("email")
                .operationType("like")
                .targetValue("@example")
                .build();

        boolean result = operator.checkIfFilter(request, fieldList);

        // fieldValue contains targetValue, so record should NOT be filtered
        assertFalse(result);
    }

    @Test
    void testCheckIfFilter_likeOperation_whenFieldValueNotContainsTargetValue_shouldReturnTrue() {
        QueryMessageRequest request = QueryMessageRequest.builder()
                .fieldName("email")
                .operationType("like")
                .targetValue("@gmail")
                .build();

        boolean result = operator.checkIfFilter(request, fieldList);

        // fieldValue does not contain targetValue, so record should be filtered
        assertTrue(result);
    }

    @Test
    void testCheckIfFilter_likeOperation_whenFieldValueIsNull_shouldReturnTrue() {
        QueryMessageRequest request = QueryMessageRequest.builder()
                .fieldName("nullField")
                .operationType("like")
                .targetValue("anyValue")
                .build();

        boolean result = operator.checkIfFilter(request, fieldList);

        // fieldValue is null, like operation is invalid, so record should be filtered
        assertTrue(result);
    }

    @Test
    void testCheckIfFilter_likeOperation_whenTargetValueIsNull_shouldReturnTrue() {
        QueryMessageRequest request = QueryMessageRequest.builder()
                .fieldName("name")
                .operationType("like")
                .targetValue(null)
                .build();

        boolean result = operator.checkIfFilter(request, fieldList);

        // targetValue is null, like operation is invalid, so record should be filtered
        assertTrue(result);
    }

    @Test
    void testCheckIfFilter_likeOperation_whenTargetValueIsExactMatch_shouldReturnFalse() {
        QueryMessageRequest request = QueryMessageRequest.builder()
                .fieldName("name")
                .operationType("like")
                .targetValue("John")
                .build();

        boolean result = operator.checkIfFilter(request, fieldList);

        // fieldValue exactly matches targetValue (contains it), so record should NOT be filtered
        assertFalse(result);
    }

    @Test
    void testCheckIfFilter_likeOperation_whenTargetValueIsPartialMatch_shouldReturnFalse() {
        QueryMessageRequest request = QueryMessageRequest.builder()
                .fieldName("name")
                .operationType("like")
                .targetValue("oh")
                .build();

        boolean result = operator.checkIfFilter(request, fieldList);

        // fieldValue "John" contains "oh", so record should NOT be filtered
        assertFalse(result);
    }

    @Test
    void testCheckIfFilter_likeOperation_caseSensitive_shouldReturnTrue() {
        QueryMessageRequest request = QueryMessageRequest.builder()
                .fieldName("name")
                .operationType("like")
                .targetValue("JOHN")
                .build();

        boolean result = operator.checkIfFilter(request, fieldList);

        // String.contains is case-sensitive, "John" does not contain "JOHN", so record should be filtered
        assertTrue(result);
    }

    // ==================== Tests for unknown operation type ====================

    @Test
    void testCheckIfFilter_unknownOperation_shouldReturnFalse() {
        QueryMessageRequest request = QueryMessageRequest.builder()
                .fieldName("name")
                .operationType("unknown")
                .targetValue("John")
                .build();

        boolean result = operator.checkIfFilter(request, fieldList);

        // Unknown operation type, ifFilter remains false (default value)
        assertFalse(result);
    }

    @Test
    void testCheckIfFilter_greaterThanOperation_shouldReturnFalse() {
        QueryMessageRequest request = QueryMessageRequest.builder()
                .fieldName("age")
                .operationType(">")
                .targetValue("20")
                .build();

        boolean result = operator.checkIfFilter(request, fieldList);

        // Greater than operation is not implemented, ifFilter remains false
        assertFalse(result);
    }
}
