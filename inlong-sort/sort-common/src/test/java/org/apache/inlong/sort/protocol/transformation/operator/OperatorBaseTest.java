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

package org.apache.inlong.sort.protocol.transformation.operator;

import com.google.common.base.Preconditions;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.inlong.sort.protocol.transformation.Operator;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public abstract class OperatorBaseTest {

    private final ObjectMapper objectMapper = new ObjectMapper();
    private String expectFormat;
    private String expectSerializeStr;
    private Operator operator;

    public abstract Operator getOperator();

    public abstract String getExpectFormat();

    public abstract String getExpectSerializeStr();

    @Before
    public void init() {
        this.expectFormat = Preconditions.checkNotNull(getExpectFormat());
        this.operator = Preconditions.checkNotNull(getOperator());
        this.expectSerializeStr = getExpectSerializeStr();
    }

    @Test
    public void testSerialize() throws JsonProcessingException {
        assertEquals(expectSerializeStr, objectMapper.writeValueAsString(operator));
    }

    @Test
    public void testDeserialize() throws JsonProcessingException {
        AndOperator operator = AndOperator.getInstance();
        ObjectMapper objectMapper = new ObjectMapper();
        Operator expected = objectMapper.readValue(expectSerializeStr, operator.getClass());
        assertEquals(expected, operator);
    }

    @Test
    public void testFormat() throws JsonProcessingException {
        assertEquals(expectFormat, operator.format());
    }
}
