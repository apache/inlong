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

package org.apache.inlong.sort.protocol.transformation;

import com.google.common.base.Preconditions;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Function test base class
 */
public abstract class FunctionBaseTest {

    private final ObjectMapper objectMapper = new ObjectMapper();
    private String expectFormat;
    private String expectSerializeStr;
    private Function function;

    public abstract Function getFunction();

    public abstract String getExpectFormat();

    public abstract String getExpectSerializeStr();

    /**
     * Init the expectFormat, function instance, expectSerializeStr
     */
    @Before
    public void init() {
        this.expectFormat = Preconditions.checkNotNull(getExpectFormat());
        this.function = Preconditions.checkNotNull(getFunction());
        this.expectSerializeStr = getExpectSerializeStr();
    }

    /**
     * Test serialize
     *
     * @throws JsonProcessingException The exception may throws when executing
     */
    @Test
    public void testSerialize() throws JsonProcessingException {
        assertEquals(expectSerializeStr, objectMapper.writeValueAsString(function));
    }

    /**
     * Test deserialize
     *
     * @throws JsonProcessingException The exception may throws when executing
     */
    @Test
    public void testDeserialize() throws JsonProcessingException {
        ObjectMapper objectMapper = new ObjectMapper();
        Function expected = objectMapper.readValue(expectSerializeStr, function.getClass());
        assertEquals(expected, function);
    }

    /**
     * Test format in standard sql
     */
    @Test
    public void testFormat() {
        assertEquals(expectFormat, function.format());
    }
}
