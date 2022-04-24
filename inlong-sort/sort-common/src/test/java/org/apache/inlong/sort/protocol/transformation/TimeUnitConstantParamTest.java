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

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.inlong.sort.protocol.transformation.TimeUnitConstantParam.TimeUnit;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Test for {@link TimeUnitConstantParam}
 */
public class TimeUnitConstantParamTest {

    /**
     * Test for serialize of {@link TimeUnitConstantParam}
     *
     * @throws JsonProcessingException The exception may throws when serialize
     */
    @Test
    public void testSerialize() throws JsonProcessingException {
        TimeUnitConstantParam constantParam = new TimeUnitConstantParam(TimeUnit.HOUR);
        ObjectMapper objectMapper = new ObjectMapper();
        String expected = "{\"type\":\"timeUnitConstant\",\"timeUnit\":\"HOUR\",\"value\":\"HOUR\"}";
        assertEquals(expected, objectMapper.writeValueAsString(constantParam));
    }

    /**
     * Test for deserialize of {@link TimeUnitConstantParam}
     *
     * @throws JsonProcessingException The exception may throws when deserialize
     */
    @Test
    public void testDeserialize() throws JsonProcessingException {
        TimeUnitConstantParam constantParam = new TimeUnitConstantParam(TimeUnit.HOUR);
        ObjectMapper objectMapper = new ObjectMapper();
        String constantParamStr = "{\"type\":\"timeUnitConstant\",\"timeUnit\":\"HOUR\",\"value\":\"HOUR\"}";
        TimeUnitConstantParam expected = objectMapper.readValue(constantParamStr, TimeUnitConstantParam.class);
        assertEquals(expected, constantParam);
    }

    /**
     * Test for format
     *
     * @see TimeUnitConstantParam#format()
     */
    @Test
    public void testFormat() {
        TimeUnitConstantParam constantParam = new TimeUnitConstantParam(TimeUnit.HOUR);
        assertEquals("HOUR", constantParam.format());
    }

}
