/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.inlong.sort.protocol;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

public abstract class ProtocolBaseTest {

    private final ObjectMapper objectMapper = new ObjectMapper();

    protected String expectedJson;

    protected Object expectedObject;

    protected Object equalObj1;

    protected Object equalObj2;

    protected Object unequalObj;

    @Before
    public abstract void init();

    @Test
    public void testJson() throws IOException {
        Object objFromExpectedObject = objectMapper.readValue(
                objectMapper.writeValueAsString(expectedObject), expectedObject.getClass()
        );

        Object objFromJson = objectMapper.readValue(expectedJson, expectedObject.getClass());

        assertEquals(expectedObject, objFromExpectedObject);
        assertEquals(objFromExpectedObject, objFromJson);
    }

    @Test
    public void testEquals() {
        assertEquals(equalObj1, equalObj2);
        assertNotEquals(equalObj1, unequalObj);
    }
}
