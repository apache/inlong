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

package org.apache.inlong.sort.protocol.node.format;

import com.google.common.base.Preconditions;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public abstract class FormatBaseTest {

    private final ObjectMapper objectMapper = new ObjectMapper();

    private Format format;

    /**
     * init format object
     */
    @Before
    public void before() {
        this.format = Preconditions.checkNotNull(getFormat());
    }

    public abstract Format getFormat();

    /**
     * Test Serialize and Deserialize
     * @throws JsonProcessingException throw JsonProcessingException when parse exception
     */
    @Test
    public void testSerializeAndDeserialize() throws JsonProcessingException {
        String jsonStr = objectMapper.writeValueAsString(format);
        Format deserializeFormat = objectMapper.readValue(jsonStr, Format.class);
        assertEquals(format, deserializeFormat);
    }

}
