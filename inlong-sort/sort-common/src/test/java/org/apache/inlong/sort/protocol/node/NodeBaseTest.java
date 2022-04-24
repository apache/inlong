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

package org.apache.inlong.sort.protocol.node;

import com.google.common.base.Preconditions;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public abstract class NodeBaseTest {

    private final ObjectMapper objectMapper = new ObjectMapper();
    private String expectSerializeStr;
    private Node node;

    public abstract Node getNode();

    public abstract String getExpectSerializeStr();

    @Before
    public void init() {
        this.node = Preconditions.checkNotNull(getNode());
        this.expectSerializeStr = Preconditions.checkNotNull(getExpectSerializeStr());
    }

    @Test
    public void testSerialize() throws JsonProcessingException {
        String actual = objectMapper.writeValueAsString(node);
        assertEquals(expectSerializeStr, actual);
    }

    @Test
    public void testDeserialize() throws JsonProcessingException {
        Node expected = objectMapper.readValue(expectSerializeStr, Node.class);
        assertEquals(expected, node);
    }

}
