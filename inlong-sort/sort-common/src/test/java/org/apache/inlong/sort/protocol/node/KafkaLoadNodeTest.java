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

import org.apache.inlong.sort.formats.common.StringFormatInfo;
import org.apache.inlong.sort.protocol.FieldInfo;
import org.apache.inlong.sort.protocol.node.load.KafkaLoadNode;
import org.apache.inlong.sort.protocol.transformation.FieldRelationShip;

import java.util.Arrays;
import java.util.TreeMap;

public class KafkaLoadNodeTest extends NodeBaseTest {

    @Override
    public Node getNode() {
        return new KafkaLoadNode("1", null,
                Arrays.asList(new FieldInfo("field", new StringFormatInfo())),
                Arrays.asList(new FieldRelationShip(new FieldInfo("field", new StringFormatInfo()),
                        new FieldInfo("field", new StringFormatInfo()))), null,
                "topic", "localhost:9092", "json",
                1, new TreeMap<>());
    }

    @Override
    public String getExpectSerializeStr() {
        return "{\"type\":\"kafkaLoad\",\"id\":\"1\",\"fields\":[{\"type\":\"base\",\"name\":\"field\","
                + "\"formatInfo\":{\"type\":\"string\"}}],\"fieldRelationShips\":[{\"type\":\"fieldRelationShip\","
                + "\"inputField\":{\"type\":\"base\",\"name\":\"field\",\"formatInfo\":{\"type\":\"string\"}},"
                + "\"outputField\":{\"type\":\"base\",\"name\":\"field\",\"formatInfo\":{\"type\":\"string\"}}}],"
                + "\"filters\":null,\"topic\":\"topic\",\"bootstrapServers\":\"localhost:9092\","
                + "\"format\":\"json\",\"sinkParallelism\":1,\"properties\":{}}";
    }
}
