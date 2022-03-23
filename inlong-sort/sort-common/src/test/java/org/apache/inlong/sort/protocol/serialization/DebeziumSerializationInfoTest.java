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

package org.apache.inlong.sort.protocol.serialization;

import org.apache.inlong.sort.protocol.ProtocolBaseTest;

public class DebeziumSerializationInfoTest extends ProtocolBaseTest {

    @Override
    public void init() {
        expectedObject = new DebeziumSerializationInfo("SQL", "LITERAL", "null", true);

        expectedJson = "{\n"
                + "    \"type\":\"debezium_json\",\n"
                + "    \"timestamp_format_standard\":\"SQL\",\n"
                + "    \"map_null_key_mod\":\"LITERAL\",\n"
                + "    \"map_null_key_literal\":\"null\",\n"
                + "    \"encode_decimal_as_plain_number\":true\n"
                + "}";

        equalObj1 = expectedObject;
        equalObj2 = new DebeziumSerializationInfo("SQL", "LITERAL", "null", true);
        unequalObj = new DebeziumSerializationInfo("SQL", "LITERAL", "null", false);

    }
}
