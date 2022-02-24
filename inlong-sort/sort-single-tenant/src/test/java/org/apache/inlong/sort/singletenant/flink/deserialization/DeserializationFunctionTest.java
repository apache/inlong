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

package org.apache.inlong.sort.singletenant.flink.deserialization;

import org.apache.flink.types.Row;
import org.apache.inlong.common.msg.InLongMsg;
import org.apache.inlong.sort.formats.common.StringFormatInfo;
import org.apache.inlong.sort.protocol.FieldInfo;
import org.apache.inlong.sort.singletenant.flink.SerializedRecord;
import org.junit.Test;

import java.nio.charset.StandardCharsets;

import static org.junit.Assert.assertEquals;

public class DeserializationFunctionTest {

    @Test
    public void testProcessElement() throws Exception {
        InLongMsg inLongMsg = InLongMsg.newInLongMsg();
        String testData = "testData";
        inLongMsg.addMsg("m=12&iname=tid", testData.getBytes(StandardCharsets.UTF_8));
        SerializedRecord serializedRecord = new SerializedRecord(1, inLongMsg.buildArray());

        DeserializationFunction function = new DeserializationFunction(
                DeserializationSchemaFactory.build(
                        new FieldInfo[]{new FieldInfo("content", StringFormatInfo.INSTANCE)}, null
                )
        );

        ListCollector<Row> collector = new ListCollector<>();
        function.processElement(serializedRecord,null, collector);
        Row row = collector.getInnerList().get(0);
        assertEquals(1, row.getArity());
        assertEquals(testData, row.getField(0));
    }

}
