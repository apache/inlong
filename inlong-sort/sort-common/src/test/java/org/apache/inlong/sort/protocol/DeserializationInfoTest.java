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

package org.apache.inlong.sort.protocol;

import static org.junit.Assert.assertEquals;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.inlong.sort.protocol.deserialization.CsvDeserializationInfo;
import java.io.IOException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.inlong.sort.protocol.deserialization.InLongMsgCsvDeserializationInfo;
import org.apache.inlong.sort.protocol.sink.ClickHouseSinkInfo;
import org.apache.inlong.sort.protocol.sink.ClickHouseSinkInfo.PartitionStrategy;
import org.apache.inlong.sort.protocol.source.TubeSourceInfo;
import org.junit.Test;

public class DeserializationInfoTest {
    @Test
    public void testCsvDeserializationInfo() throws IOException {
        ObjectMapper objectMapper = new ObjectMapper();
        String str = "{\"type\":\"csv\",\"splitter\":\"\\u0026\"}";
        CsvDeserializationInfo deserializationInfo = objectMapper.readValue(
                str.getBytes(), CsvDeserializationInfo.class);
        assertEquals('&', deserializationInfo.getSplitter());
    }

    @Test
    public void testToJson() throws JsonProcessingException {
        DataFlowInfo dataFlowInfo = new DataFlowInfo(
            1,
            new TubeSourceInfo("topic" + System.currentTimeMillis(), "ma", "cg",
                new InLongMsgCsvDeserializationInfo("tid", ','), new FieldInfo[0]),
            new ClickHouseSinkInfo("url", "dn", "tn", "un", "pw",
                false, PartitionStrategy.HASH, "pk", new FieldInfo[0], new String[0],
                100, 100, 100));
        ObjectMapper objectMapper = new ObjectMapper();

        System.out.println(objectMapper.writeValueAsString(dataFlowInfo));
    }
}
