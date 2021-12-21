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

package org.apache.inlong.sort.flink.deserialization;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.Map;
import org.apache.inlong.commons.msg.TDMsg1;
import org.apache.inlong.sort.flink.Record;
import org.apache.inlong.sort.flink.TDMsgSerializedRecord;
import org.apache.inlong.sort.formats.common.LongFormatInfo;
import org.apache.inlong.sort.formats.common.StringFormatInfo;
import org.apache.inlong.sort.formats.tdmsg.TDMsgUtils;
import org.apache.inlong.sort.protocol.DataFlowInfo;
import org.apache.inlong.sort.protocol.FieldInfo;
import org.apache.inlong.sort.protocol.deserialization.TDMsgCsvDeserializationInfo;
import org.apache.inlong.sort.protocol.source.TubeSourceInfo;
import org.apache.inlong.sort.util.TestLogger;
import org.apache.inlong.sort.util.TestingUtils.EmptySinkInfo;
import org.apache.inlong.sort.util.TestingUtils.EmptySourceInfo;
import org.apache.inlong.sort.util.TestingUtils.TestingCollector;
import org.junit.Test;

/**
 * Unit test for {@link MultiTenancyTDMsgMixedDeserializer}.
 */
public class MultiTenancyTDMsgMixedDeserializerTest extends TestLogger {

    @Test
    public void testIsTDMsgDataFlow() {
        final TubeSourceInfo tubeSourceInfo = new TubeSourceInfo(
                "topic",
                "address",
                null,
                new TDMsgCsvDeserializationInfo("tid", ',', false),
                new FieldInfo[0]);
        final EmptySinkInfo sinkInfo = new EmptySinkInfo();
        final DataFlowInfo dataFlowInfo = new DataFlowInfo(1L, tubeSourceInfo, sinkInfo);

        assertTrue(MultiTenancyTDMsgMixedDeserializer.isTDMsgDataFlow(dataFlowInfo));

        final DataFlowInfo nonTDMsgDataFlow = new DataFlowInfo(2L, new EmptySourceInfo(), sinkInfo);
        assertFalse(MultiTenancyTDMsgMixedDeserializer.isTDMsgDataFlow(nonTDMsgDataFlow));
    }

    @Test
    public void testDeserialize() throws Exception {
        final MultiTenancyTDMsgMixedDeserializer deserializer = new MultiTenancyTDMsgMixedDeserializer();
        final FieldInfo stringField = new FieldInfo("not_important", new StringFormatInfo());
        final FieldInfo longField = new FieldInfo("id", new LongFormatInfo());

        final TubeSourceInfo tubeSourceInfo = new TubeSourceInfo(
                "topic",
                "address",
                null,
                new TDMsgCsvDeserializationInfo("tid", '|', false),
                new FieldInfo[]{stringField, longField});
        final EmptySinkInfo sinkInfo = new EmptySinkInfo();
        final DataFlowInfo dataFlowInfo = new DataFlowInfo(1L, tubeSourceInfo, sinkInfo);
        deserializer.addDataFlow(dataFlowInfo);

        final TDMsg1 tdMsg1 = TDMsg1.newTDMsg();
        final String attrs = "m=0&" + TDMsgUtils.TDMSG_ATTR_STREAM_ID + "=tid&t=20210513";
        final String body1 = "tianqiwan|29";
        tdMsg1.addMsg(attrs, body1.getBytes());

        final TestingCollector<Record> collector = new TestingCollector<>();
        deserializer.deserialize(new TDMsgSerializedRecord("topic", 0, tdMsg1.buildArray()), collector);

        assertEquals(1, collector.results.size());
        assertEquals(1L, collector.results.get(0).getDataflowId());
        assertEquals(4, collector.results.get(0).getRow().getArity());
        final long time = new SimpleDateFormat("yyyyMMdd").parse("20210513").getTime();
        assertEquals(new Timestamp(time), collector.results.get(0).getRow().getField(0));
        final Map<String, String> attributes = new HashMap<>();
        attributes.put("m", "0");
        attributes.put(TDMsgUtils.TDMSG_ATTR_STREAM_ID, "tid");
        attributes.put("t", "20210513");
        assertEquals(attributes, collector.results.get(0).getRow().getField(1));
        assertEquals("tianqiwan", collector.results.get(0).getRow().getField(2));
        assertEquals(29L, collector.results.get(0).getRow().getField(3));
    }
}
