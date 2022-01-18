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

import java.util.ArrayDeque;
import java.util.Queue;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.apache.inlong.sort.flink.Record;
import org.apache.inlong.sort.flink.TDMsgMixedSerializedRecord;
import org.apache.inlong.sort.formats.tdmsg.AbstractTDMsgFormatDeserializer;
import org.apache.inlong.sort.formats.tdmsg.TDMsgBody;
import org.apache.inlong.sort.formats.tdmsg.TDMsgHead;
import org.apache.inlong.sort.formats.tdmsg.TDMsgMixedFormatConverter;
import org.apache.inlong.sort.util.TestLogger;
import org.apache.inlong.sort.util.TestingUtils.TestingCollector;
import org.junit.Before;
import org.junit.Test;

/**
 * Unit test for {@link TDMsgMixedDeserializer}.
 */
public class TDMsgMixedDeserializerTest extends TestLogger {

    private final long dataFlowId1 = 1L;
    private final long dataFlowId2 = 2L;
    private final String tid = "tid";
    private final TestingAbstractTDMsgFormatDeserializer preDeserializer = new TestingAbstractTDMsgFormatDeserializer();
    private final TestingConverter deserializer = new TestingConverter();
    private final TestingCollector<Record> collector = new TestingCollector<>();

    @Before
    public void setup() {
        preDeserializer.records.clear();
        collector.results.clear();
    }

    @Test
    public void testUpdateAndRemoveDataFlow() {
        final TDMsgMixedDeserializer mixedDeserializer = new TDMsgMixedDeserializer();
        assertTrue(mixedDeserializer.isEmpty());

        mixedDeserializer.updateDataFlow(dataFlowId1, tid, new TestingAbstractTDMsgFormatDeserializer(), deserializer);
        mixedDeserializer.updateDataFlow(dataFlowId2, tid, preDeserializer, deserializer);
        assertFalse(mixedDeserializer.isEmpty());

        // verify the preDeserializer is updated
        assertEquals(preDeserializer, mixedDeserializer.getPreDeserializer());
        assertEquals(1, mixedDeserializer.getDeserializers().size());
        assertEquals(deserializer, mixedDeserializer.getDeserializers().get(tid));
        assertEquals(1, mixedDeserializer.getInterface2DataFlowsMap().size());
        assertEquals(2, mixedDeserializer.getInterface2DataFlowsMap().get(tid).size());
        assertTrue(mixedDeserializer.getInterface2DataFlowsMap().get(tid).contains(dataFlowId1));
        assertTrue(mixedDeserializer.getInterface2DataFlowsMap().get(tid).contains(dataFlowId2));

        mixedDeserializer.removeDataFlow(dataFlowId1, tid);
        assertFalse(mixedDeserializer.isEmpty());
        mixedDeserializer.removeDataFlow(dataFlowId2, tid);
        assertTrue(mixedDeserializer.isEmpty());
        mixedDeserializer.updateDataFlow(dataFlowId2, tid, preDeserializer, deserializer);
        assertFalse(mixedDeserializer.isEmpty());
    }

    @Test
    public void testDeserialize() throws Exception {
        final TDMsgMixedDeserializer mixedDeserializer = new TDMsgMixedDeserializer();
        mixedDeserializer.updateDataFlow(dataFlowId1, tid, preDeserializer, deserializer);
        mixedDeserializer.updateDataFlow(dataFlowId2, tid, preDeserializer, deserializer);

        final Row row = new Row(3);
        row.setField(0, null);
        row.setField(1, new byte[0]);
        row.setField(2, tid);
        preDeserializer.records.add(row);

        mixedDeserializer.deserialize(new TDMsgMixedSerializedRecord(), collector);
        assertEquals(2, collector.results.size());
        assertEquals(dataFlowId1, collector.results.get(0).getDataflowId());
        assertEquals(tid, collector.results.get(0).getRow().getField(2));
        assertEquals(dataFlowId2, collector.results.get(1).getDataflowId());
        assertEquals(tid, collector.results.get(1).getRow().getField(2));
    }

    private static class TestingAbstractTDMsgFormatDeserializer extends AbstractTDMsgFormatDeserializer {

        private static final long serialVersionUID = -5356395595688009428L;

        public final Queue<Row> records = new ArrayDeque<>();

        public TestingAbstractTDMsgFormatDeserializer() {
            super(false);
        }

        @Override
        public void flatMap(byte[] bytes, Collector<Row> collector) throws Exception {
            collector.collect(records.poll());
        }

        @Override
        protected TDMsgHead parseHead(String s) throws Exception {
            return null;
        }

        @Override
        protected TDMsgBody parseBody(byte[] bytes) throws Exception {
            return null;
        }

        @Override
        protected Row convertRow(TDMsgHead tdMsgHead, TDMsgBody tdMsgBody) throws Exception {
            return null;
        }

        @Override
        public TypeInformation<Row> getProducedType() {
            return null;
        }
    }

    private static class TestingConverter implements TDMsgMixedFormatConverter {

        private static final long serialVersionUID = -2239130770704933846L;

        @Override
        public void flatMap(Row row, Collector<Row> collector) throws Exception {
            collector.collect(row);
        }

        @Override
        public TypeInformation<Row> getProducedType() {
            return null;
        }
    }
}