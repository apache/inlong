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

package org.apache.inlong.sort.flink.transformation;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;

import java.util.Map;
import org.apache.flink.api.java.typeutils.runtime.RowSerializer;
import org.apache.flink.types.Row;
import org.apache.inlong.sort.flink.Record;
import org.apache.inlong.sort.flink.SerializedRecord;
import org.apache.inlong.sort.formats.common.IntFormatInfo;
import org.apache.inlong.sort.formats.common.LongFormatInfo;
import org.apache.inlong.sort.formats.common.StringFormatInfo;
import org.apache.inlong.sort.protocol.DataFlowInfo;
import org.apache.inlong.sort.protocol.FieldInfo;
import org.apache.inlong.sort.util.TestLogger;
import org.apache.inlong.sort.util.TestingUtils.EmptySourceInfo;
import org.apache.inlong.sort.util.TestingUtils.TestingSinkInfo;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit test for {@link RecordTransformer}.
 */
public class RecordTransformerTest extends TestLogger {

    @Test
    public void testTransformation() throws Exception {
        final int bufferSize = 1024;
        final RecordTransformer transformer = new RecordTransformer(bufferSize);
        final FieldInfo field1 = new FieldInfo("field1", new LongFormatInfo());
        final FieldInfo field2 = new FieldInfo("field2", new StringFormatInfo());
        final TestingSinkInfo sinkInfo = new TestingSinkInfo(new FieldInfo[]{field1, field2});
        final DataFlowInfo dataFlowInfo = new DataFlowInfo(1L, new EmptySourceInfo(), sinkInfo);
        transformer.addDataFlow(dataFlowInfo);
        final Row row = new Row(2);
        row.setField(0, 1024L);
        row.setField(1, "9527");
        final Record record = new Record(1L, row);
        final Record transformed = transformer.toRecord(transformer.toSerializedRecord(record));
        assertEquals(record, transformed);
        // check the buffers
        assertEquals(0, transformer.getDataInputDeserializer().available());
        assertEquals(0, transformer.getDataOutputSerializer().length());
        assertEquals(bufferSize, transformer.getDataOutputSerializer().getSharedBuffer().length);

        transformer.removeDataFlow(dataFlowInfo);
        assertEquals(0, transformer.getRowSerializers().size());
    }

    @Test
    public void testRecordMatchSerializer() throws Exception {
        final int bufferSize = 1024;
        final RecordTransformer transformer = new RecordTransformer(bufferSize);

        final FieldInfo field1 = new FieldInfo("field1", new LongFormatInfo());
        final FieldInfo field2 = new FieldInfo("field2", new StringFormatInfo());
        final TestingSinkInfo sinkInfo = new TestingSinkInfo(new FieldInfo[]{field1, field2});
        final DataFlowInfo dataFlowInfo = new DataFlowInfo(1L, new EmptySourceInfo(), sinkInfo);

        transformer.addDataFlow(dataFlowInfo);
        Map<Long, RowSerializer> rowSerializers = transformer.getRowSerializers();

        final Row row = new Row(2);
        row.setField(0, 1024L);
        row.setField(1, "9527");
        final Record record = new Record(1L, row);

        assertSame(record, transformer.matchRecordAndSerializerField(record, rowSerializers.get(1L)));
    }

    @Test
    public void testRecordNotMatchSerializer() throws Exception {
        final int bufferSize = 1024;
        final RecordTransformer transformer = new RecordTransformer(bufferSize);

        final FieldInfo field1 = new FieldInfo("field1", new LongFormatInfo());
        final FieldInfo field2 = new FieldInfo("field2", new StringFormatInfo());
        final TestingSinkInfo sinkInfo = new TestingSinkInfo(new FieldInfo[]{field1, field2});
        final DataFlowInfo dataFlowInfo = new DataFlowInfo(1L, new EmptySourceInfo(), sinkInfo);

        transformer.addDataFlow(dataFlowInfo);
        Map<Long, RowSerializer> rowSerializers = transformer.getRowSerializers();

        final Row oneFieldRow = new Row(1);
        oneFieldRow.setField(0, 1024L);
        final Record oneFieldRecord = new Record(1L, oneFieldRow);

        assertEquals(2,
                transformer.matchRecordAndSerializerField(oneFieldRecord, rowSerializers.get(1L)).getRow().getArity());

        final Row threeFieldRow = new Row(3);
        threeFieldRow.setField(0, 1024L);
        threeFieldRow.setField(1, "9527");
        threeFieldRow.setField(2, 2048);
        final Record threeFieldRecord = new Record(1L, threeFieldRow);

        assertEquals(2,
                transformer.matchRecordAndSerializerField(threeFieldRecord, rowSerializers.get(1L)).getRow()
                        .getArity());

    }

    @Test
    public void testSerializerNotMatchRecord() throws Exception {
        final Row row = new Row(2);
        row.setField(0, 1024L);
        row.setField(1, "9527");
        final Record record = new Record(1L, row);

        final int bufferSize = 1024;
        final RecordTransformer transformer = new RecordTransformer(bufferSize);

        final FieldInfo field1 = new FieldInfo("field1", new LongFormatInfo());
        final TestingSinkInfo sinkInfo = new TestingSinkInfo(new FieldInfo[]{field1});
        final DataFlowInfo dataFlowInfo = new DataFlowInfo(1L, new EmptySourceInfo(), sinkInfo);

        transformer.addDataFlow(dataFlowInfo);
        Map<Long, RowSerializer> rowSerializers = transformer.getRowSerializers();

        assertEquals(1,
                transformer.matchRecordAndSerializerField(record, rowSerializers.get(1L)).getRow().getArity());

        final FieldInfo newField1 = new FieldInfo("field1", new LongFormatInfo());
        final FieldInfo newField2 = new FieldInfo("field2", new LongFormatInfo());
        final FieldInfo newField3 = new FieldInfo("field3", new LongFormatInfo());
        final TestingSinkInfo newSinkInfo = new TestingSinkInfo(new FieldInfo[]{newField1, newField2, newField3});
        final DataFlowInfo newDataFlowInfo = new DataFlowInfo(1L, new EmptySourceInfo(), newSinkInfo);

        transformer.addDataFlow(newDataFlowInfo);
        Map<Long, RowSerializer> newRowSerializers = transformer.getRowSerializers();

        assertEquals(3,
                transformer.matchRecordAndSerializerField(record, newRowSerializers.get(1L)).getRow().getArity());
    }

    @Test
    public void testRecordAndSerializerFieldNotMatch() throws Exception {
        final int bufferSize = 1024;
        final RecordTransformer transformer = new RecordTransformer(bufferSize);

        final FieldInfo field1 = new FieldInfo("field1", new LongFormatInfo());
        final FieldInfo field2 = new FieldInfo("field2", new StringFormatInfo());
        final TestingSinkInfo sinkInfo = new TestingSinkInfo(new FieldInfo[]{field1, field2});
        final DataFlowInfo dataFlowInfo = new DataFlowInfo(1L, new EmptySourceInfo(), sinkInfo);
        transformer.addDataFlow(dataFlowInfo);

        final Row row = new Row(2);
        row.setField(0, 1024L);
        row.setField(1, 2048);
        final Record record = new Record(1L, row);

        try {
            transformer.toSerializedRecord(record);
            Assert.fail();
        } catch (Exception expected) {
        }

        final FieldInfo newField1 = new FieldInfo("field1", new LongFormatInfo());
        final FieldInfo newField2 = new FieldInfo("field2", new IntFormatInfo());
        final TestingSinkInfo newSinkInfo = new TestingSinkInfo(new FieldInfo[]{newField1, newField2});
        final DataFlowInfo newDataFlowInfo = new DataFlowInfo(1L, new EmptySourceInfo(), newSinkInfo);

        transformer.updateDataFlow(newDataFlowInfo);
        SerializedRecord serializedRecord = transformer.toSerializedRecord(record);
        transformer.updateDataFlow(dataFlowInfo);

        try {
            transformer.toRecord(serializedRecord);
            Assert.fail();
        } catch (Exception expected) {
        }
    }
}