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

import static org.apache.inlong.sort.flink.transformation.FieldMappingTransformer.SOURCE_FIELD_SKIP_STEP;
import static org.junit.Assert.assertEquals;

import org.apache.flink.types.Row;
import org.apache.inlong.sort.flink.Record;
import org.apache.inlong.sort.formats.common.LongFormatInfo;
import org.apache.inlong.sort.formats.common.StringFormatInfo;
import org.apache.inlong.sort.formats.common.TimestampFormatInfo;
import org.apache.inlong.sort.protocol.DataFlowInfo;
import org.apache.inlong.sort.protocol.FieldInfo;
import org.apache.inlong.sort.protocol.sink.SinkInfo;
import org.apache.inlong.sort.protocol.source.SourceInfo;
import org.apache.inlong.sort.util.TestLogger;
import org.apache.inlong.sort.util.TestingUtils.TestingSinkInfo;
import org.apache.inlong.sort.util.TestingUtils.TestingSourceInfo;
import org.junit.Test;

/**
 * Unit test for {@link FieldMappingTransformer}.
 */
public class FieldMappingTransformerTest extends TestLogger {
    @Test
    public void testTransform() throws Exception {
        final FieldInfo fieldInfo = new FieldInfo("id", new LongFormatInfo());
        final FieldInfo extraFieldInfo = new FieldInfo("not_important", new StringFormatInfo());

        final SourceInfo sourceInfo = new TestingSourceInfo(new FieldInfo[]{extraFieldInfo, fieldInfo});
        final SinkInfo sinkInfo = new TestingSinkInfo(new FieldInfo[]{fieldInfo});
        final long dataFlowId = 1L;
        final DataFlowInfo dataFlowInfo = new DataFlowInfo(dataFlowId, sourceInfo, sinkInfo);

        final FieldMappingTransformer transformer = new FieldMappingTransformer();
        transformer.addDataFlow(dataFlowInfo);
        // should be 4 fields (2 origin fields + time + attr)
        final Row sourceRow = new Row(2 + SOURCE_FIELD_SKIP_STEP);
        sourceRow.setField(0, System.currentTimeMillis());
        sourceRow.setField(1, "attr");
        sourceRow.setField(2, "not important");
        sourceRow.setField(3, 9527L);
        final Record sourceRecord = new Record(dataFlowId, System.currentTimeMillis(), sourceRow);
        final Record sinkRecord = transformer.transform(sourceRecord);
        assertEquals(dataFlowId, sinkRecord.getDataflowId());
        assertEquals(1, sinkRecord.getRow().getArity());
        assertEquals(9527L, sinkRecord.getRow().getField(0));
    }

    @Test
    public void testTransformWithDt() throws Exception {
        final FieldInfo fieldInfo = new FieldInfo("id", new LongFormatInfo());
        final FieldInfo dtFieldInfo = new FieldInfo("dt", new TimestampFormatInfo("MILLIS"));

        final SourceInfo sourceInfo = new TestingSourceInfo(new FieldInfo[]{fieldInfo});
        final SinkInfo sinkInfo = new TestingSinkInfo(new FieldInfo[]{fieldInfo, dtFieldInfo});
        final long dataFlowId = 1L;
        final DataFlowInfo dataFlowInfo = new DataFlowInfo(dataFlowId, sourceInfo, sinkInfo);

        final FieldMappingTransformer transformer = new FieldMappingTransformer();
        transformer.addDataFlow(dataFlowInfo);
        // should be 3 fields (1 origin fields + time + attr)
        final Row sourceRow = new Row(1 + SOURCE_FIELD_SKIP_STEP);
        final long dt = System.currentTimeMillis();
        sourceRow.setField(0, dt);
        sourceRow.setField(1, "attr");
        sourceRow.setField(2, 9527L);
        final Record sourceRecord = new Record(dataFlowId, System.currentTimeMillis(), sourceRow);
        final Record sinkRecord = transformer.transform(sourceRecord);
        assertEquals(dataFlowId, sinkRecord.getDataflowId());
        assertEquals(2, sinkRecord.getRow().getArity());
        assertEquals(9527L, sinkRecord.getRow().getField(0));
        assertEquals(dt, sinkRecord.getRow().getField(1));
    }
}