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

import java.util.HashMap;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.apache.inlong.sort.configuration.Configuration;
import org.apache.inlong.sort.formats.common.IntFormatInfo;
import org.apache.inlong.sort.formats.common.LongFormatInfo;
import org.apache.inlong.sort.formats.common.ShortFormatInfo;
import org.apache.inlong.sort.formats.common.StringFormatInfo;
import org.apache.inlong.sort.formats.common.TimestampFormatInfo;
import org.apache.inlong.sort.protocol.BuiltInFieldInfo;
import org.apache.inlong.sort.protocol.BuiltInFieldInfo.BuiltInField;
import org.apache.inlong.sort.protocol.FieldInfo;
import org.junit.Test;

import java.sql.Timestamp;

import static org.apache.inlong.sort.configuration.Constants.SINK_FIELD_TYPE_INT_NULLABLE;
import static org.apache.inlong.sort.configuration.Constants.SINK_FIELD_TYPE_LONG_NULLABLE;
import static org.apache.inlong.sort.configuration.Constants.SINK_FIELD_TYPE_SHORT_NULLABLE;
import static org.junit.Assert.assertEquals;

public class FieldMappingTransformerTest {

    @Test
    public void testTransform() {
        final FieldInfo[] fieldInfos = new FieldInfo[] {
                new BuiltInFieldInfo("dt", new TimestampFormatInfo(), BuiltInFieldInfo.BuiltInField.DATA_TIME),
                new FieldInfo("f1", IntFormatInfo.INSTANCE),
                new FieldInfo("f2", StringFormatInfo.INSTANCE),
                new FieldInfo("f3", IntFormatInfo.INSTANCE),
                new FieldInfo("f4", ShortFormatInfo.INSTANCE),
                new FieldInfo("f5", LongFormatInfo.INSTANCE),
                new BuiltInFieldInfo("event_type", new StringFormatInfo(), BuiltInField.MYSQL_METADATA_EVENT_TYPE)
        };
        final long ms = 10000000000L;
        final Configuration configuration = new Configuration();
        configuration.setBoolean(SINK_FIELD_TYPE_INT_NULLABLE, false);
        configuration.setBoolean(SINK_FIELD_TYPE_SHORT_NULLABLE, false);
        configuration.setBoolean(SINK_FIELD_TYPE_LONG_NULLABLE, false);
        FieldMappingTransformer transformer = new FieldMappingTransformer(configuration, fieldInfos);
        Row resultRow = transformer.transform(Row.of(new HashMap<>(), 1), ms);
        resultRow.setKind(RowKind.UPDATE_AFTER);

        assertEquals(7, resultRow.getArity());
        assertEquals(new Timestamp(ms), resultRow.getField(0));
        assertEquals(1, resultRow.getField(1));
        assertEquals("", resultRow.getField(2));
        assertEquals(0, resultRow.getField(3));
        assertEquals(0, resultRow.getField(4));
        assertEquals(0L, resultRow.getField(5));
        assertEquals("INSERT", resultRow.getField(6));
    }
}
