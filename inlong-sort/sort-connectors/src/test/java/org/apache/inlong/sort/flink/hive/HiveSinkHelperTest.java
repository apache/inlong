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

package org.apache.inlong.sort.flink.hive;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.TimeType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.inlong.sort.formats.common.IntFormatInfo;
import org.apache.inlong.sort.formats.common.StringFormatInfo;
import org.apache.inlong.sort.formats.common.TimeFormatInfo;
import org.apache.inlong.sort.protocol.FieldInfo;
import org.apache.inlong.sort.protocol.sink.HiveSinkInfo;
import org.apache.inlong.sort.protocol.sink.HiveSinkInfo.HiveFieldPartitionInfo;
import org.apache.inlong.sort.protocol.sink.HiveSinkInfo.HivePartitionInfo;
import org.apache.inlong.sort.protocol.sink.HiveSinkInfo.TextFileFormat;
import org.junit.Test;

public class HiveSinkHelperTest {

    @Test
    public void testGetNonPartitionFields() {
        final FieldInfo[] fieldInfos = new FieldInfo[]{
                new FieldInfo("string", StringFormatInfo.INSTANCE),
                new FieldInfo("int", IntFormatInfo.INSTANCE),
                new FieldInfo("timestamp", new TimeFormatInfo("MILLIS"))
        };

        final HivePartitionInfo[] partitionInfos = new HivePartitionInfo[]{
                new HiveFieldPartitionInfo("string"),
                new HiveFieldPartitionInfo("int")
        };

        final HiveSinkInfo hiveSinkInfo = new HiveSinkInfo(
                fieldInfos,
                "jdbc",
                "db",
                "table",
                "user",
                "password",
                "path",
                partitionInfos,
                new TextFileFormat(','));

        final FieldInfo[] nonPartitionFieldInfos = HiveSinkHelper.getNonPartitionFields(hiveSinkInfo);
        assertEquals(1, nonPartitionFieldInfos.length);
        assertEquals("timestamp", nonPartitionFieldInfos[0].getName());
        assertTrue(nonPartitionFieldInfos[0].getFormatInfo() instanceof TimeFormatInfo);
    }

    @Test
    public void testGetFieldNames() {
        final FieldInfo[] fieldInfos = new FieldInfo[]{
                new FieldInfo("string", StringFormatInfo.INSTANCE),
                new FieldInfo("int", IntFormatInfo.INSTANCE),
                new FieldInfo("timestamp", new TimeFormatInfo("MILLIS"))
        };
        final String[] fieldNames = HiveSinkHelper.getFieldNames(fieldInfos);
        assertEquals(3, fieldNames.length);
        assertEquals("string", fieldNames[0]);
        assertEquals("int", fieldNames[1]);
        assertEquals("timestamp", fieldNames[2]);
    }

    @Test
    public void testGetFieldLogicalTypes() {
        final FieldInfo[] fieldInfos = new FieldInfo[]{
                new FieldInfo("string", StringFormatInfo.INSTANCE),
                new FieldInfo("int", IntFormatInfo.INSTANCE),
                new FieldInfo("timestamp", new TimeFormatInfo("MILLIS"))
        };
        final LogicalType[] fieldTypes = HiveSinkHelper.getFieldLogicalTypes(fieldInfos);
        assertEquals(3, fieldTypes.length);
        assertTrue(fieldTypes[0] instanceof VarCharType);
        assertTrue(fieldTypes[1] instanceof IntType);
        assertTrue(fieldTypes[2] instanceof TimeType);
    }
}
