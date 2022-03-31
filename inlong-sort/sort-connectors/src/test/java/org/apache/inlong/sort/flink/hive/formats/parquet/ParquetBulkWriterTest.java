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

package org.apache.inlong.sort.flink.hive.formats.parquet;

import static org.apache.inlong.sort.flink.hive.formats.parquet.ParquetRowWriter.ARRAY_FIELD_NAME;
import static org.apache.inlong.sort.flink.hive.formats.parquet.ParquetRowWriter.MAP_ENTITY_FIELD_NAME;
import static org.apache.inlong.sort.flink.hive.formats.parquet.ParquetRowWriter.MAP_KEY_FIELD_NAME;
import static org.apache.inlong.sort.flink.hive.formats.parquet.ParquetRowWriter.MAP_VALUE_FIELD_NAME;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;

import java.io.File;
import java.io.IOException;
import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.Map;

import org.apache.flink.core.fs.local.LocalDataOutputStream;
import org.apache.flink.formats.parquet.ParquetBulkWriter;
import org.apache.flink.types.Row;
import org.apache.hadoop.fs.Path;
import org.apache.inlong.sort.configuration.Configuration;
import org.apache.inlong.sort.flink.hive.HiveSinkHelper;
import org.apache.inlong.sort.formats.common.ArrayFormatInfo;
import org.apache.inlong.sort.formats.common.BooleanFormatInfo;
import org.apache.inlong.sort.formats.common.ByteFormatInfo;
import org.apache.inlong.sort.formats.common.DateFormatInfo;
import org.apache.inlong.sort.formats.common.DecimalFormatInfo;
import org.apache.inlong.sort.formats.common.DoubleFormatInfo;
import org.apache.inlong.sort.formats.common.FloatFormatInfo;
import org.apache.inlong.sort.formats.common.IntFormatInfo;
import org.apache.inlong.sort.formats.common.LongFormatInfo;
import org.apache.inlong.sort.formats.common.MapFormatInfo;
import org.apache.inlong.sort.formats.common.ShortFormatInfo;
import org.apache.inlong.sort.formats.common.StringFormatInfo;
import org.apache.inlong.sort.formats.common.TimeFormatInfo;
import org.apache.inlong.sort.formats.common.TimestampFormatInfo;
import org.apache.inlong.sort.protocol.FieldInfo;
import org.apache.inlong.sort.protocol.sink.HiveSinkInfo;
import org.apache.inlong.sort.protocol.sink.HiveSinkInfo.HivePartitionInfo;
import org.apache.inlong.sort.protocol.sink.HiveSinkInfo.ParquetFileFormat;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.ParquetReader.Builder;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class ParquetBulkWriterTest {
    @ClassRule
    public static TemporaryFolder temporaryFolder = new TemporaryFolder();

    @Test
    public void testAllSupportedTypes() throws IOException {
        File testFile = temporaryFolder.newFile("test.parquet");
        final ParquetBulkWriter<Row> parquetBulkWriter = (ParquetBulkWriter<Row>) HiveSinkHelper
                .createBulkWriterFactory(prepareHiveSinkInfo(), new Configuration())
                .create(new LocalDataOutputStream(testFile));

        final int[] testArray = new int[] {1, 2, 3};
        Map<String, Double> testMap = new HashMap<>();
        testMap.put("key1", 1.0);
        testMap.put("key2", 2.0);
        testMap.put("key3", 3.0);
        final Integer[] testEmptyArray = new Integer[] {};
        final Map<String, Double> testEmptyMap = new HashMap<>();
        parquetBulkWriter.addElement(Row.of(
                "string",
                false,
                (byte) 1,
                (short) 11,
                111,
                1111L,
                (float) 1.1,
                1.11,
                new BigDecimal("123456789123456789"),
                new Date(24 * 60 * 60 * 1000),
                new Time(0),
                new Timestamp(0),
                testArray,
                testMap,
                testEmptyArray,
                testEmptyMap
        ));
        parquetBulkWriter.finish();

        GroupReadSupport readSupport = new GroupReadSupport();
        Builder<Group> builder = ParquetReader.builder(readSupport, new Path(testFile.toURI()));
        ParquetReader<Group> reader = builder.build();
        Group line = reader.read();

        assertNotNull(line);
        assertEquals("string", line.getString(0, 0));
        assertFalse(line.getBoolean(1, 0));
        assertEquals(1, line.getInteger(2, 0));
        assertEquals(11, line.getInteger(3, 0));
        assertEquals(111, line.getInteger(4, 0));
        assertEquals(1111, line.getLong(5, 0));
        assertEquals(1.1, line.getFloat(6, 0), 0.01);
        assertEquals(1.11, line.getDouble(7, 0), 0.001);
        assertEquals("123456789123456789", line.getString(8, 0));
        assertEquals(1, line.getInteger(9, 0));
        assertEquals(0, line.getInteger(10, 0));
        assertEquals(ParquetRowWriter.timestampToInt96(new Timestamp(0)), line.getInt96(11, 0));

        Group f13 = line.getGroup("f13", 0);
        assertEquals(1, f13.getGroup(ARRAY_FIELD_NAME, 0).getInteger(0, 0));
        assertEquals(2, f13.getGroup(ARRAY_FIELD_NAME, 1).getInteger(0, 0));
        assertEquals(3, f13.getGroup(ARRAY_FIELD_NAME, 2).getInteger(0, 0));

        Group f14 = line.getGroup("f14", 0);
        assertEquals("key1", f14.getGroup(MAP_ENTITY_FIELD_NAME, 0).getString(MAP_KEY_FIELD_NAME, 0));
        assertEquals(1.0, f14.getGroup(MAP_ENTITY_FIELD_NAME, 0).getDouble(MAP_VALUE_FIELD_NAME, 0), 0.01);
        assertEquals("key2", f14.getGroup(MAP_ENTITY_FIELD_NAME, 1).getString(MAP_KEY_FIELD_NAME, 0));
        assertEquals(2.0, f14.getGroup(MAP_ENTITY_FIELD_NAME, 1).getDouble(MAP_VALUE_FIELD_NAME, 0), 0.01);
        assertEquals("key3", f14.getGroup(MAP_ENTITY_FIELD_NAME, 2).getString(MAP_KEY_FIELD_NAME, 0));
        assertEquals(3.0, f14.getGroup(MAP_ENTITY_FIELD_NAME, 2).getDouble(MAP_VALUE_FIELD_NAME, 0), 0.01);

        assertEquals("", line.getGroup("f15", 0).toString());
        assertEquals("", line.getGroup("f16", 0).toString());
    }

    private HiveSinkInfo prepareHiveSinkInfo() {
        return new HiveSinkInfo(
                new FieldInfo[] {
                        new FieldInfo("f1", StringFormatInfo.INSTANCE),
                        new FieldInfo("f2", BooleanFormatInfo.INSTANCE),
                        new FieldInfo("f3", ByteFormatInfo.INSTANCE),
                        new FieldInfo("f4", ShortFormatInfo.INSTANCE),
                        new FieldInfo("f5", IntFormatInfo.INSTANCE),
                        new FieldInfo("f6", LongFormatInfo.INSTANCE),
                        new FieldInfo("f7", FloatFormatInfo.INSTANCE),
                        new FieldInfo("f8", DoubleFormatInfo.INSTANCE),
                        new FieldInfo("f9", DecimalFormatInfo.INSTANCE),
                        new FieldInfo("f10", new DateFormatInfo()),
                        new FieldInfo("f11", new TimeFormatInfo()),
                        new FieldInfo("f12", new TimestampFormatInfo()),
                        new FieldInfo("f13", new ArrayFormatInfo(IntFormatInfo.INSTANCE)),
                        new FieldInfo("f14", new MapFormatInfo(StringFormatInfo.INSTANCE, DoubleFormatInfo.INSTANCE)),
                        new FieldInfo("f15", new ArrayFormatInfo(IntFormatInfo.INSTANCE)),
                        new FieldInfo("f16", new MapFormatInfo(StringFormatInfo.INSTANCE, DoubleFormatInfo.INSTANCE))
                },
                "jdbc:mysql://127.0.0.1:3306/testDatabaseName",
                "testDatabaseName",
                "testTableName",
                "testUsername",
                "testPassword",
                "/path",
                new HivePartitionInfo[] {
                },
                new ParquetFileFormat()
        );
    }
}
