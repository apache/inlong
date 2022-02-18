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

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.types.Row;
import org.apache.inlong.sort.formats.common.DateFormatInfo;
import org.apache.inlong.sort.formats.common.StringFormatInfo;
import org.apache.inlong.sort.formats.common.TimeFormatInfo;
import org.apache.inlong.sort.formats.common.TimestampFormatInfo;
import org.apache.inlong.sort.protocol.FieldInfo;
import org.junit.Test;

import java.io.IOException;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.text.ParseException;

import static org.apache.inlong.sort.singletenant.flink.utils.CommonUtils.extractFormatInfos;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class CustomDateFormatDeserializationSchemaWrapperTest {

    @Test
    public void testFromStringToDateAndTime() throws IOException, ClassNotFoundException, ParseException {
        FieldInfo[] fieldInfos = new FieldInfo[]{
                new FieldInfo("f1", new DateFormatInfo()),
                new FieldInfo("f2", new TimeFormatInfo()),
                new FieldInfo("f3", new TimestampFormatInfo()),
                new FieldInfo("f4", StringFormatInfo.INSTANCE)
        };

        DeserializationSchema<Row> stringSchema = DeserializationSchemaFactory.build(fieldInfos, null);
        CustomDateFormatDeserializationSchemaWrapper schemaWrapper
                = new CustomDateFormatDeserializationSchemaWrapper(stringSchema, extractFormatInfos(fieldInfos));

        Row testRow = Row.of("2022-02-15", "15:52:30", "2022-02-15 15:52:30", "don't convert");
        Row resultRow = schemaWrapper.fromStringToDateAndTime(testRow);
        assertTrue(resultRow.getField(0) instanceof Date);
        assertTrue(resultRow.getField(1) instanceof Time);
        assertTrue(resultRow.getField(2) instanceof Timestamp);

        final Row expectedRow = Row.of(Date.valueOf("2022-02-15"), Time.valueOf("15:52:30"),
                Timestamp.valueOf("2022-02-15 15:52:30"), "don't convert");
        assertEquals(expectedRow, resultRow);
    }

}
