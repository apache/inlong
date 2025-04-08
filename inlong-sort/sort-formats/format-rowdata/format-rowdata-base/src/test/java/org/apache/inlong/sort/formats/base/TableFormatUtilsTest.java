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

package org.apache.inlong.sort.formats.base;

import org.apache.inlong.common.pojo.sort.dataflow.field.format.ArrayTypeInfo;
import org.apache.inlong.common.pojo.sort.dataflow.field.format.DateFormatInfo;
import org.apache.inlong.common.pojo.sort.dataflow.field.format.FloatFormatInfo;
import org.apache.inlong.common.pojo.sort.dataflow.field.format.FormatInfo;
import org.apache.inlong.common.pojo.sort.dataflow.field.format.IntFormatInfo;
import org.apache.inlong.common.pojo.sort.dataflow.field.format.IntTypeInfo;
import org.apache.inlong.common.pojo.sort.dataflow.field.format.LongTypeInfo;
import org.apache.inlong.common.pojo.sort.dataflow.field.format.RowFormatInfo;
import org.apache.inlong.common.pojo.sort.dataflow.field.format.StringFormatInfo;
import org.apache.inlong.common.pojo.sort.dataflow.field.format.StringTypeInfo;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.junit.Test;

import static org.apache.inlong.sort.formats.base.TableFormatUtils.deserializeBasicField;
import static org.apache.inlong.sort.formats.base.TableFormatUtils.getDataType;
import static org.apache.inlong.sort.formats.base.TableFormatUtils.serializeBasicField;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

/**
 * Tests for {@link TableFormatForRowDataUtils}.
 */
public class TableFormatUtilsTest {

    @Test
    public void testDeserializeStringWithoutNullLiteral() throws Exception {
        Object result1 =
                deserializeBasicField(
                        "f",
                        StringFormatInfo.INSTANCE,
                        "data",
                        null, null);
        assertEquals("data", result1);

        Object result2 =
                deserializeBasicField(
                        "f",
                        StringFormatInfo.INSTANCE,
                        "",
                        null, null);
        assertEquals("", result2);
    }

    @Test
    public void testSerializeStringWithoutNullLiteral() {
        String result1 =
                serializeBasicField(
                        "f",
                        StringFormatInfo.INSTANCE,
                        "data",
                        null);
        assertEquals("data", result1);

        String result2 =
                serializeBasicField(
                        "f",
                        StringFormatInfo.INSTANCE,
                        "",
                        null);
        assertEquals("", result2);
    }

    @Test
    public void testDeserializeStringWithNullLiteral() throws Exception {
        Object result1 =
                deserializeBasicField(
                        "f",
                        StringFormatInfo.INSTANCE,
                        "data",
                        "n/a", null);
        assertEquals("data", result1);

        Object result2 =
                deserializeBasicField(
                        "f",
                        StringFormatInfo.INSTANCE,
                        "",
                        "n/a", null);
        assertEquals("", result2);

        Object result3 =
                deserializeBasicField(
                        "f",
                        StringFormatInfo.INSTANCE,
                        "n/a",
                        "n/a", null);
        assertNull(result3);
    }

    @Test
    public void testSerializeStringWithNullLiteral() {
        String result1 =
                serializeBasicField(
                        "f",
                        StringFormatInfo.INSTANCE,
                        "data",
                        "n/a");
        assertEquals("data", result1);

        String result2 =
                serializeBasicField(
                        "f",
                        StringFormatInfo.INSTANCE,
                        "",
                        "n/a");
        assertEquals("", result2);

        String result3 =
                serializeBasicField(
                        "f",
                        StringFormatInfo.INSTANCE,
                        null,
                        "n/a");
        assertEquals("n/a", result3);
    }

    @Test
    public void testDeserializeNumberWithoutNullLiteral() throws Exception {
        Object result1 =
                deserializeBasicField(
                        "f",
                        IntFormatInfo.INSTANCE,
                        "1",
                        null, null);
        assertEquals(1, result1);

        Object result2 =
                deserializeBasicField(
                        "f",
                        IntFormatInfo.INSTANCE,
                        "",
                        null, null);
        assertNull(result2);
    }

    @Test
    public void testSerializeNumberWithoutNullLiteral() {
        String result1 =
                serializeBasicField(
                        "f",
                        IntFormatInfo.INSTANCE,
                        1,
                        null);
        assertEquals("1", result1);

        String result2 =
                serializeBasicField(
                        "f",
                        IntFormatInfo.INSTANCE,
                        null,
                        null);
        assertEquals("", result2);
    }

    @Test
    public void testDeserializeNumberWithNullLiteral() throws Exception {
        Object result1 =
                deserializeBasicField(
                        "f",
                        IntFormatInfo.INSTANCE,
                        "1",
                        "n/a", null);
        assertEquals(1, result1);

        try {
            Object result2 = deserializeBasicField(
                    "f",
                    IntFormatInfo.INSTANCE,
                    "",
                    "n/a", null);

            assertNull(result2);
        } catch (Exception e) {
            // ignored
        }

        Object result3 =
                deserializeBasicField(
                        "f",
                        IntFormatInfo.INSTANCE,
                        "n/a",
                        "n/a", null);
        assertNull(result3);
    }

    @Test
    public void testSerializeNumberWithNullLiteral() {
        String result1 =
                serializeBasicField(
                        "f",
                        IntFormatInfo.INSTANCE,
                        1,
                        "n/a");
        assertEquals("1", result1);

        String result2 =
                serializeBasicField(
                        "f",
                        IntFormatInfo.INSTANCE,
                        null,
                        "n/a");
        assertEquals("n/a", result2);
    }

    @Test
    public void testGetDataType() {
        DataType stringType = getDataType(new StringTypeInfo());
        assertEquals(LogicalTypeRoot.VARCHAR, stringType.getLogicalType().getTypeRoot());
        assertEquals(String.class, stringType.getConversionClass());

        DataType intType = getDataType(new IntTypeInfo());
        assertEquals(LogicalTypeRoot.INTEGER, intType.getLogicalType().getTypeRoot());
        assertEquals(Integer.class, intType.getConversionClass());

        DataType arrayLongType = getDataType(new ArrayTypeInfo(new LongTypeInfo()));
        assertEquals(LogicalTypeRoot.ARRAY, arrayLongType.getLogicalType().getTypeRoot());
        assertEquals(Long[].class, arrayLongType.getConversionClass());
    }

    @Test
    public void testProjectRowFormatInfo() {
        DataType dataType = ResolvedSchema.of(
                Column.physical("student_name", DataTypes.STRING()),
                Column.physical("score", DataTypes.FLOAT()),
                Column.physical("date", DataTypes.DATE()))
                .toPhysicalRowDataType();
        RowFormatInfo rowFormatInfo = new RowFormatInfo(
                new String[]{"student_name", "score", "date"},
                new FormatInfo[]{
                        StringFormatInfo.INSTANCE,
                        FloatFormatInfo.INSTANCE,
                        new DateFormatInfo("yyyy-MM-dd")
                });

        RowFormatInfo projectedRowFormatInfo =
                TableFormatForRowDataUtils.projectRowFormatInfo(rowFormatInfo, dataType);
        assertEquals(rowFormatInfo, projectedRowFormatInfo);
    }

    @Test
    public void testProjectRowFormatInfoWithProjection() {
        DataType dataType = ResolvedSchema.of(
                Column.physical("date", DataTypes.DATE()),
                Column.physical("score", DataTypes.FLOAT()))
                .toPhysicalRowDataType();
        RowFormatInfo rowFormatInfo = new RowFormatInfo(
                new String[]{"student_name", "score", "date"},
                new FormatInfo[]{
                        StringFormatInfo.INSTANCE,
                        FloatFormatInfo.INSTANCE,
                        new DateFormatInfo("yyyy-MM-dd")
                });

        RowFormatInfo projectedRowFormatInfo =
                TableFormatForRowDataUtils.projectRowFormatInfo(rowFormatInfo, dataType);
        assertEquals(2, projectedRowFormatInfo.getFieldNames().length);
        assertEquals(2, projectedRowFormatInfo.getFieldFormatInfos().length);
        assertEquals("date", projectedRowFormatInfo.getFieldNames()[0]);
        assertEquals("score", projectedRowFormatInfo.getFieldNames()[1]);
        assertEquals(new DateFormatInfo("yyyy-MM-dd"),
                projectedRowFormatInfo.getFieldFormatInfos()[0]);
        assertEquals(FloatFormatInfo.INSTANCE,
                projectedRowFormatInfo.getFieldFormatInfos()[1]);
    }
}
