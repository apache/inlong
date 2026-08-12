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

package org.apache.inlong.sdk.transform.process.processor;

import org.apache.inlong.common.pojo.sort.dataflow.field.format.ArrayFormatInfo;
import org.apache.inlong.common.pojo.sort.dataflow.field.format.FormatInfo;
import org.apache.inlong.common.pojo.sort.dataflow.field.format.RowFormatInfo;
import org.apache.inlong.common.pojo.sort.dataflow.field.format.StringFormatInfo;
import org.apache.inlong.sdk.transform.decode.SourceDecoderFactory;
import org.apache.inlong.sdk.transform.encode.SinkEncoderFactory;
import org.apache.inlong.sdk.transform.pojo.FieldInfo;
import org.apache.inlong.sdk.transform.pojo.JsonSourceInfo;
import org.apache.inlong.sdk.transform.pojo.RowDataSinkInfo;
import org.apache.inlong.sdk.transform.pojo.TransformConfig;
import org.apache.inlong.sdk.transform.process.TransformProcessor;

import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.List;

public class TestJson2RowDataProcessor extends AbstractProcessorTestBase {

    @Test
    public void testJson2RowData() throws Exception {
        List<FieldInfo> sinkFields = this.getTestFieldList("audit_data_time", "session_begin_time", "session_id",
                "business", "product_id", "channel",
                "agent_id", "archive_p1", "archive_p2",
                "archive_p3", "archive_p4", "array_field");
        // sql
        String transformSql = "select '' as audit_data_time,"
                + "$root.session_begin_time as session_begin_time,"
                + "$root.session_id as session_id,"
                + "$root.business as business,"
                + "$root.product_id as product_id,"
                + "$root.channel as channel,"
                + "$root.agent_id as agent_id,"
                + "$root.archive_p1 as archive_p1,"
                + "$root.archive_p2 as archive_p2,"
                + "$root.archive_p3 as archive_p3,"
                + "$root.archive_p4 as archive_p4,"
                + "$root.array_field as array_field from source";
        // case1
        TransformProcessor<String, RowData> processor = TransformProcessor.create(
                new TransformConfig(transformSql),
                SourceDecoderFactory.createJsonDecoder(new JsonSourceInfo("UTF-8", null)),
                SinkEncoderFactory.createRowEncoder(new RowDataSinkInfo("UTF-8", sinkFields)));
        String strJson =
                "{\"session_id\":\"1782780884\",\"session_begin_time\":\"2026-06-30 08:54:56\",\"business\":\"pay\","
                        + "\"product_id\":\"1314\",\"channel\":\"todo\",\"agent_id\":\"095d2\",\"archive_p1\":\"money\","
                        + "\"archive_p2\":\"product\",\"archive_p3\":\"short”\",\"archive_p4\":\"\",\"array_field\":[{\"isArray\":true}]}";
        List<RowData> output = processor.transform(strJson, new HashMap<>());
        Assert.assertEquals(1, output.size());
        Assert.assertEquals(output.get(0).getString(9).toString(), "short”");
        Assert.assertEquals(output.get(0).getString(11).toString(), "[{\"isArray\":true}]");
    }

    @Test
    public void testJsonExtractStructWithObject() throws Exception {
        List<FieldInfo> sinkFields = this.getTestFieldList("name", "age");
        // person struct sink field
        FieldInfo personStruct = new FieldInfo("personStruct");
        RowFormatInfo personStructFormat = new RowFormatInfo(
                new String[]{"name", "age"},
                new FormatInfo[]{new StringFormatInfo(), new StringFormatInfo()});
        personStruct.setFormatInfo(personStructFormat);
        sinkFields.add(personStruct);
        // items struct sink field (array of struct)
        FieldInfo itemsStruct = new FieldInfo("itemsStruct");
        RowFormatInfo itemsRowFormat = new RowFormatInfo(
                new String[]{"id", "value"},
                new FormatInfo[]{new StringFormatInfo(), new StringFormatInfo()});
        ArrayFormatInfo itemsStructFormat = new ArrayFormatInfo(itemsRowFormat);
        itemsStruct.setFormatInfo(itemsStructFormat);
        sinkFields.add(itemsStruct);

        RowDataSinkInfo rowSink = new RowDataSinkInfo("UTF-8", sinkFields);

        String transformSql = "select $root.name as name,"
                + "$root.age as age,"
                + "json_extract_struct($root.person,name,age) as personStruct,"
                + "json_extract_struct($root.items,id,value) as itemsStruct"
                + " from source";

        TransformConfig config = new TransformConfig(transformSql);
        TransformProcessor<String, RowData> processor = TransformProcessor.create(
                config,
                SourceDecoderFactory.createJsonDecoder(new JsonSourceInfo("UTF-8", null)),
                SinkEncoderFactory.createRowEncoder(rowSink));

        String strJson = "{\"name\":\"John\",\"age\":30,"
                + "\"person\":{\"name\":\"Jane\",\"age\":25},"
                + "\"items\":[{\"id\":\"1\",\"value\":\"item1\"},{\"id\":\"2\",\"value\":\"item2\"}]}";

        List<RowData> output = processor.transform(strJson, new HashMap<>());
        Assert.assertEquals(1, output.size());

        // verify basic fields
        Assert.assertEquals("John", output.get(0).getString(0).toString());
        Assert.assertEquals("30", output.get(0).getString(1).toString());

        // verify personStruct: GenericRowData with name=Jane, age=25
        GenericRowData personRow = (GenericRowData) output.get(0).getRow(2, 2);
        Assert.assertEquals("Jane", personRow.getString(0).toString());
        Assert.assertEquals("25", personRow.getString(1).toString());

        // verify itemsStruct: GenericArrayData of GenericRowData
        GenericArrayData itemsArray = (GenericArrayData) output.get(0).getArray(3);
        Assert.assertEquals(2, itemsArray.size());
        RowData item0 = itemsArray.getRow(0, 2);
        Assert.assertEquals("1", item0.getString(0).toString());
        Assert.assertEquals("item1", item0.getString(1).toString());
        RowData item1 = itemsArray.getRow(1, 2);
        Assert.assertEquals("2", item1.getString(0).toString());
        Assert.assertEquals("item2", item1.getString(1).toString());
    }

    @Test
    public void testJsonExtractStructWithMissingField() throws Exception {
        List<FieldInfo> sinkFields = this.getTestFieldList("personStruct");
        FieldInfo personStruct = sinkFields.get(0);
        RowFormatInfo personStructFormat = new RowFormatInfo(
                new String[]{"name", "age", "missing_field"},
                new FormatInfo[]{new StringFormatInfo(), new StringFormatInfo(), new StringFormatInfo()});
        personStruct.setFormatInfo(personStructFormat);

        RowDataSinkInfo rowSink = new RowDataSinkInfo("UTF-8", sinkFields);

        String transformSql =
                "select json_extract_struct($root.person,name,age,missing_field) as personStruct from source";

        TransformConfig config = new TransformConfig(transformSql);
        TransformProcessor<String, RowData> processor = TransformProcessor.create(
                config,
                SourceDecoderFactory.createJsonDecoder(new JsonSourceInfo("UTF-8", null)),
                SinkEncoderFactory.createRowEncoder(rowSink));

        String strJson = "{\"person\":{\"name\":\"Jane\",\"age\":25}}";

        List<RowData> output = processor.transform(strJson, new HashMap<>());
        Assert.assertEquals(1, output.size());

        GenericRowData personRow = (GenericRowData) output.get(0).getRow(0, 3);
        Assert.assertEquals("Jane", personRow.getString(0).toString());
        Assert.assertEquals("25", personRow.getString(1).toString());
        Assert.assertNull(personRow.getString(2)); // missing field returns null
    }

    @Test
    public void testJsonExtractStructWithNestedPath() throws Exception {
        List<FieldInfo> sinkFields = this.getTestFieldList("addressStruct");
        FieldInfo addressStruct = sinkFields.get(0);
        RowFormatInfo addressStructFormat = new RowFormatInfo(
                new String[]{"city", "zip"},
                new FormatInfo[]{new StringFormatInfo(), new StringFormatInfo()});
        addressStruct.setFormatInfo(addressStructFormat);

        RowDataSinkInfo rowSink = new RowDataSinkInfo("UTF-8", sinkFields);

        String transformSql =
                "select json_extract_struct($root.address,city,zip) as addressStruct from source";

        TransformConfig config = new TransformConfig(transformSql);
        TransformProcessor<String, RowData> processor = TransformProcessor.create(
                config,
                SourceDecoderFactory.createJsonDecoder(new JsonSourceInfo("UTF-8", null)),
                SinkEncoderFactory.createRowEncoder(rowSink));

        String strJson = "{\"address\":{\"city\":\"NYC\",\"zip\":\"10001\"}}";

        List<RowData> output = processor.transform(strJson, new HashMap<>());
        Assert.assertEquals(1, output.size());

        GenericRowData addressRow = (GenericRowData) output.get(0).getRow(0, 2);
        Assert.assertEquals("NYC", addressRow.getString(0).toString());
        Assert.assertEquals("10001", addressRow.getString(1).toString());
    }

    @Test
    public void testJsonExtractStructWithNonExistentPath() throws Exception {
        List<FieldInfo> sinkFields = this.getTestFieldList("personStruct");
        FieldInfo personStruct = sinkFields.get(0);
        RowFormatInfo personStructFormat = new RowFormatInfo(
                new String[]{"name", "age"},
                new FormatInfo[]{new StringFormatInfo(), new StringFormatInfo()});
        personStruct.setFormatInfo(personStructFormat);

        RowDataSinkInfo rowSink = new RowDataSinkInfo("UTF-8", sinkFields);

        String transformSql =
                "select json_extract_struct($root.non_existent_path,name,age) as personStruct from source";

        TransformConfig config = new TransformConfig(transformSql);
        TransformProcessor<String, RowData> processor = TransformProcessor.create(
                config,
                SourceDecoderFactory.createJsonDecoder(new JsonSourceInfo("UTF-8", null)),
                SinkEncoderFactory.createRowEncoder(rowSink));

        String strJson = "{\"person\":{\"name\":\"Jane\",\"age\":25}}";

        List<RowData> output = processor.transform(strJson, new HashMap<>());
        Assert.assertEquals(1, output.size());
        // non-existent path should return null struct
        Assert.assertTrue(output.get(0).isNullAt(0));
    }

    @Test
    public void testJsonExtractStructWithEmptyArray() throws Exception {
        List<FieldInfo> sinkFields = this.getTestFieldList("itemsStruct");
        FieldInfo itemsStruct = sinkFields.get(0);
        RowFormatInfo itemsRowFormat = new RowFormatInfo(
                new String[]{"id", "value"},
                new FormatInfo[]{new StringFormatInfo(), new StringFormatInfo()});
        ArrayFormatInfo itemsStructFormat = new ArrayFormatInfo(itemsRowFormat);
        itemsStruct.setFormatInfo(itemsStructFormat);

        RowDataSinkInfo rowSink = new RowDataSinkInfo("UTF-8", sinkFields);

        String transformSql =
                "select json_extract_struct($root.items,id,value) as itemsStruct from source";

        TransformConfig config = new TransformConfig(transformSql);
        TransformProcessor<String, RowData> processor = TransformProcessor.create(
                config,
                SourceDecoderFactory.createJsonDecoder(new JsonSourceInfo("UTF-8", null)),
                SinkEncoderFactory.createRowEncoder(rowSink));

        String strJson = "{\"items\":[]}";

        List<RowData> output = processor.transform(strJson, new HashMap<>());
        Assert.assertEquals(1, output.size());

        GenericArrayData itemsArray = (GenericArrayData) output.get(0).getArray(0);
        Assert.assertEquals(0, itemsArray.size());
    }

    @Test
    public void testJsonExtractStructWithPrimitiveArray() throws Exception {
        List<FieldInfo> sinkFields = this.getTestFieldList("tagsStruct");
        FieldInfo tagsStruct = sinkFields.get(0);
        ArrayFormatInfo tagsStructFormat = new ArrayFormatInfo(new StringFormatInfo());
        tagsStruct.setFormatInfo(tagsStructFormat);

        RowDataSinkInfo rowSink = new RowDataSinkInfo("UTF-8", sinkFields);

        String transformSql =
                "select json_extract_struct($root.data,tag) as tagsStruct from source";

        TransformConfig config = new TransformConfig(transformSql);
        TransformProcessor<String, RowData> processor = TransformProcessor.create(
                config,
                SourceDecoderFactory.createJsonDecoder(new JsonSourceInfo("UTF-8", null)),
                SinkEncoderFactory.createRowEncoder(rowSink));

        String strJson = "{\"data\":[{\"tag\":\"a\"},{\"tag\":\"b\"},{\"tag\":\"c\"}]}";

        List<RowData> output = processor.transform(strJson, new HashMap<>());
        Assert.assertEquals(1, output.size());

        GenericArrayData tagsArray = (GenericArrayData) output.get(0).getArray(0);
        Assert.assertEquals(3, tagsArray.size());
    }

    // ========== JsonExtractStructExcludingFunction tests ==========

    @Test
    public void testJsonExtractStructExcludingWithObject() throws Exception {
        List<FieldInfo> sinkFields = this.getTestFieldList("name", "age");
        // personStruct sink field: after excluding email,phone from person object,
        // the remaining fields are name,age in original JSON order
        FieldInfo personStruct = new FieldInfo("personStruct");
        RowFormatInfo personStructFormat = new RowFormatInfo(
                new String[]{"name", "age"},
                new FormatInfo[]{new StringFormatInfo(), new StringFormatInfo()});
        personStruct.setFormatInfo(personStructFormat);
        sinkFields.add(personStruct);
        // itemsStruct sink field: after excluding extra from items array elements,
        // the remaining fields are id,value
        FieldInfo itemsStruct = new FieldInfo("itemsStruct");
        RowFormatInfo itemsRowFormat = new RowFormatInfo(
                new String[]{"id", "value"},
                new FormatInfo[]{new StringFormatInfo(), new StringFormatInfo()});
        ArrayFormatInfo itemsStructFormat = new ArrayFormatInfo(itemsRowFormat);
        itemsStruct.setFormatInfo(itemsStructFormat);
        sinkFields.add(itemsStruct);

        RowDataSinkInfo rowSink = new RowDataSinkInfo("UTF-8", sinkFields);

        String transformSql = "select $root.name as name,"
                + "$root.age as age,"
                + "json_extract_struct_excluding($root.person,email,phone) as personStruct,"
                + "json_extract_struct_excluding($root.items,extra) as itemsStruct"
                + " from source";

        TransformConfig config = new TransformConfig(transformSql);
        TransformProcessor<String, RowData> processor = TransformProcessor.create(
                config,
                SourceDecoderFactory.createJsonDecoder(new JsonSourceInfo("UTF-8", null)),
                SinkEncoderFactory.createRowEncoder(rowSink));

        String strJson = "{\"name\":\"John\",\"age\":30,"
                + "\"person\":{\"name\":\"Jane\",\"age\":25,\"email\":\"jane@test.com\",\"phone\":\"123456\"},"
                + "\"items\":[{\"id\":\"1\",\"value\":\"item1\",\"extra\":\"x1\"},"
                + "{\"id\":\"2\",\"value\":\"item2\",\"extra\":\"x2\"}]}";

        List<RowData> output = processor.transform(strJson, new HashMap<>());
        Assert.assertEquals(1, output.size());

        // verify basic fields
        Assert.assertEquals("John", output.get(0).getString(0).toString());
        Assert.assertEquals("30", output.get(0).getString(1).toString());

        // verify personStruct: GenericRowData with name=Jane, age=25 (email,phone excluded)
        GenericRowData personRow = (GenericRowData) output.get(0).getRow(2, 2);
        Assert.assertEquals("Jane", personRow.getString(0).toString());
        Assert.assertEquals("25", personRow.getString(1).toString());

        // verify itemsStruct: GenericArrayData of GenericRowData (extra excluded from each)
        GenericArrayData itemsArray = (GenericArrayData) output.get(0).getArray(3);
        Assert.assertEquals(2, itemsArray.size());
        RowData item0 = itemsArray.getRow(0, 2);
        Assert.assertEquals("1", item0.getString(0).toString());
        Assert.assertEquals("item1", item0.getString(1).toString());
        RowData item1 = itemsArray.getRow(1, 2);
        Assert.assertEquals("2", item1.getString(0).toString());
        Assert.assertEquals("item2", item1.getString(1).toString());
    }

    @Test
    public void testJsonExtractStructExcludingWithNoExcludedFields() throws Exception {
        List<FieldInfo> sinkFields = this.getTestFieldList("personStruct");
        FieldInfo personStruct = sinkFields.get(0);
        // No fields excluded, all fields (name,age,email) should be returned in order
        RowFormatInfo personStructFormat = new RowFormatInfo(
                new String[]{"name", "age", "email"},
                new FormatInfo[]{new StringFormatInfo(), new StringFormatInfo(), new StringFormatInfo()});
        personStruct.setFormatInfo(personStructFormat);

        RowDataSinkInfo rowSink = new RowDataSinkInfo("UTF-8", sinkFields);

        String transformSql =
                "select json_extract_struct_excluding($root.person,non_existent_field) as personStruct from source";

        TransformConfig config = new TransformConfig(transformSql);
        TransformProcessor<String, RowData> processor = TransformProcessor.create(
                config,
                SourceDecoderFactory.createJsonDecoder(new JsonSourceInfo("UTF-8", null)),
                SinkEncoderFactory.createRowEncoder(rowSink));

        String strJson = "{\"person\":{\"name\":\"Jane\",\"age\":25,\"email\":\"jane@test.com\"}}";

        List<RowData> output = processor.transform(strJson, new HashMap<>());
        Assert.assertEquals(1, output.size());

        // All three fields should be present since non_existent_field doesn't match
        GenericRowData personRow = (GenericRowData) output.get(0).getRow(0, 3);
        Assert.assertEquals("Jane", personRow.getString(0).toString());
        Assert.assertEquals("25", personRow.getString(1).toString());
        Assert.assertEquals("jane@test.com", personRow.getString(2).toString());
    }

    @Test
    public void testJsonExtractStructExcludingWithAllFieldsExcluded() throws Exception {
        List<FieldInfo> sinkFields = this.getTestFieldList("personStruct");
        FieldInfo personStruct = sinkFields.get(0);
        // All fields excluded, result is an empty GenericRowData
        RowFormatInfo personStructFormat = new RowFormatInfo(
                new String[]{},
                new FormatInfo[]{});
        personStruct.setFormatInfo(personStructFormat);

        RowDataSinkInfo rowSink = new RowDataSinkInfo("UTF-8", sinkFields);

        String transformSql =
                "select json_extract_struct_excluding($root.person,name,age) as personStruct from source";

        TransformConfig config = new TransformConfig(transformSql);
        TransformProcessor<String, RowData> processor = TransformProcessor.create(
                config,
                SourceDecoderFactory.createJsonDecoder(new JsonSourceInfo("UTF-8", null)),
                SinkEncoderFactory.createRowEncoder(rowSink));

        String strJson = "{\"person\":{\"name\":\"Jane\",\"age\":25}}";

        List<RowData> output = processor.transform(strJson, new HashMap<>());
        Assert.assertEquals(1, output.size());

        // All fields excluded, should return empty GenericRowData with 0 arity
        GenericRowData personRow = (GenericRowData) output.get(0).getRow(0, 0);
        Assert.assertEquals(0, personRow.getArity());
    }

    @Test
    public void testJsonExtractStructExcludingWithArray() throws Exception {
        List<FieldInfo> sinkFields = this.getTestFieldList("itemsStruct");
        FieldInfo itemsStruct = sinkFields.get(0);
        // exclude 'extra' from each array element, remaining: id,value
        RowFormatInfo itemsRowFormat = new RowFormatInfo(
                new String[]{"id", "value"},
                new FormatInfo[]{new StringFormatInfo(), new StringFormatInfo()});
        ArrayFormatInfo itemsStructFormat = new ArrayFormatInfo(itemsRowFormat);
        itemsStruct.setFormatInfo(itemsStructFormat);

        RowDataSinkInfo rowSink = new RowDataSinkInfo("UTF-8", sinkFields);

        String transformSql =
                "select json_extract_struct_excluding($root.items,extra) as itemsStruct from source";

        TransformConfig config = new TransformConfig(transformSql);
        TransformProcessor<String, RowData> processor = TransformProcessor.create(
                config,
                SourceDecoderFactory.createJsonDecoder(new JsonSourceInfo("UTF-8", null)),
                SinkEncoderFactory.createRowEncoder(rowSink));

        String strJson = "{\"items\":[{\"id\":\"1\",\"value\":\"v1\",\"extra\":\"x1\"},"
                + "{\"id\":\"2\",\"value\":\"v2\",\"extra\":\"x2\"},{\"id\":\"3\",\"value\":\"v3\",\"extra\":\"x3\"}]}";

        List<RowData> output = processor.transform(strJson, new HashMap<>());
        Assert.assertEquals(1, output.size());

        GenericArrayData itemsArray = (GenericArrayData) output.get(0).getArray(0);
        Assert.assertEquals(3, itemsArray.size());
        RowData item0 = itemsArray.getRow(0, 2);
        Assert.assertEquals("1", item0.getString(0).toString());
        Assert.assertEquals("v1", item0.getString(1).toString());
        RowData item2 = itemsArray.getRow(2, 2);
        Assert.assertEquals("3", item2.getString(0).toString());
        Assert.assertEquals("v3", item2.getString(1).toString());
    }

    @Test
    public void testJsonExtractStructExcludingWithEmptyArray() throws Exception {
        List<FieldInfo> sinkFields = this.getTestFieldList("itemsStruct");
        FieldInfo itemsStruct = sinkFields.get(0);
        RowFormatInfo itemsRowFormat = new RowFormatInfo(
                new String[]{"id", "value"},
                new FormatInfo[]{new StringFormatInfo(), new StringFormatInfo()});
        ArrayFormatInfo itemsStructFormat = new ArrayFormatInfo(itemsRowFormat);
        itemsStruct.setFormatInfo(itemsStructFormat);

        RowDataSinkInfo rowSink = new RowDataSinkInfo("UTF-8", sinkFields);

        String transformSql =
                "select json_extract_struct_excluding($root.items,extra) as itemsStruct from source";

        TransformConfig config = new TransformConfig(transformSql);
        TransformProcessor<String, RowData> processor = TransformProcessor.create(
                config,
                SourceDecoderFactory.createJsonDecoder(new JsonSourceInfo("UTF-8", null)),
                SinkEncoderFactory.createRowEncoder(rowSink));

        String strJson = "{\"items\":[]}";

        List<RowData> output = processor.transform(strJson, new HashMap<>());
        Assert.assertEquals(1, output.size());

        // Empty array should return empty GenericArrayData
        GenericArrayData itemsArray = (GenericArrayData) output.get(0).getArray(0);
        Assert.assertEquals(0, itemsArray.size());
    }

    @Test
    public void testJsonExtractStructExcludingWithNonExistentPath() throws Exception {
        List<FieldInfo> sinkFields = this.getTestFieldList("personStruct");
        FieldInfo personStruct = sinkFields.get(0);
        RowFormatInfo personStructFormat = new RowFormatInfo(
                new String[]{"name", "age"},
                new FormatInfo[]{new StringFormatInfo(), new StringFormatInfo()});
        personStruct.setFormatInfo(personStructFormat);

        RowDataSinkInfo rowSink = new RowDataSinkInfo("UTF-8", sinkFields);

        String transformSql =
                "select json_extract_struct_excluding($root.non_existent_path,address) as personStruct from source";

        TransformConfig config = new TransformConfig(transformSql);
        TransformProcessor<String, RowData> processor = TransformProcessor.create(
                config,
                SourceDecoderFactory.createJsonDecoder(new JsonSourceInfo("UTF-8", null)),
                SinkEncoderFactory.createRowEncoder(rowSink));

        String strJson = "{\"person\":{\"name\":\"Jane\",\"age\":25}}";

        List<RowData> output = processor.transform(strJson, new HashMap<>());
        Assert.assertEquals(1, output.size());
        // Non-existent path should return null struct
        Assert.assertTrue(output.get(0).isNullAt(0));
    }

    @Test
    public void testJsonExtractStructExcludingWithObjectPreservesFieldOrder() throws Exception {
        List<FieldInfo> sinkFields = this.getTestFieldList("personStruct");
        FieldInfo personStruct = sinkFields.get(0);
        // Original JSON field order: a,d,c,b -> exclude 'd' -> remaining: a,c,b (in original order)
        RowFormatInfo personStructFormat = new RowFormatInfo(
                new String[]{"a", "c", "b"},
                new FormatInfo[]{new StringFormatInfo(), new StringFormatInfo(), new StringFormatInfo()});
        personStruct.setFormatInfo(personStructFormat);

        RowDataSinkInfo rowSink = new RowDataSinkInfo("UTF-8", sinkFields);

        String transformSql =
                "select json_extract_struct_excluding($root.person,d) as personStruct from source";

        TransformConfig config = new TransformConfig(transformSql);
        TransformProcessor<String, RowData> processor = TransformProcessor.create(
                config,
                SourceDecoderFactory.createJsonDecoder(new JsonSourceInfo("UTF-8", null)),
                SinkEncoderFactory.createRowEncoder(rowSink));

        // JSON with fields in order: a,d,c,b
        String strJson = "{\"person\":{\"a\":\"v_a\",\"d\":\"v_d\",\"c\":\"v_c\",\"b\":\"v_b\"}}";

        List<RowData> output = processor.transform(strJson, new HashMap<>());
        Assert.assertEquals(1, output.size());

        // After excluding 'd', fields should remain in order: a, c, b
        GenericRowData personRow = (GenericRowData) output.get(0).getRow(0, 3);
        Assert.assertEquals("v_a", personRow.getString(0).toString());
        Assert.assertEquals("v_c", personRow.getString(1).toString());
        Assert.assertEquals("v_b", personRow.getString(2).toString());
    }

    // ========== JsonToArrayFunction tests ==========

    @Test
    public void testJsonToArrayWithObjectArray() throws Exception {
        List<FieldInfo> sinkFields = this.getTestFieldList("itemsArray");
        FieldInfo itemsArrayField = sinkFields.get(0);
        // Each element has id,name,active in order
        RowFormatInfo itemsRowFormat = new RowFormatInfo(
                new String[]{"id", "name", "active"},
                new FormatInfo[]{new StringFormatInfo(), new StringFormatInfo(), new StringFormatInfo()});
        ArrayFormatInfo itemsArrayFormat = new ArrayFormatInfo(itemsRowFormat);
        itemsArrayField.setFormatInfo(itemsArrayFormat);

        RowDataSinkInfo rowSink = new RowDataSinkInfo("UTF-8", sinkFields);

        String transformSql =
                "select json_to_array($root.items) as itemsArray from source";

        TransformConfig config = new TransformConfig(transformSql);
        TransformProcessor<String, RowData> processor = TransformProcessor.create(
                config,
                SourceDecoderFactory.createJsonDecoder(new JsonSourceInfo("UTF-8", null)),
                SinkEncoderFactory.createRowEncoder(rowSink));

        String strJson = "{\"items\":[{\"id\":\"1\",\"name\":\"item1\",\"active\":true},"
                + "{\"id\":\"2\",\"name\":\"item2\",\"active\":false}]}";

        List<RowData> output = processor.transform(strJson, new HashMap<>());
        Assert.assertEquals(1, output.size());

        GenericArrayData itemsArray = (GenericArrayData) output.get(0).getArray(0);
        Assert.assertEquals(2, itemsArray.size());

        RowData item0 = itemsArray.getRow(0, 3);
        Assert.assertEquals("1", item0.getString(0).toString());
        Assert.assertEquals("item1", item0.getString(1).toString());
        Assert.assertEquals("true", item0.getString(2).toString());

        RowData item1 = itemsArray.getRow(1, 3);
        Assert.assertEquals("2", item1.getString(0).toString());
        Assert.assertEquals("item2", item1.getString(1).toString());
        Assert.assertEquals("false", item1.getString(2).toString());
    }

    @Test
    public void testJsonToArrayWithPrimitiveArray() throws Exception {
        List<FieldInfo> sinkFields = this.getTestFieldList("strArray");
        FieldInfo strArrayField = sinkFields.get(0);
        ArrayFormatInfo strArrayFormat = new ArrayFormatInfo(new StringFormatInfo());
        strArrayField.setFormatInfo(strArrayFormat);

        RowDataSinkInfo rowSink = new RowDataSinkInfo("UTF-8", sinkFields);

        String transformSql =
                "select json_to_array($root.tags) as strArray from source";

        TransformConfig config = new TransformConfig(transformSql);
        TransformProcessor<String, RowData> processor = TransformProcessor.create(
                config,
                SourceDecoderFactory.createJsonDecoder(new JsonSourceInfo("UTF-8", null)),
                SinkEncoderFactory.createRowEncoder(rowSink));

        String strJson = "{\"tags\":[\"a\",\"b\",\"c\"]}";

        List<RowData> output = processor.transform(strJson, new HashMap<>());
        Assert.assertEquals(1, output.size());

        GenericArrayData tagsArray = (GenericArrayData) output.get(0).getArray(0);
        Assert.assertEquals(3, tagsArray.size());
        Assert.assertEquals("a", tagsArray.getString(0).toString());
        Assert.assertEquals("b", tagsArray.getString(1).toString());
        Assert.assertEquals("c", tagsArray.getString(2).toString());
    }

    @Test
    public void testJsonToArrayWithNumberArray() throws Exception {
        List<FieldInfo> sinkFields = this.getTestFieldList("numArray");
        FieldInfo numArrayField = sinkFields.get(0);
        ArrayFormatInfo numArrayFormat = new ArrayFormatInfo(new StringFormatInfo());
        numArrayField.setFormatInfo(numArrayFormat);

        RowDataSinkInfo rowSink = new RowDataSinkInfo("UTF-8", sinkFields);

        String transformSql =
                "select json_to_array($root.values) as numArray from source";

        TransformConfig config = new TransformConfig(transformSql);
        TransformProcessor<String, RowData> processor = TransformProcessor.create(
                config,
                SourceDecoderFactory.createJsonDecoder(new JsonSourceInfo("UTF-8", null)),
                SinkEncoderFactory.createRowEncoder(rowSink));

        String strJson = "{\"values\":[1,2,3]}";

        List<RowData> output = processor.transform(strJson, new HashMap<>());
        Assert.assertEquals(1, output.size());

        GenericArrayData numArray = (GenericArrayData) output.get(0).getArray(0);
        Assert.assertEquals(3, numArray.size());
    }

    @Test
    public void testJsonToArrayWithNonArrayPath() throws Exception {
        List<FieldInfo> sinkFields = this.getTestFieldList("resultArray");
        FieldInfo resultArrayField = sinkFields.get(0);
        ArrayFormatInfo resultArrayFormat = new ArrayFormatInfo(new StringFormatInfo());
        resultArrayField.setFormatInfo(resultArrayFormat);

        RowDataSinkInfo rowSink = new RowDataSinkInfo("UTF-8", sinkFields);

        // path resolves to a JSON object, not an array → should return null
        String transformSql =
                "select json_to_array($root.person) as resultArray from source";

        TransformConfig config = new TransformConfig(transformSql);
        TransformProcessor<String, RowData> processor = TransformProcessor.create(
                config,
                SourceDecoderFactory.createJsonDecoder(new JsonSourceInfo("UTF-8", null)),
                SinkEncoderFactory.createRowEncoder(rowSink));

        String strJson = "{\"person\":{\"name\":\"Jane\",\"age\":25}}";

        List<RowData> output = processor.transform(strJson, new HashMap<>());
        Assert.assertEquals(1, output.size());
        // Person is an object, not an array → null
        Assert.assertTrue(output.get(0).isNullAt(0));
    }

    @Test
    public void testJsonToArrayWithNonExistentPath() throws Exception {
        List<FieldInfo> sinkFields = this.getTestFieldList("resultArray");
        FieldInfo resultArrayField = sinkFields.get(0);
        ArrayFormatInfo resultArrayFormat = new ArrayFormatInfo(new StringFormatInfo());
        resultArrayField.setFormatInfo(resultArrayFormat);

        RowDataSinkInfo rowSink = new RowDataSinkInfo("UTF-8", sinkFields);

        String transformSql =
                "select json_to_array($root.non_existent) as resultArray from source";

        TransformConfig config = new TransformConfig(transformSql);
        TransformProcessor<String, RowData> processor = TransformProcessor.create(
                config,
                SourceDecoderFactory.createJsonDecoder(new JsonSourceInfo("UTF-8", null)),
                SinkEncoderFactory.createRowEncoder(rowSink));

        String strJson = "{\"items\":[1,2,3]}";

        List<RowData> output = processor.transform(strJson, new HashMap<>());
        Assert.assertEquals(1, output.size());
        // Non-existent path → null
        Assert.assertTrue(output.get(0).isNullAt(0));
    }

    @Test
    public void testJsonToArrayWithEmptyArray() throws Exception {
        List<FieldInfo> sinkFields = this.getTestFieldList("resultArray");
        FieldInfo resultArrayField = sinkFields.get(0);
        ArrayFormatInfo resultArrayFormat = new ArrayFormatInfo(new StringFormatInfo());
        resultArrayField.setFormatInfo(resultArrayFormat);

        RowDataSinkInfo rowSink = new RowDataSinkInfo("UTF-8", sinkFields);

        String transformSql =
                "select json_to_array($root.items) as resultArray from source";

        TransformConfig config = new TransformConfig(transformSql);
        TransformProcessor<String, RowData> processor = TransformProcessor.create(
                config,
                SourceDecoderFactory.createJsonDecoder(new JsonSourceInfo("UTF-8", null)),
                SinkEncoderFactory.createRowEncoder(rowSink));

        String strJson = "{\"items\":[]}";

        List<RowData> output = processor.transform(strJson, new HashMap<>());
        Assert.assertEquals(1, output.size());

        GenericArrayData resultArray = (GenericArrayData) output.get(0).getArray(0);
        Assert.assertEquals(0, resultArray.size());
    }

    @Test
    public void testJsonToArrayWithNestedArray() throws Exception {
        List<FieldInfo> sinkFields = this.getTestFieldList("nestedArray");
        FieldInfo nestedArrayField = sinkFields.get(0);
        RowFormatInfo innerRowFormat = new RowFormatInfo(
                new String[]{"id", "value"},
                new FormatInfo[]{new StringFormatInfo(), new StringFormatInfo()});
        ArrayFormatInfo innerArrayFormat = new ArrayFormatInfo(innerRowFormat);
        ArrayFormatInfo nestedArrayFormat = new ArrayFormatInfo(innerArrayFormat);
        nestedArrayField.setFormatInfo(nestedArrayFormat);

        RowDataSinkInfo rowSink = new RowDataSinkInfo("UTF-8", sinkFields);

        String transformSql =
                "select json_to_array($root.matrix) as nestedArray from source";

        TransformConfig config = new TransformConfig(transformSql);
        TransformProcessor<String, RowData> processor = TransformProcessor.create(
                config,
                SourceDecoderFactory.createJsonDecoder(new JsonSourceInfo("UTF-8", null)),
                SinkEncoderFactory.createRowEncoder(rowSink));

        String strJson = "{\"matrix\":[[{\"id\":\"1\",\"value\":\"a\"}],[{\"id\":\"2\",\"value\":\"b\"}]]}";

        List<RowData> output = processor.transform(strJson, new HashMap<>());
        Assert.assertEquals(1, output.size());

        GenericArrayData outerArray = (GenericArrayData) output.get(0).getArray(0);
        Assert.assertEquals(2, outerArray.size());
        GenericArrayData innerArray0 = (GenericArrayData) outerArray.getArray(0);
        Assert.assertEquals(1, innerArray0.size());
        RowData innerItem0 = innerArray0.getRow(0, 2);
        Assert.assertEquals("1", innerItem0.getString(0).toString());
        Assert.assertEquals("a", innerItem0.getString(1).toString());
    }

    // ========== JsonToStructFunction tests ==========

    @Test
    public void testJsonToStructWithObject() throws Exception {
        List<FieldInfo> sinkFields = this.getTestFieldList("personStruct");
        FieldInfo personStruct = sinkFields.get(0);
        RowFormatInfo personStructFormat = new RowFormatInfo(
                new String[]{"name", "age", "email"},
                new FormatInfo[]{new StringFormatInfo(), new StringFormatInfo(), new StringFormatInfo()});
        personStruct.setFormatInfo(personStructFormat);

        RowDataSinkInfo rowSink = new RowDataSinkInfo("UTF-8", sinkFields);

        String transformSql =
                "select json_to_struct($root.person) as personStruct from source";

        TransformConfig config = new TransformConfig(transformSql);
        TransformProcessor<String, RowData> processor = TransformProcessor.create(
                config,
                SourceDecoderFactory.createJsonDecoder(new JsonSourceInfo("UTF-8", null)),
                SinkEncoderFactory.createRowEncoder(rowSink));

        String strJson = "{\"person\":{\"name\":\"Jane\",\"age\":25,\"email\":\"jane@test.com\"}}";

        List<RowData> output = processor.transform(strJson, new HashMap<>());
        Assert.assertEquals(1, output.size());

        GenericRowData personRow = (GenericRowData) output.get(0).getRow(0, 3);
        Assert.assertEquals("Jane", personRow.getString(0).toString());
        Assert.assertEquals("25", personRow.getString(1).toString());
        Assert.assertEquals("jane@test.com", personRow.getString(2).toString());
    }

    @Test
    public void testJsonToStructWithNonObjectPath() throws Exception {
        List<FieldInfo> sinkFields = this.getTestFieldList("resultStruct");
        FieldInfo resultStruct = sinkFields.get(0);
        RowFormatInfo resultStructFormat = new RowFormatInfo(
                new String[]{"name", "age"},
                new FormatInfo[]{new StringFormatInfo(), new StringFormatInfo()});
        resultStruct.setFormatInfo(resultStructFormat);

        RowDataSinkInfo rowSink = new RowDataSinkInfo("UTF-8", sinkFields);

        // path resolves to a JSON array, not an object -> should return null
        String transformSql =
                "select json_to_struct($root.items) as resultStruct from source";

        TransformConfig config = new TransformConfig(transformSql);
        TransformProcessor<String, RowData> processor = TransformProcessor.create(
                config,
                SourceDecoderFactory.createJsonDecoder(new JsonSourceInfo("UTF-8", null)),
                SinkEncoderFactory.createRowEncoder(rowSink));

        String strJson = "{\"items\":[{\"name\":\"a\",\"age\":1}]}";

        List<RowData> output = processor.transform(strJson, new HashMap<>());
        Assert.assertEquals(1, output.size());
        // Items is an array, not an object -> null
        Assert.assertTrue(output.get(0).isNullAt(0));
    }

    @Test
    public void testJsonToStructWithNonExistentPath() throws Exception {
        List<FieldInfo> sinkFields = this.getTestFieldList("resultStruct");
        FieldInfo resultStruct = sinkFields.get(0);
        RowFormatInfo resultStructFormat = new RowFormatInfo(
                new String[]{"name", "age"},
                new FormatInfo[]{new StringFormatInfo(), new StringFormatInfo()});
        resultStruct.setFormatInfo(resultStructFormat);

        RowDataSinkInfo rowSink = new RowDataSinkInfo("UTF-8", sinkFields);

        String transformSql =
                "select json_to_struct($root.non_existent) as resultStruct from source";

        TransformConfig config = new TransformConfig(transformSql);
        TransformProcessor<String, RowData> processor = TransformProcessor.create(
                config,
                SourceDecoderFactory.createJsonDecoder(new JsonSourceInfo("UTF-8", null)),
                SinkEncoderFactory.createRowEncoder(rowSink));

        String strJson = "{\"person\":{\"name\":\"Jane\",\"age\":25}}";

        List<RowData> output = processor.transform(strJson, new HashMap<>());
        Assert.assertEquals(1, output.size());
        // Non-existent path -> null
        Assert.assertTrue(output.get(0).isNullAt(0));
    }

    @Test
    public void testJsonToStructWithNestedObject() throws Exception {
        List<FieldInfo> sinkFields = this.getTestFieldList("personStruct");
        // person has name, age, address (nested object with city, zip)
        FieldInfo personStruct = sinkFields.get(0);
        RowFormatInfo addressRowFormat = new RowFormatInfo(
                new String[]{"city", "zip"},
                new FormatInfo[]{new StringFormatInfo(), new StringFormatInfo()});
        RowFormatInfo personStructFormat = new RowFormatInfo(
                new String[]{"name", "age", "address"},
                new FormatInfo[]{new StringFormatInfo(), new StringFormatInfo(), addressRowFormat});
        personStruct.setFormatInfo(personStructFormat);

        RowDataSinkInfo rowSink = new RowDataSinkInfo("UTF-8", sinkFields);

        String transformSql =
                "select json_to_struct($root.person) as personStruct from source";

        TransformConfig config = new TransformConfig(transformSql);
        TransformProcessor<String, RowData> processor = TransformProcessor.create(
                config,
                SourceDecoderFactory.createJsonDecoder(new JsonSourceInfo("UTF-8", null)),
                SinkEncoderFactory.createRowEncoder(rowSink));

        String strJson = "{\"person\":{\"name\":\"Jane\",\"age\":25,"
                + "\"address\":{\"city\":\"NYC\",\"zip\":\"10001\"}}}";

        List<RowData> output = processor.transform(strJson, new HashMap<>());
        Assert.assertEquals(1, output.size());

        GenericRowData personRow = (GenericRowData) output.get(0).getRow(0, 3);
        Assert.assertEquals("Jane", personRow.getString(0).toString());
        Assert.assertEquals("25", personRow.getString(1).toString());

        // address is a nested GenericRowData
        GenericRowData addressRow = (GenericRowData) personRow.getRow(2, 2);
        Assert.assertEquals("NYC", addressRow.getString(0).toString());
        Assert.assertEquals("10001", addressRow.getString(1).toString());
    }

    @Test
    public void testJsonToStructWithEmptyObject() throws Exception {
        List<FieldInfo> sinkFields = this.getTestFieldList("resultStruct");
        FieldInfo resultStruct = sinkFields.get(0);
        RowFormatInfo resultStructFormat = new RowFormatInfo(
                new String[]{},
                new FormatInfo[]{});
        resultStruct.setFormatInfo(resultStructFormat);

        RowDataSinkInfo rowSink = new RowDataSinkInfo("UTF-8", sinkFields);

        String transformSql =
                "select json_to_struct($root.empty) as resultStruct from source";

        TransformConfig config = new TransformConfig(transformSql);
        TransformProcessor<String, RowData> processor = TransformProcessor.create(
                config,
                SourceDecoderFactory.createJsonDecoder(new JsonSourceInfo("UTF-8", null)),
                SinkEncoderFactory.createRowEncoder(rowSink));

        String strJson = "{\"empty\":{}}";

        List<RowData> output = processor.transform(strJson, new HashMap<>());
        Assert.assertEquals(1, output.size());

        GenericRowData resultRow = (GenericRowData) output.get(0).getRow(0, 0);
        Assert.assertEquals(0, resultRow.getArity());
    }

    @Test
    public void testJsonToStructWithArrayField() throws Exception {
        List<FieldInfo> sinkFields = this.getTestFieldList("personStruct");
        FieldInfo personStruct = sinkFields.get(0);
        ArrayFormatInfo tagsFormat = new ArrayFormatInfo(new StringFormatInfo());
        RowFormatInfo personStructFormat = new RowFormatInfo(
                new String[]{"name", "tags"},
                new FormatInfo[]{new StringFormatInfo(), tagsFormat});
        personStruct.setFormatInfo(personStructFormat);

        RowDataSinkInfo rowSink = new RowDataSinkInfo("UTF-8", sinkFields);

        String transformSql =
                "select json_to_struct($root.person) as personStruct from source";

        TransformConfig config = new TransformConfig(transformSql);
        TransformProcessor<String, RowData> processor = TransformProcessor.create(
                config,
                SourceDecoderFactory.createJsonDecoder(new JsonSourceInfo("UTF-8", null)),
                SinkEncoderFactory.createRowEncoder(rowSink));

        String strJson = "{\"person\":{\"name\":\"Jane\",\"tags\":[\"a\",\"b\",\"c\"]}}";

        List<RowData> output = processor.transform(strJson, new HashMap<>());
        Assert.assertEquals(1, output.size());

        GenericRowData personRow = (GenericRowData) output.get(0).getRow(0, 2);
        Assert.assertEquals("Jane", personRow.getString(0).toString());

        GenericArrayData tagsArray = (GenericArrayData) personRow.getArray(1);
        Assert.assertEquals(3, tagsArray.size());
        Assert.assertEquals("a", tagsArray.getString(0).toString());
        Assert.assertEquals("b", tagsArray.getString(1).toString());
        Assert.assertEquals("c", tagsArray.getString(2).toString());
    }

    @Test
    public void testJsonToStructPreservesFieldOrder() throws Exception {
        List<FieldInfo> sinkFields = this.getTestFieldList("personStruct");
        FieldInfo personStruct = sinkFields.get(0);
        RowFormatInfo personStructFormat = new RowFormatInfo(
                new String[]{"a", "d", "c", "b"},
                new FormatInfo[]{new StringFormatInfo(), new StringFormatInfo(),
                        new StringFormatInfo(), new StringFormatInfo()});
        personStruct.setFormatInfo(personStructFormat);

        RowDataSinkInfo rowSink = new RowDataSinkInfo("UTF-8", sinkFields);

        String transformSql =
                "select json_to_struct($root.person) as personStruct from source";

        TransformConfig config = new TransformConfig(transformSql);
        TransformProcessor<String, RowData> processor = TransformProcessor.create(
                config,
                SourceDecoderFactory.createJsonDecoder(new JsonSourceInfo("UTF-8", null)),
                SinkEncoderFactory.createRowEncoder(rowSink));

        // JSON with fields in order: a,d,c,b
        String strJson = "{\"person\":{\"a\":\"v_a\",\"d\":\"v_d\",\"c\":\"v_c\",\"b\":\"v_b\"}}";

        List<RowData> output = processor.transform(strJson, new HashMap<>());
        Assert.assertEquals(1, output.size());

        // Fields should remain in original JSON order: a, d, c, b
        GenericRowData personRow = (GenericRowData) output.get(0).getRow(0, 4);
        Assert.assertEquals("v_a", personRow.getString(0).toString());
        Assert.assertEquals("v_d", personRow.getString(1).toString());
        Assert.assertEquals("v_c", personRow.getString(2).toString());
        Assert.assertEquals("v_b", personRow.getString(3).toString());
    }
}
