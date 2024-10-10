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

import org.apache.inlong.sdk.transform.decode.SourceDecoderFactory;
import org.apache.inlong.sdk.transform.encode.ParquetSinkEncoder;
import org.apache.inlong.sdk.transform.encode.SinkEncoderFactory;
import org.apache.inlong.sdk.transform.pojo.FieldInfo;
import org.apache.inlong.sdk.transform.pojo.JsonSourceInfo;
import org.apache.inlong.sdk.transform.pojo.ParquetSinkInfo;
import org.apache.inlong.sdk.transform.pojo.TransformConfig;
import org.apache.inlong.sdk.transform.process.TransformProcessor;

import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.util.HashMap;
import java.util.List;

public class TestJson2ParquetProcessor extends AbstractProcessorTestBase {

    @Test
    public void testJson2Parquet() throws Exception {
        List<FieldInfo> fields;
        JsonSourceInfo jsonSource;
        ParquetSinkInfo parquetSinkInfo;
        ParquetSinkEncoder parquetEncoder;
        String transformSql;
        TransformConfig config;
        TransformProcessor<String, ByteArrayOutputStream> processor;
        String srcString;
        List<ByteArrayOutputStream> output;
        List<String> result;
        byte[] bytes;

        fields = this.getTestFieldList("sid", "packageID", "msgTime", "msg");
        jsonSource = new JsonSourceInfo("UTF-8", "msgs");
        parquetSinkInfo = new ParquetSinkInfo("UTF-8", fields);
        parquetEncoder = SinkEncoderFactory.createParquetEncoder(parquetSinkInfo);
        transformSql = "select $root.sid,$root.packageID,$child.msgTime,$child.msg from source";
        config = new TransformConfig(transformSql);
        // case1
        processor = TransformProcessor
                .create(config, SourceDecoderFactory.createJsonDecoder(jsonSource),
                        parquetEncoder);
        srcString = "{\n"
                + "  \"sid\":\"value1\",\n"
                + "  \"packageID\":\"value2\",\n"
                + "  \"msgs\":[\n"
                + "  {\"msg\":\"value4\",\"msgTime\":1713243918000},\n"
                + "  {\"msg\":\"v4\",\"msgTime\":1713243918000}\n"
                + "  ]\n"
                + "}";
        output = processor.transform(srcString, new HashMap<>());
        bytes = parquetEncoder.mergeByteArray(output);
        result = ParquetByteArray2CsvStr(bytes);
        Assert.assertEquals(2, result.size());
        Assert.assertEquals("value1|value2|1713243918000|value4", result.get(0));
        Assert.assertEquals("value1|value2|1713243918000|v4", result.get(1));

        fields = this.getTestFieldList("id", "itemId", "subItemId", "msg");
        jsonSource = new JsonSourceInfo("UTF-8", "items");
        parquetSinkInfo = new ParquetSinkInfo("UTF-8", fields);
        parquetEncoder = SinkEncoderFactory.createParquetEncoder(parquetSinkInfo);
        transformSql = "select $root.id,$child.itemId,$child.subItems(0).subItemId,$child.subItems(1).msg from source";
        config = new TransformConfig(transformSql);
        // case2
        processor = TransformProcessor
                .create(config, SourceDecoderFactory.createJsonDecoder(jsonSource),
                        parquetEncoder);
        srcString = "{\n"
                + "  \"id\":\"value1\",\n"
                + "  \"name\":\"value2\",\n"
                + "  \"items\":[\n"
                + "    {\"itemId\":\"item1\",\n"
                + "     \"subItems\":[\n"
                + "       {\"subItemId\":\"1001\", \"msg\":\"1001msg\"},\n"
                + "       {\"subItemId\":\"1002\", \"msg\":\"1002msg\"}\n"
                + "     ]\n"
                + "    },\n"
                + "    {\"itemId\":\"item2\",\n"
                + "     \"subItems\":[\n"
                + "       {\"subItemId\":\"2001\", \"msg\":\"2001msg\"},\n"
                + "       {\"subItemId\":\"2002\", \"msg\":\"2002msg\"}\n"
                + "     ]\n"
                + "    }\n"
                + "  ]\n"
                + "}";
        output = processor.transform(srcString, new HashMap<>());
        bytes = parquetEncoder.mergeByteArray(output);
        result = ParquetByteArray2CsvStr(bytes);
        Assert.assertEquals(2, result.size());
        Assert.assertEquals("value1|item1|1001|1002msg", result.get(0));
        Assert.assertEquals("value1|item2|2001|2002msg", result.get(1));

        fields = this.getTestFieldList("matrix(0,0)", "matrix(1,1)", "matrix(2,2)");
        jsonSource = new JsonSourceInfo("UTF-8", "");
        parquetSinkInfo = new ParquetSinkInfo("UTF-8", fields);
        parquetEncoder = SinkEncoderFactory.createParquetEncoder(parquetSinkInfo);
        transformSql = "select $root.matrix(0, 0), $root.matrix(1, 1), $root.matrix(2, 2) from source";
        config = new TransformConfig(transformSql);
        // case3
        processor = TransformProcessor
                .create(config, SourceDecoderFactory.createJsonDecoder(jsonSource),
                        parquetEncoder);
        srcString = "{\n"
                + "  \"matrix\": [\n"
                + "    [1, 2, 3],\n"
                + "    [4, 5, 6],\n"
                + "    [7, 8, 9]\n"
                + "  ]\n"
                + "}";
        output = processor.transform(srcString, new HashMap<>());
        bytes = parquetEncoder.mergeByteArray(output);
        result = ParquetByteArray2CsvStr(bytes);
        Assert.assertEquals(1, result.size());
        Assert.assertEquals("1|5|9", result.get(0));

        fields = this.getTestFieldList("department_name", "course_id", "num");
        jsonSource = new JsonSourceInfo("UTF-8", "");
        parquetSinkInfo = new ParquetSinkInfo("UTF-8", fields);
        parquetEncoder = SinkEncoderFactory.createParquetEncoder(parquetSinkInfo);
        transformSql =
                "select $root.departments(0).name, $root.departments(0).courses(0,1).courseId, sqrt($root.departments(0).courses(0,1).courseId - 2) from source";
        config = new TransformConfig(transformSql);
        // case4
        processor = TransformProcessor
                .create(config, SourceDecoderFactory.createJsonDecoder(jsonSource),
                        parquetEncoder);
        srcString = "{\n" +
                "  \"departments\": [\n" +
                "    {\n" +
                "      \"name\": \"Mathematics\",\n" +
                "      \"courses\": [\n" +
                "        [\n" +
                "          {\"courseId\": \"101\", \"title\": \"Calculus I\"},\n" +
                "          {\"courseId\": \"102\", \"title\": \"Linear Algebra\"}\n" +
                "        ],\n" +
                "        [\n" +
                "          {\"courseId\": \"201\", \"title\": \"Calculus II\"},\n" +
                "          {\"courseId\": \"202\", \"title\": \"Abstract Algebra\"}\n" +
                "        ]\n" +
                "      ]\n" +
                "    }\n" +
                "  ]\n" +
                "}";
        output = processor.transform(srcString, new HashMap<>());
        bytes = parquetEncoder.mergeByteArray(output);
        result = ParquetByteArray2CsvStr(bytes);
        Assert.assertEquals(1, result.size());
        Assert.assertEquals("Mathematics|102|10.0", result.get(0));
    }

    @Test
    public void testJson2ParquetForOne() throws Exception {
        List<FieldInfo> fields;
        JsonSourceInfo jsonSource;
        ParquetSinkInfo parquetSinkInfo;
        ParquetSinkEncoder parquetEncoder;
        String transformSql;
        TransformConfig config;
        TransformProcessor<String, ByteArrayOutputStream> processor;
        String srcString;
        List<ByteArrayOutputStream> output;
        List<String> result;
        byte[] bytes;

        fields = this.getTestFieldList("sid", "packageID", "msgTime", "msg");
        jsonSource = new JsonSourceInfo("UTF-8", "");
        parquetSinkInfo = new ParquetSinkInfo("UTF-8", fields);
        parquetEncoder = SinkEncoderFactory.createParquetEncoder(parquetSinkInfo);
        transformSql = "select $root.sid,$root.packageID,$root.msgs(1).msgTime,$root.msgs(0).msg from source";
        config = new TransformConfig(transformSql);
        // case1
        processor = TransformProcessor
                .create(config, SourceDecoderFactory.createJsonDecoder(jsonSource),
                        parquetEncoder);
        srcString = "{\n"
                + "  \"sid\":\"value1\",\n"
                + "  \"packageID\":\"value2\",\n"
                + "  \"msgs\":[\n"
                + "  {\"msg\":\"value4\",\"msgTime\":1713243918000},\n"
                + "  {\"msg\":\"v4\",\"msgTime\":1713243918000}\n"
                + "  ]\n"
                + "}";
        output = processor.transform(srcString, new HashMap<>());
        bytes = parquetEncoder.mergeByteArray(output);
        result = ParquetByteArray2CsvStr(bytes);
        Assert.assertEquals(1, result.size());
        Assert.assertEquals("value1|value2|1713243918000|value4", result.get(0));
    }
}
