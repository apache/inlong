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
import org.apache.inlong.sdk.transform.encode.SinkEncoderFactory;
import org.apache.inlong.sdk.transform.pojo.CsvSinkInfo;
import org.apache.inlong.sdk.transform.pojo.FieldInfo;
import org.apache.inlong.sdk.transform.pojo.JsonSourceInfo;
import org.apache.inlong.sdk.transform.pojo.TransformConfig;
import org.apache.inlong.sdk.transform.process.TransformProcessor;

import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TestJson2CsvProcessor extends AbstractProcessorTestBase {

    @Test
    public void testJson2Csv() throws Exception {
        List<FieldInfo> fields1 = this.getTestFieldList("sid", "packageID", "msgTime", "msg", "ds");
        JsonSourceInfo jsonSource1 = new JsonSourceInfo("UTF-8", "msgs");
        CsvSinkInfo csvSink1 = new CsvSinkInfo("UTF-8", '|', '\\', fields1);
        String transformSql1 = "select $root.sid,$root.packageID,$child.msgTime,$child.msg,$ctx.partition from source";
        TransformConfig config1 = new TransformConfig(transformSql1);
        // case1
        TransformProcessor<String, String> processor1 = TransformProcessor
                .create(config1, SourceDecoderFactory.createJsonDecoder(jsonSource1),
                        SinkEncoderFactory.createCsvEncoder(csvSink1));
        String srcString1 = "{\n"
                + "  \"sid\":\"value1\",\n"
                + "  \"packageID\":\"value2\",\n"
                + "  \"msgs\":[\n"
                + "  {\"msg\":\"value4\",\"msgTime\":1713243918000},\n"
                + "  {\"msg\":\"v4\",\"msgTime\":1713243918000}\n"
                + "  ]\n"
                + "}";
        Map<String, Object> extParams = new HashMap<>();
        extParams.put("partition", "2024042801");
        List<String> output1 = processor1.transform(srcString1, extParams);
        Assert.assertEquals(2, output1.size());
        Assert.assertEquals(output1.get(0), "value1|value2|1713243918000|value4|2024042801");
        Assert.assertEquals(output1.get(1), "value1|value2|1713243918000|v4|2024042801");
        // case2
        List<FieldInfo> fields2 = this.getTestFieldList("id", "itemId", "subItemId", "msg");
        JsonSourceInfo jsonSource2 = new JsonSourceInfo("UTF-8", "items");
        CsvSinkInfo csvSink2 = new CsvSinkInfo("UTF-8", '|', '\\', fields2);
        String transformSql2 =
                "select $root.id,$child.itemId,$child.subItems(0).subItemId,$child.subItems(1).msg from source";
        TransformConfig config2 = new TransformConfig(transformSql2);
        TransformProcessor<String, String> processor2 = TransformProcessor
                .create(config2, SourceDecoderFactory.createJsonDecoder(jsonSource2),
                        SinkEncoderFactory.createCsvEncoder(csvSink2));
        String srcString2 = "{\n"
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
        List<String> output2 = processor2.transform(srcString2, new HashMap<>());
        Assert.assertEquals(2, output2.size());
        Assert.assertEquals(output2.get(0), "value1|item1|1001|1002msg");
        Assert.assertEquals(output2.get(1), "value1|item2|2001|2002msg");
        // case 3
        List<FieldInfo> fields3 = this.getTestFieldList("matrix(0,0)", "matrix(1,1)", "matrix(2,2)");
        JsonSourceInfo jsonSource3 = new JsonSourceInfo("UTF-8", "");
        CsvSinkInfo csvSink3 = new CsvSinkInfo("UTF-8", '|', '\\', fields3);
        String transformSql3 = "select $root.matrix(0, 0), $root.matrix(1, 1), $root.matrix(2, 2) from source";
        TransformConfig config3 = new TransformConfig(transformSql3);
        TransformProcessor<String, String> processor3 = TransformProcessor
                .create(config3, SourceDecoderFactory.createJsonDecoder(jsonSource3),
                        SinkEncoderFactory.createCsvEncoder(csvSink3));
        String srcString3 = "{\n"
                + "  \"matrix\": [\n"
                + "    [1, 2, 3],\n"
                + "    [4, 5, 6],\n"
                + "    [7, 8, 9]\n"
                + "  ]\n"
                + "}";
        List<String> output3 = processor3.transform(srcString3, new HashMap<>());
        Assert.assertEquals(1, output3.size());
        Assert.assertEquals(output3.get(0), "1|5|9");
        // case 4
        List<FieldInfo> fields4 = this.getTestFieldList("department_name", "course_id", "num");
        JsonSourceInfo jsonSource4 = new JsonSourceInfo("UTF-8", "");
        CsvSinkInfo csvSink4 = new CsvSinkInfo("UTF-8", '|', '\\', fields4);
        String transformSql4 =
                "select $root.departments(0).name, $root.departments(0).courses(0,1).courseId, sqrt($root.departments(0).courses(0,1).courseId - 2) from source";
        TransformConfig config4 = new TransformConfig(transformSql4);
        TransformProcessor<String, String> processor4 = TransformProcessor
                .create(config4, SourceDecoderFactory.createJsonDecoder(jsonSource4),
                        SinkEncoderFactory.createCsvEncoder(csvSink4));
        String srcString4 = "{\n" +
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
        List<String> output4 = processor4.transform(srcString4, new HashMap<>());
        Assert.assertEquals(1, output4.size());
        Assert.assertEquals(output4.get(0), "Mathematics|102|10.0");
    }

    @Test
    public void testJson2CsvForOne() throws Exception {
        List<FieldInfo> fields = this.getTestFieldList("sid", "packageID", "msgTime", "msg");
        JsonSourceInfo jsonSource = new JsonSourceInfo("UTF-8", "");
        CsvSinkInfo csvSink = new CsvSinkInfo("UTF-8", '|', '\\', fields);
        String transformSql = "select $root.sid,$root.packageID,$root.msgs(1).msgTime,$root.msgs(0).msg from source";
        TransformConfig config = new TransformConfig(transformSql);
        // case1
        TransformProcessor<String, String> processor = TransformProcessor
                .create(config, SourceDecoderFactory.createJsonDecoder(jsonSource),
                        SinkEncoderFactory.createCsvEncoder(csvSink));
        String srcString = "{\n"
                + "  \"sid\":\"value1\",\n"
                + "  \"packageID\":\"value2\",\n"
                + "  \"msgs\":[\n"
                + "  {\"msg\":\"value4\",\"msgTime\":1713243918000},\n"
                + "  {\"msg\":\"v4\",\"msgTime\":1713243918000}\n"
                + "  ]\n"
                + "}";
        List<String> output = processor.transform(srcString, new HashMap<>());
        Assert.assertEquals(1, output.size());
        Assert.assertEquals(output.get(0), "value1|value2|1713243918000|value4");
    }
}
