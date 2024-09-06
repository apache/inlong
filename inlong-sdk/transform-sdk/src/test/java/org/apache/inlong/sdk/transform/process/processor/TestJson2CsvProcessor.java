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

public class TestJson2CsvProcessor extends AbstractProcessorTestBase {

    @Test
    public void testJson2Csv() throws Exception {
        List<FieldInfo> fields1 = this.getTestFieldList("sid", "packageID", "msgTime", "msg");
        JsonSourceInfo jsonSource1 = new JsonSourceInfo("UTF-8", "msgs");
        CsvSinkInfo csvSink1 = new CsvSinkInfo("UTF-8", '|', '\\', fields1);
        String transformSql1 = "select $root.sid,$root.packageID,$child.msgTime,$child.msg from source";
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
        List<String> output1 = processor1.transform(srcString1, new HashMap<>());
        Assert.assertEquals(2, output1.size());
        Assert.assertEquals(output1.get(0), "value1|value2|1713243918000|value4");
        Assert.assertEquals(output1.get(1), "value1|value2|1713243918000|v4");
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
