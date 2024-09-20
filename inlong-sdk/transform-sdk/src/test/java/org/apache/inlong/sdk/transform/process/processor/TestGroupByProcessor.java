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

public class TestGroupByProcessor extends AbstractProcessorTestBase {

    @Test
    public void testJsonGroupBy() throws Exception {
        List<FieldInfo> fields1 = this.getTestFieldList("sid", "packageID", "msgTime", "msg");
        JsonSourceInfo jsonSource1 = new JsonSourceInfo("UTF-8", "msgs");
        CsvSinkInfo csvSink1 = new CsvSinkInfo("UTF-8", '|', '\\', fields1);
        String transformSql1 = "select $root.sid,$root.packageID,$child.msgTime,$child.msg from source " +
                "group by $child.msgTime,$child.msg";
        TransformConfig config1 = new TransformConfig(transformSql1);
        // case1
        TransformProcessor<String, String> processor1 = TransformProcessor
                .create(config1, SourceDecoderFactory.createJsonDecoder(jsonSource1),
                        SinkEncoderFactory.createCsvEncoder(csvSink1));
        String srcString = "{\n"
                + "  \"sid\":\"value1\",\n"
                + "  \"packageID\":\"value2\",\n"
                + "  \"msgs\":[\n"
                + "  {\"msg\":\"value3\",\"msgTime\":1713243918000},\n"
                + "  {\"msg\":\"value4\",\"msgTime\":1713243918001},\n"
                + "  {\"msg\":\"value5\",\"msgTime\":1713243918002},\n"
                + "  {\"msg\":\"value6\",\"msgTime\":1713243918003},\n"
                + "  {\"msg\":\"value3\",\"msgTime\":1713243918000},\n"
                + "  {\"msg\":\"value8\",\"msgTime\":1713243918001},\n"
                + "  {\"msg\":\"value9\",\"msgTime\":1713243918002},\n"
                + "  {\"msg\":\"value10\",\"msgTime\":1713243918003}\n"
                + "  ]\n"
                + "}";
        List<String> output1 = processor1.transform(srcString, new HashMap<>());
        Assert.assertEquals(7, output1.size());
        Assert.assertEquals(output1.get(0), "value1|value2|1713243918000|value3");
        Assert.assertEquals(output1.get(1), "value1|value2|1713243918001|value8");
        Assert.assertEquals(output1.get(2), "value1|value2|1713243918002|value5");
        Assert.assertEquals(output1.get(3), "value1|value2|1713243918003|value10");
        Assert.assertEquals(output1.get(4), "value1|value2|1713243918002|value9");
        Assert.assertEquals(output1.get(5), "value1|value2|1713243918001|value4");
        Assert.assertEquals(output1.get(6), "value1|value2|1713243918003|value6");
    }

    @Test
    public void testJsonGroupByForOneField() throws Exception {
        List<FieldInfo> fields1 = this.getTestFieldList("sid", "packageID", "msgTime", "msg");
        JsonSourceInfo jsonSource1 = new JsonSourceInfo("UTF-8", "msgs");
        CsvSinkInfo csvSink1 = new CsvSinkInfo("UTF-8", '|', '\\', fields1);
        String transformSql1 =
                "select $root.sid,$root.packageID,$child.msgTime,$child.msg from source group by $child.msgTime";
        TransformConfig config1 = new TransformConfig(transformSql1);
        // case1
        TransformProcessor<String, String> processor1 = TransformProcessor
                .create(config1, SourceDecoderFactory.createJsonDecoder(jsonSource1),
                        SinkEncoderFactory.createCsvEncoder(csvSink1));
        String srcString = "{\n"
                + "  \"sid\":\"value1\",\n"
                + "  \"packageID\":\"value2\",\n"
                + "  \"msgs\":[\n"
                + "  {\"msg\":\"value3\",\"msgTime\":1713243918000},\n"
                + "  {\"msg\":\"value4\",\"msgTime\":1713243918000},\n"
                + "  {\"msg\":\"value5\",\"msgTime\":1713243918001},\n"
                + "  {\"msg\":\"value6\",\"msgTime\":1713243918001},\n"
                + "  {\"msg\":\"value7\",\"msgTime\":1713243918000},\n"
                + "  {\"msg\":\"value8\",\"msgTime\":1713243918004},\n"
                + "  {\"msg\":\"value10\",\"msgTime\":1713243918005}\n"
                + "  ]\n"
                + "}";
        List<String> output1 = processor1.transform(srcString, new HashMap<>());
        Assert.assertEquals(4, output1.size());
        Assert.assertEquals(output1.get(0), "value1|value2|1713243918000|value7");
        Assert.assertEquals(output1.get(1), "value1|value2|1713243918001|value6");
        Assert.assertEquals(output1.get(2), "value1|value2|1713243918004|value8");
        Assert.assertEquals(output1.get(3), "value1|value2|1713243918005|value10");
    }
}
