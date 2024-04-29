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

package org.apache.inlong.sdk.transform.process;

import org.apache.inlong.sdk.transform.pojo.CsvSinkInfo;
import org.apache.inlong.sdk.transform.pojo.CsvSourceInfo;
import org.apache.inlong.sdk.transform.pojo.FieldInfo;
import org.apache.inlong.sdk.transform.pojo.JsonSourceInfo;
import org.apache.inlong.sdk.transform.pojo.KvSinkInfo;
import org.apache.inlong.sdk.transform.pojo.KvSourceInfo;
import org.apache.inlong.sdk.transform.pojo.SinkInfo;
import org.apache.inlong.sdk.transform.pojo.SourceInfo;
import org.apache.inlong.sdk.transform.pojo.TransformConfig;

import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * TestTransformProcessor
 * 
 */
public class TestTransformProcessor {

    @Test
    public void testCsv2Kv() {
        try {
            List<FieldInfo> fields = new ArrayList<>();
            FieldInfo ftime = new FieldInfo();
            ftime.setName("ftime");
            fields.add(ftime);
            FieldInfo extinfo = new FieldInfo();
            extinfo.setName("extinfo");
            fields.add(extinfo);
            SourceInfo csvSource = new CsvSourceInfo("UTF-8", "|", "\\", fields);
            SinkInfo kvSink = new KvSinkInfo("UTF-8", fields);
            String transformSql = "select ftime,extinfo from source where extinfo='ok'";
            TransformConfig config = new TransformConfig(csvSource, kvSink, transformSql);
            // case1
            TransformProcessor processor1 = new TransformProcessor(config);
            List<String> output1 = processor1.transform("2024-04-28 00:00:00|ok", new HashMap<>());
            Assert.assertTrue(output1.size() == 1);
            Assert.assertEquals(output1.get(0), "ftime=2024-04-28 00:00:00&extinfo=ok");
            // case2
            config.setTransformSql("select ftime,extinfo from source where extinfo!='ok'");
            TransformProcessor processor2 = new TransformProcessor(config);
            List<String> output2 = processor2.transform("2024-04-28 00:00:00|ok", new HashMap<>());
            Assert.assertTrue(output2.size() == 0);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testKv2Csv() {
        try {
            List<FieldInfo> fields = new ArrayList<>();
            FieldInfo ftime = new FieldInfo();
            ftime.setName("ftime");
            fields.add(ftime);
            FieldInfo extinfo = new FieldInfo();
            extinfo.setName("extinfo");
            fields.add(extinfo);
            SourceInfo kvSource = new KvSourceInfo("UTF-8", fields);
            SinkInfo csvSink = new CsvSinkInfo("UTF-8", "|", "\\", fields);
            String transformSql = "select ftime,extinfo from source where extinfo='ok'";
            TransformConfig config = new TransformConfig(kvSource, csvSink, transformSql);
            // case1
            TransformProcessor processor1 = new TransformProcessor(config);
            List<String> output1 = processor1.transform("ftime=2024-04-28 00:00:00&extinfo=ok", new HashMap<>());
            Assert.assertTrue(output1.size() == 1);
            Assert.assertEquals(output1.get(0), "2024-04-28 00:00:00|ok");
            // case2
            config.setTransformSql("select ftime,extinfo from source where extinfo!='ok'");
            TransformProcessor processor2 = new TransformProcessor(config);
            List<String> output2 = processor2.transform("ftime=2024-04-28 00:00:00&extinfo=ok", new HashMap<>());
            Assert.assertTrue(output2.size() == 0);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testJson2Csv() {
        try {
            List<FieldInfo> fields = new ArrayList<>();
            FieldInfo sid = new FieldInfo();
            sid.setName("sid");
            fields.add(sid);
            FieldInfo packageID = new FieldInfo();
            packageID.setName("packageID");
            fields.add(packageID);
            FieldInfo msgTime = new FieldInfo();
            msgTime.setName("msgTime");
            fields.add(msgTime);
            FieldInfo msg = new FieldInfo();
            msg.setName("msg");
            fields.add(msg);
            SourceInfo jsonSource = new JsonSourceInfo("UTF-8", "msgs");
            SinkInfo csvSink = new CsvSinkInfo("UTF-8", "|", "\\", fields);
            String transformSql = "select $root.sid,$root.packageID,$child.msgTime,$child.msg from source";
            TransformConfig config = new TransformConfig(jsonSource, csvSink, transformSql);
            // case1
            TransformProcessor processor = new TransformProcessor(config);
            String srcString = "{\n"
                    + "  \"sid\":\"value1\",\n"
                    + "  \"packageID\":\"value2\",\n"
                    + "  \"msgs\":[\n"
                    + "  {\"msg\":\"value4\",\"msgTime\":1713243918000},\n"
                    + "  {\"msg\":\"v4\",\"msgTime\":1713243918000}\n"
                    + "  ]\n"
                    + "}";
            List<String> output = processor.transform(srcString, new HashMap<>());
            Assert.assertTrue(output.size() == 2);
            Assert.assertEquals(output.get(0), "value1|value2|1713243918000|value4");
            Assert.assertEquals(output.get(1), "value1|value2|1713243918000|v4");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testKvCsvByJsonConfig() {
        try {
            String configString1 = "{\"sourceInfo\":{\"type\":\"kv\",\"charset\":\"UTF-8\","
                    + "\"fields\":[{\"name\":\"ftime\"},{\"name\":\"extinfo\"}]},"
                    + "\"sinkInfo\":{\"type\":\"csv\",\"charset\":\"UTF-8\",\"delimiter\":\"|\","
                    + "\"escapeChar\":\"\\\\\","
                    + "\"fields\":[{\"name\":\"ftime\"},{\"name\":\"extinfo\"}]},"
                    + "\"transformSql\":\"select ftime,extinfo from source where extinfo='ok'\"}";
            // case1
            TransformProcessor processor1 = new TransformProcessor(configString1);
            List<String> output1 = processor1.transform("ftime=2024-04-28 00:00:00&extinfo=ok", new HashMap<>());
            Assert.assertTrue(output1.size() == 1);
            Assert.assertEquals(output1.get(0), "2024-04-28 00:00:00|ok");
            // case2
            String configString2 = "{\"sourceInfo\":{\"type\":\"kv\",\"charset\":\"UTF-8\","
                    + "\"fields\":[{\"name\":\"ftime\"},{\"name\":\"extinfo\"}]},"
                    + "\"sinkInfo\":{\"type\":\"csv\",\"charset\":\"UTF-8\",\"delimiter\":\"|\","
                    + "\"escapeChar\":\"\\\\\","
                    + "\"fields\":[{\"name\":\"ftime\"},{\"name\":\"extinfo\"}]},"
                    + "\"transformSql\":\"select ftime,extinfo from source where extinfo!='ok'\"}";
            TransformProcessor processor2 = new TransformProcessor(configString2);
            List<String> output2 = processor2.transform("ftime=2024-04-28 00:00:00&extinfo=ok", new HashMap<>());
            Assert.assertTrue(output2.size() == 0);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
