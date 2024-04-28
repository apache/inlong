/**
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
    public void testKvCsv() {
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
}
