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
import org.apache.inlong.sdk.transform.pojo.CsvSourceInfo;
import org.apache.inlong.sdk.transform.pojo.FieldInfo;
import org.apache.inlong.sdk.transform.pojo.KvSinkInfo;
import org.apache.inlong.sdk.transform.pojo.TransformConfig;
import org.apache.inlong.sdk.transform.process.TransformProcessor;

import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TestCsv2KvProcessor extends AbstractProcessorTestBase {

    @Test
    public void testCsv2Kv() throws Exception {
        List<FieldInfo> fields = this.getTestFieldList("ftime", "extinfo", "ds");
        CsvSourceInfo csvSource = new CsvSourceInfo("UTF-8", '|', '\\', fields);
        KvSinkInfo kvSink = new KvSinkInfo("UTF-8", fields);
        String transformSql = "select ftime,extinfo,$ctx.partition from source where extinfo='ok'";
        TransformConfig config = new TransformConfig(transformSql);
        // case1
        TransformProcessor<String, String> processor1 = TransformProcessor
                .create(config, SourceDecoderFactory.createCsvDecoder(csvSource),
                        SinkEncoderFactory.createKvEncoder(kvSink));

        Map<String, Object> extParams = new HashMap<>();
        extParams.put("partition", "2024042801");
        List<String> output1 = processor1.transform("2024-04-28 00:00:00|ok", extParams);
        Assert.assertEquals(1, output1.size());
        Assert.assertEquals(output1.get(0), "ftime=2024-04-28 00:00:00&extinfo=ok&ds=2024042801");
        // case2
        config.setTransformSql("select ftime,extinfo from source where extinfo!='ok'");
        TransformProcessor<String, String> processor2 = TransformProcessor
                .create(config, SourceDecoderFactory.createCsvDecoder(csvSource),
                        SinkEncoderFactory.createKvEncoder(kvSink));

        List<String> output2 = processor2.transform("2024-04-28 00:00:00|ok", new HashMap<>());
        Assert.assertEquals(0, output2.size());
    }

    @Test
    public void testCsv2KvNoField() throws Exception {
        CsvSourceInfo csvSource = new CsvSourceInfo("UTF-8", '|', '\\', null);
        KvSinkInfo kvSink = new KvSinkInfo("UTF-8", null);
        String transformSql = "select $1 ftime,$2 extinfo from source where $2='ok'";
        TransformConfig config = new TransformConfig(transformSql);
        // case1
        TransformProcessor<String, String> processor1 = TransformProcessor
                .create(config, SourceDecoderFactory.createCsvDecoder(csvSource),
                        SinkEncoderFactory.createKvEncoder(kvSink));
        List<String> output1 = processor1.transform("2024-04-28 00:00:00|ok", new HashMap<>());
        Assert.assertEquals(1, output1.size());
        Assert.assertEquals(output1.get(0), "ftime=2024-04-28 00:00:00&extinfo=ok");
        // case2
        config.setTransformSql("select $1 ftime,$2 extinfo from source where $2!='ok'");
        TransformProcessor<String, String> processor2 = TransformProcessor
                .create(config, SourceDecoderFactory.createCsvDecoder(csvSource),
                        SinkEncoderFactory.createKvEncoder(kvSink));
        List<String> output2 = processor2.transform("2024-04-28 00:00:00|ok", new HashMap<>());
        Assert.assertEquals(0, output2.size());
    }
}
