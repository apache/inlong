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
import org.apache.inlong.sdk.transform.pojo.CsvSourceInfo;
import org.apache.inlong.sdk.transform.pojo.FieldInfo;
import org.apache.inlong.sdk.transform.pojo.TransformConfig;
import org.apache.inlong.sdk.transform.process.TransformProcessor;

import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class TestCsv2StarProcessor extends AbstractProcessorTestBase {

    @Test
    public void testCsv2Star() throws Exception {
        List<FieldInfo> fields = this.getTestFieldList("ftime", "extinfo");
        CsvSourceInfo csvSource = new CsvSourceInfo("UTF-8", '|', '\\', fields);
        CsvSinkInfo csvSink = new CsvSinkInfo("UTF-8", '|', '\\', new ArrayList<>());
        String transformSql = "select *";
        TransformConfig config = new TransformConfig(transformSql);
        // case1
        TransformProcessor<String, String> processor1 = TransformProcessor
                .create(config, SourceDecoderFactory.createCsvDecoder(csvSource),
                        SinkEncoderFactory.createCsvEncoder(csvSink));

        List<String> output1 = processor1.transform("2024-04-28 00:00:00|ok", new HashMap<>());
        Assert.assertEquals(1, output1.size());
        Assert.assertEquals(output1.get(0), "2024-04-28 00:00:00|ok");
        // case2
        config.setTransformSql("select * from source where extinfo!='ok'");
        TransformProcessor<String, String> processor2 = TransformProcessor
                .create(config, SourceDecoderFactory.createCsvDecoder(csvSource),
                        SinkEncoderFactory.createCsvEncoder(csvSink));

        List<String> output2 = processor2.transform("2024-04-28 00:00:00|ok", new HashMap<>());
        Assert.assertEquals(0, output2.size());
        // case3
        config.setTransformSql("select *,extinfo,ftime from source where extinfo!='ok'");
        TransformProcessor<String, String> processor3 = TransformProcessor
                .create(config, SourceDecoderFactory.createCsvDecoder(csvSource),
                        SinkEncoderFactory.createCsvEncoder(csvSink));

        List<String> output3 = processor3.transform("2024-04-28 00:00:00|nok", new HashMap<>());
        Assert.assertEquals(1, output3.size());
        Assert.assertEquals(output3.get(0), "2024-04-28 00:00:00|nok|nok|2024-04-28 00:00:00");
        // case4
        CsvSourceInfo csvSourceNoField = new CsvSourceInfo("UTF-8", '|', '\\', new ArrayList<>());
        CsvSinkInfo csvSinkNoField = new CsvSinkInfo("UTF-8", '|', '\\', new ArrayList<>());
        config.setTransformSql("select *,$2,$1 from source where $2='nok'");
        TransformProcessor<String, String> processor4 = TransformProcessor
                .create(config, SourceDecoderFactory.createCsvDecoder(csvSourceNoField),
                        SinkEncoderFactory.createCsvEncoder(csvSinkNoField));

        List<String> output4 = processor4.transform("2024-04-28 00:00:00|nok", new HashMap<>());
        Assert.assertEquals(1, output4.size());
        Assert.assertEquals(output4.get(0), "nok|2024-04-28 00:00:00");
    }
}
