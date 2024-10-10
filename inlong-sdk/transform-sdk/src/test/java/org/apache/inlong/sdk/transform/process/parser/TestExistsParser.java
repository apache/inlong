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

package org.apache.inlong.sdk.transform.process.parser;

import org.apache.inlong.sdk.transform.decode.SourceDecoderFactory;
import org.apache.inlong.sdk.transform.encode.SinkEncoderFactory;
import org.apache.inlong.sdk.transform.pojo.TransformConfig;
import org.apache.inlong.sdk.transform.process.TransformProcessor;
import org.apache.inlong.sdk.transform.process.operator.AbstractOperatorTestBase;

import org.junit.Assert;
import org.junit.Test;

import java.util.List;

public class TestExistsParser extends AbstractOperatorTestBase {

    @Test
    public void testExistsParser() throws Exception {
        String transformSql = null, data = null;
        TransformConfig config = null;
        TransformProcessor<String, String> processor = null;
        List<String> output = null;

        transformSql = "select exists if(numeric1 >= 4,1,0) from source";
        config = new TransformConfig(transformSql);
        processor = TransformProcessor
                .create(config, SourceDecoderFactory.createCsvDecoder(csvSource),
                        SinkEncoderFactory.createKvEncoder(kvSink));

        // case1: exists if(3.14159265358979323846 >= 4,1,0)
        List<String> output1 = processor.transform("3.14159265358979323846|3a|4|8");
        Assert.assertEquals(1, output1.size());
        Assert.assertEquals(output1.get(0), "result=true");

        transformSql = "select exists tan(numeric1) from source";
        config = new TransformConfig(transformSql);
        processor = TransformProcessor
                .create(config, SourceDecoderFactory.createCsvDecoder(csvSource),
                        SinkEncoderFactory.createKvEncoder(kvSink));

        // case2: exists tan(null)
        List<String> output2 = processor.transform("|3a|6|8");
        Assert.assertEquals(1, output2.size());
        Assert.assertEquals(output2.get(0), "result=false");

        transformSql = "select exists numeric1 from source";
        config = new TransformConfig(transformSql);
        processor = TransformProcessor
                .create(config, SourceDecoderFactory.createCsvDecoder(csvSource),
                        SinkEncoderFactory.createKvEncoder(kvSink));

        // case3: exists 3.14159265358979323846
        List<String> output3 = processor.transform("3.14159265358979323846|5|4|8");
        Assert.assertEquals(1, output3.size());
        Assert.assertEquals(output3.get(0), "result=true");

        transformSql = "select exists tan(numeric1) from source";
        config = new TransformConfig(transformSql);
        processor = TransformProcessor
                .create(config, SourceDecoderFactory.createCsvDecoder(csvSource),
                        SinkEncoderFactory.createKvEncoder(kvSink));

        // case4: exists tan(3.14159265358979323846)
        List<String> output4 = processor.transform("3.14159265358979323846|4|4|8");
        Assert.assertEquals(1, output4.size());
        Assert.assertEquals(output4.get(0), "result=true");

        transformSql = "select exists trim(xxd) from source";
        config = new TransformConfig(transformSql);
        processor = TransformProcessor
                .create(config, SourceDecoderFactory.createCsvDecoder(csvSource),
                        SinkEncoderFactory.createKvEncoder(kvSink));

        // case5: exists trim(xxd)
        List<String> output5 = processor.transform("3.14159265358979323846|4|3.2|8");
        Assert.assertEquals(1, output5.size());
        Assert.assertEquals(output5.get(0), "result=false");
    }
}
