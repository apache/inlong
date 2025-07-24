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

package org.apache.inlong.sdk.transform.process.function.string;

import org.apache.inlong.sdk.transform.decode.SourceDecoderFactory;
import org.apache.inlong.sdk.transform.encode.SinkEncoderFactory;
import org.apache.inlong.sdk.transform.pojo.TransformConfig;
import org.apache.inlong.sdk.transform.process.TransformProcessor;

import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.List;

public class TestUrlDecodeFunction extends AbstractFunctionStringTestBase {

    @Test
    public void testUrlDecodeFunction() throws Exception {
        String transformSql = "select url_decode(string1) from source";
        TransformConfig config = new TransformConfig(transformSql);
        TransformProcessor<String, String> processor = TransformProcessor
                .create(config, SourceDecoderFactory.createCsvDecoder(csvSource),
                        SinkEncoderFactory.createKvEncoder(kvSink));

        // case1: url_decode('https%3A%2F%2Fwww.google.com%2Fsearch%3Fq%3Djava+url+encode')
        List<String> output1 = processor.transform(
                "https%3A%2F%2Fwww.google.com%2Fsearch%3Fq%3Djava+url+encode|banana|cloud|1", new HashMap<>());
        Assert.assertEquals(1, output1.size());
        Assert.assertEquals(output1.get(0), "result=https://www.google.com/search?q=java url encode");

        String transformSql2 = "select url_decode(stringX) from source";
        TransformConfig config2 = new TransformConfig(transformSql2);
        TransformProcessor<String, String> processor2 = TransformProcessor
                .create(config2, SourceDecoderFactory.createCsvDecoder(csvSource),
                        SinkEncoderFactory.createKvEncoder(kvSink));
        // case2: url_decode(null)
        List<String> output2 = processor2.transform("|apple|banana|cloud|1", new HashMap<>());
        Assert.assertEquals(1, output2.size());
        Assert.assertEquals(output2.get(0), "result=");
    }

    @Test
    public void testUrlDecodeCharsetFunction() throws Exception {
        String transformSql = "select url_decode(string1,'GBK') from source";
        TransformConfig config = new TransformConfig(transformSql);
        TransformProcessor<String, String> processor = TransformProcessor
                .create(config, SourceDecoderFactory.createCsvDecoder(csvSource),
                        SinkEncoderFactory.createKvEncoder(kvSink));

        // case1: url_decode('A160%3D%C9%C7%CD%B7%CA%D0%26vuserid%3D%26version_build%3D76','GBK')
        List<String> output1 = processor.transform(
                "A160%3D%C9%C7%CD%B7%CA%D0%26vuserid%3D%26version_build%3D76|banana|cloud|1", new HashMap<>());
        Assert.assertEquals(1, output1.size());
        Assert.assertEquals(output1.get(0), "result=A160=汕头市&vuserid=&version_build=76");

        String transformSql2 = "select url_decode(string1,'UTF-8') from source";
        TransformConfig config2 = new TransformConfig(transformSql2);
        TransformProcessor<String, String> processor2 = TransformProcessor
                .create(config2, SourceDecoderFactory.createCsvDecoder(csvSource),
                        SinkEncoderFactory.createKvEncoder(kvSink));
        // case2: url_decode('A160%3D%E6%B1%95%E5%A4%B4%E5%B8%82%26vuserid%3D%26version_build%3D76','UTF-8')
        List<String> output2 = processor2.transform(
                "A160%3D%E6%B1%95%E5%A4%B4%E5%B8%82%26vuserid%3D%26version_build%3D76|banana|cloud|1",
                new HashMap<>());
        Assert.assertEquals(1, output2.size());
        Assert.assertEquals(output2.get(0), "result=A160=汕头市&vuserid=&version_build=76");

        String transformSql3 =
                "select json_query(parse_url(url_decode(string1,'GBK'),'QUERY','udf_kv'),'$.vcid') from source";
        TransformConfig config3 = new TransformConfig(transformSql3);
        TransformProcessor<String, String> processor3 = TransformProcessor
                .create(config3, SourceDecoderFactory.createCsvDecoder(csvSource),
                        SinkEncoderFactory.createKvEncoder(kvSink));
        // case3: url_decode('A160%3D%C9%C7%CD%B7%CA%D0%26udf_kv%3D%7B%22vcid%22%3A%22%C9%C7%CD%B7%CA%D0%22%7D','GBK')
        List<String> output3 = processor3.transform(
                "https%3A%2F%2Fwww.google.com%2Fsearch%3F"
                        + "A160%3D%C9%C7%CD%B7%CA%D0%26udf_kv%3D%7B%22vcid%22%3A%22%C9%C7%CD%B7%CA%D0%22%7D|banana|cloud|1",
                new HashMap<>());
        Assert.assertEquals(1, output3.size());
        Assert.assertEquals(output3.get(0), "result=汕头市");
    }
}
