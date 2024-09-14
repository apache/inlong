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
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public class TestRegexExtractFunction extends AbstractFunctionStringTestBase {

    @Test
    public void testRegexpExtractFunction() throws Exception {
        String transformSql1 = "select regexp_extract(string1,string2,numeric1) from source";
        TransformConfig config1 = new TransformConfig(transformSql1);
        TransformProcessor<String, String> processor1 = TransformProcessor
                .create(config1, SourceDecoderFactory.createCsvDecoder(csvSource),
                        SinkEncoderFactory.createKvEncoder(kvSink));
        // case1: regexp_extract("abc123def", "(\\d+)", 1)
        List<String> output1 = processor1.transform("abc123def|(\\\\d+)|2|1|3|4", new HashMap<>());
        Assert.assertEquals(1, output1.size());
        Assert.assertEquals(output1.get(0), "result=123");
        // case2: regexp_extract("abc123def123", "(\\d+)", 0)
        List<String> output2 = processor1.transform("abc123def124|(\\\\d+)|1|0|3", new HashMap<>());
        Assert.assertEquals(1, output2.size());
        Assert.assertEquals(output2.get(0), "result=123");
        // case3: regexp_extract("Name: John, Age: 25, Location: NY", "Name: (\\w+), Age: (\\d+), Location: (\\w+)", 2)
        List<String> output3 = processor1.transform(
                "Name: John, Age: 25, Location: NY|Name: (\\\\w+), Age: (\\\\d+), Location: (\\\\w+)|1|2|3",
                new HashMap<>());
        Assert.assertEquals(1, output3.size());
        Assert.assertEquals(output3.get(0), "result=25");
        // case4: regexp_extract("Email: john.doe@example.com", "([a-zA-Z]+)\\.([a-zA-Z]+)@([a-zA-Z]+)\\.([a-zA-Z]+)",
        // 3)
        List<String> output4 = processor1.transform(
                "Email: john.doe@example.com|([a-zA-Z]+)\\\\.([a-zA-Z]+)@([a-zA-Z]+)\\\\.([a-zA-Z]+)|1|3|2",
                new HashMap<>());
        Assert.assertEquals(1, output4.size());
        Assert.assertEquals(output4.get(0), "result=example");

        String transformSql2 = "select regexp_extract(string1) from source";
        TransformConfig config2 = new TransformConfig(transformSql2);
        TransformProcessor<String, String> processor2 = TransformProcessor
                .create(config2, SourceDecoderFactory.createCsvDecoder(csvSource),
                        SinkEncoderFactory.createKvEncoder(kvSink));
        // case4: regexp_extract("The quick brown fox quick")
        List<String> output5 =
                processor2.transform("The quick brown fox quick|quick|QAQ|2|1|3", new HashMap<>());
        Assert.assertEquals(1, output5.size());
        Assert.assertEquals(output5.get(0), "result=");
        String transformSql3 = "select regexp_extract(string1,string2) from source";
        TransformConfig config3 = new TransformConfig(transformSql3);
        TransformProcessor<String, String> processor3 = TransformProcessor
                .create(config3, SourceDecoderFactory.createCsvDecoder(csvSource),
                        SinkEncoderFactory.createKvEncoder(kvSink));
        // case5: regexp_extract("The quick brown fox quick", "[q-")
        List<String> output6 =
                processor3.transform("The quick brown fox quick|[q-|QAQ|2|1|3", new HashMap<>());
        Assert.assertEquals(1, output6.size());
        PatternSyntaxException exception = assertThrows(PatternSyntaxException.class, () -> {
            Pattern.compile("[q-");
        });
        assertTrue(exception.getMessage().contains("Illegal character range near index 3"));
    }
}
