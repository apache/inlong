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

public class TestRegexpMatchesFunction extends AbstractFunctionStringTestBase {

    @Test
    public void testRegexpMatchesFunction() throws Exception {
        String transformSql1 = "select regexp_matches(string1,string2) from source";
        TransformConfig config1 = new TransformConfig(transformSql1);
        TransformProcessor<String, String> processor1 = TransformProcessor
                .create(config1, SourceDecoderFactory.createCsvDecoder(csvSource),
                        SinkEncoderFactory.createKvEncoder(kvSink));

        // case1: regexp_matches("The quick brown fox", "quick")
        List<String> output1 = processor1.transform("The quick brown fox|quick|5|2|1|3", new HashMap<>());
        Assert.assertEquals(1, output1.size());
        Assert.assertEquals(output1.get(0), "result=[{\"quick\"}]");
        String transformSql2 = "select regexp_matches(string1,string2) from source";
        TransformConfig config2 = new TransformConfig(transformSql2);
        TransformProcessor<String, String> processor2 = TransformProcessor
                .create(config2, SourceDecoderFactory.createCsvDecoder(csvSource),
                        SinkEncoderFactory.createKvEncoder(kvSink));

        // case2: regexp_matches("User: Alice, ID: 12345", "User: (\\w+), ID: (\\d+)")
        List<String> output2 =
                processor2.transform("User: Alice, ID: 12345|User: (\\\\w+), ID: (\\\\d+)|5|2|1|3", new HashMap<>());
        Assert.assertEquals(1, output2.size());
        Assert.assertEquals(output2.get(0), "result=[{\"Alice\",\"12345\"}]");

        // case3: regexp_matches("User: Alice, ID: 12345User: Alice, ID: 12345;
        // User: Bob, ID: 67890", "User: (\\w+), ID: (\\d+)")
        List<String> output3 =
                processor2.transform("User: Alice, ID: 12345|User: (\\\\w+), ID: (\\\\d+)|5|2|1|3", new HashMap<>());
        Assert.assertEquals(1, output3.size());
        Assert.assertEquals(output3.get(0), "result=[{\"Alice\",\"12345\"}]");
        String transformSql3 = "select regexp_matches(string1,string2,string3) from source";
        TransformConfig config3 = new TransformConfig(transformSql3);
        TransformProcessor<String, String> processor3 = TransformProcessor
                .create(config3, SourceDecoderFactory.createCsvDecoder(csvSource),
                        SinkEncoderFactory.createKvEncoder(kvSink));

        // case4: regexp_matches("foo 123 bar 456", "\\d+", "g")
        List<String> output4 = processor3.transform("foo 123 bar 456|\\\\d+|g|2|1|3", new HashMap<>());
        Assert.assertEquals(1, output4.size());
        Assert.assertEquals(output4.get(0), "result=[{\"123\"},{\"456\"}]");

        // case5: regexp_matches("User: Alice, ID: 12345User: Alice, ID: 12345;
        // User: Bob, ID: 67890", "User: (\\w+),ID: (\\d+)", "g")
        List<String> output5 = processor3.transform(
                "User: Alice, ID: 12345; User: Bob, ID: 67890|User: (\\\\w+), ID: (\\\\d+)|g|2|1|3", new HashMap<>());
        Assert.assertEquals(1, output5.size());
        Assert.assertEquals(output5.get(0), "result=[{\"Alice\",\"12345\"},{\"Bob\",\"67890\"}]");
        String transformSql4 = "select regexp_matches(string1,string2,string3) from source";
        TransformConfig config4 = new TransformConfig(transformSql4);
        TransformProcessor<String, String> processor4 = TransformProcessor
                .create(config4, SourceDecoderFactory.createCsvDecoder(csvSource),
                        SinkEncoderFactory.createKvEncoder(kvSink));

        // case6: regexp_matches("Hello! hello World", "hello", "ig")
        List<String> output6 = processor4.transform("Hello! hello World|hello|ig|2|1|3", new HashMap<>());
        Assert.assertEquals(1, output6.size());
        Assert.assertEquals(output6.get(0), "result=[{\"Hello\"},{\"hello\"}]");
        String transformSql5 = "select regexp_matches(string1,string2,string3) from source";
        TransformConfig config5 = new TransformConfig(transformSql5);
        TransformProcessor<String, String> processor5 = TransformProcessor
                .create(config5, SourceDecoderFactory.createCsvDecoder(csvSource),
                        SinkEncoderFactory.createKvEncoder(kvSink));

        // case7: regexp_matches("First line\nSecond line", "^Second", "m")
        List<String> output7 = processor5.transform("First line\\\nSecond line|^Second|m|2|1|3", new HashMap<>());
        Assert.assertEquals(1, output7.size());
        Assert.assertEquals(output7.get(0), "result=[{\"Second\"}]");

        // without 'g' flag
        // case7: regexp_matches("Hello! hello World", "hello", "i")
        List<String> output8 = processor5.transform("Hello! hello World|hello|i|2|1|3", new HashMap<>());
        Assert.assertEquals(1, output8.size());
        Assert.assertEquals(output8.get(0), "result=[{\"Hello\"}]");
    }
}
