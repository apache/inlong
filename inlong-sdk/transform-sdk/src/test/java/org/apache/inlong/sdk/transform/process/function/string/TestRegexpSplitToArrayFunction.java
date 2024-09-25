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

public class TestRegexpSplitToArrayFunction extends AbstractFunctionStringTestBase {

    @Test
    public void testRegexpSplitToArrayFunction() throws Exception {
        String transformSql1 = "select regexp_split_to_array(string1,string2) from source";
        TransformConfig config1 = new TransformConfig(transformSql1);
        TransformProcessor<String, String> processor1 = TransformProcessor
                .create(config1, SourceDecoderFactory.createCsvDecoder(csvSource),
                        SinkEncoderFactory.createKvEncoder(kvSink));
        // case1: regexp_split_to_array("hello,world", "\s+")
        List<String> output1 = processor1.transform("hello,world|\\s+|5|2|1|3", new HashMap<>());
        Assert.assertEquals(1, output1.size());
        Assert.assertEquals(output1.get(0), "result=[hello,world]");
        String transformSql2 = "select regexp_split_to_array(string1,string2) from source";
        TransformConfig config2 = new TransformConfig(transformSql2);
        TransformProcessor<String, String> processor2 = TransformProcessor
                .create(config2, SourceDecoderFactory.createCsvDecoder(csvSource),
                        SinkEncoderFactory.createKvEncoder(kvSink));
        // case2: regexp_split_to_array("User: Alice, ID: 12345", ":")
        List<String> output2 =
                processor2.transform("User: Alice, ID: 12345|:|5|2|1|3", new HashMap<>());
        Assert.assertEquals(1, output2.size());
        Assert.assertEquals(output2.get(0), "result=[User,  Alice, ID,  12345]");

        String transformSql3 = "select regexp_split_to_array(string1,string2,string3) from source";
        TransformConfig config3 = new TransformConfig(transformSql3);
        TransformProcessor<String, String> processor3 = TransformProcessor
                .create(config3, SourceDecoderFactory.createCsvDecoder(csvSource),
                        SinkEncoderFactory.createKvEncoder(kvSink));
        // case3: regexp_split_to_array("foo 123 bar 456", "\\d+", "g")
        List<String> output3 = processor3.transform("foo 123 bar 456|\\\\d+|g|2|1|3", new HashMap<>());
        Assert.assertEquals(1, output3.size());
        Assert.assertEquals(output3.get(0), "result=[foo ,  bar ]");

        // case4: regexp_split_to_array("foo 123 bAr 456", "bar", "i")
        List<String> output4 = processor3.transform("foo 123 bAr 456|bar|i|2|1|3", new HashMap<>());
        Assert.assertEquals(1, output4.size());
        Assert.assertEquals(output4.get(0), "result=[foo 123 ,  456]");

        // case5: regexp_split_to_array("Hello! hello World", "hello", "ig")
        List<String> output5 = processor3.transform("Hello! hello World|hello|ig|2|1|3", new HashMap<>());
        Assert.assertEquals(1, output5.size());
        Assert.assertEquals(output5.get(0), "result=[, ! ,  World]");

        // case6: regexp_split_to_array("First line\nSecond line", "^Second", "m")
        List<String> output6 = processor3.transform("First line\\\nSecond line|^Second|m|2|1|3", new HashMap<>());
        Assert.assertEquals(1, output6.size());
        Assert.assertEquals(output6.get(0), "result=[First line\n,  line]");

        // case7: regexp_split_to_array("Hello! hello World", "hello", "igx")
        List<String> output7 = processor3.transform("Hello! hello World|hello|igx|2|1|3", new HashMap<>());
        Assert.assertEquals(1, output7.size());
        Assert.assertEquals(output7.get(0), "result=[, ! ,  World]");

        // case6: regexp_split_to_array("First line\nSecond line", "^Second", "n")
        List<String> output8 = processor3.transform("First line\\\nSecond line|^Second|n|2|1|3", new HashMap<>());
        Assert.assertEquals(1, output8.size());
        Assert.assertEquals(output8.get(0), "result=[First line\n,  line]");
    }
}
