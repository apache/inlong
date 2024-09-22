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
import org.apache.inlong.sdk.transform.pojo.TransformConfig;
import org.apache.inlong.sdk.transform.pojo.YamlSourceInfo;
import org.apache.inlong.sdk.transform.process.TransformProcessor;

import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.List;

public class TestYaml2CsvProcessor extends AbstractProcessorTestBase {

    @Test
    public void testYaml2Csv() throws Exception {
        List<FieldInfo> fields = null;
        YamlSourceInfo yamlSource = null;
        CsvSinkInfo csvSink = null;
        String transformSql = null;
        TransformConfig config = null;
        TransformProcessor<String, String> processor = null;
        String srcString = null;
        List<String> output = null;

        // case1
        fields = this.getTestFieldList("sid", "packageID", "msgTime", "msg");
        yamlSource = new YamlSourceInfo("UTF-8", "msgs");
        csvSink = new CsvSinkInfo("UTF-8", '|', '\\', fields);
        transformSql = "select $root.sid,$root.packageID,$child.data,$child.msgTime from source";
        config = new TransformConfig(transformSql);
        processor = TransformProcessor
                .create(config, SourceDecoderFactory.createYamlDecoder(yamlSource),
                        SinkEncoderFactory.createCsvEncoder(csvSink));
        srcString = "sid: sid1\n" +
                "packageID: pid1\n" +
                "msgs:\n" +
                "  - data: value1\n" +
                "    msgTime: Time1\n" +
                "  - data: value2\n" +
                "    msgTime: Time2\n";
        output = processor.transform(srcString, new HashMap<>());
        Assert.assertEquals(2, output.size());
        Assert.assertEquals(output.get(0), "sid1|pid1|value1|Time1");
        Assert.assertEquals(output.get(1), "sid1|pid1|value2|Time2");

        // case2
        yamlSource = new YamlSourceInfo("UTF-8", "Persons");
        csvSink = new CsvSinkInfo("UTF-8", '|', '\\', fields);
        transformSql = "select $root.sid,$root.packageID,$child.data,$child.habbies(2).name from source";
        config = new TransformConfig(transformSql);
        processor = TransformProcessor
                .create(config, SourceDecoderFactory.createYamlDecoder(yamlSource),
                        SinkEncoderFactory.createCsvEncoder(csvSink));
        srcString = "sid: sid\n" +
                "packageID: pid\n" +
                "Persons:\n" +
                "  - data: value1\n" +
                "    habbies:\n" +
                "      - index: 1\n" +
                "        name: sing1\n" +
                "      - index: 2\n" +
                "        name: dance1\n" +
                "      - index: 3\n" +
                "        name: rap1\n" +
                "  - data: value2\n" +
                "    habbies:\n" +
                "      - index: 1\n" +
                "        name: sing2\n" +
                "      - index: 2\n" +
                "        name: dance2\n" +
                "      - index: 3\n" +
                "        name: rap2\n";
        output = processor.transform(srcString, new HashMap<>());
        Assert.assertEquals(2, output.size());
        Assert.assertEquals("sid|pid|value1|rap1", output.get(0));
        Assert.assertEquals("sid|pid|value2|rap2", output.get(1));
    }

    @Test
    public void testYaml2CsvForOne() throws Exception {
        List<FieldInfo> fields = null;
        YamlSourceInfo yamlSource = null;
        CsvSinkInfo csvSink = null;
        String transformSql = null;
        TransformConfig config = null;
        TransformProcessor<String, String> processor = null;
        String srcString = null;
        List<String> output = null;
        fields = this.getTestFieldList("sid", "packageID", "msgTime", "msg");
        yamlSource = new YamlSourceInfo("UTF-8", "");
        csvSink = new CsvSinkInfo("UTF-8", '|', '\\', fields);
        transformSql = "select $root.sid,$root.packageID,$root.msgs(1).msgTime,$root.msgs(0).data from source";
        config = new TransformConfig(transformSql);
        processor = TransformProcessor.create(config,
                SourceDecoderFactory.createYamlDecoder(yamlSource), SinkEncoderFactory.createCsvEncoder(csvSink));
        srcString = "Message:\n" +
                "sid: sid1\n" +
                "packageID: pid1\n" +
                "msgs:\n" +
                "  - data: value1\n" +
                "    msgTime: Time1\n" +
                "  - data: value2\n" +
                "    msgTime: Time2\n";
        output = processor.transform(srcString, new HashMap<>());
        Assert.assertEquals(1, output.size());
        Assert.assertEquals(output.get(0), "sid1|pid1|Time2|value1");
    }
}
