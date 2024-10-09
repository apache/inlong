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
import org.apache.inlong.sdk.transform.pojo.XmlSourceInfo;
import org.apache.inlong.sdk.transform.process.TransformProcessor;

import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.List;

public class TestXml2CsvProcessor extends AbstractProcessorTestBase {

    @Test
    public void testXml2Csv() throws Exception {
        List<FieldInfo> fields = null;
        XmlSourceInfo xmlSource = null;
        CsvSinkInfo csvSink = null;
        String transformSql = null;
        TransformConfig config = null;
        TransformProcessor<String, String> processor = null;
        String srcString = null;
        List<String> output = null;

        // case1
        fields = this.getTestFieldList("sid", "packageID", "msgTime", "msg");
        xmlSource = new XmlSourceInfo("UTF-8", "msgs");
        csvSink = new CsvSinkInfo("UTF-8", '|', '\\', fields);
        transformSql = "select $root.sid,$root.packageID,$child.data,$child.msgTime from source";
        config = new TransformConfig(transformSql);
        processor = TransformProcessor
                .create(config, SourceDecoderFactory.createXmlDecoder(xmlSource),
                        SinkEncoderFactory.createCsvEncoder(csvSink));
        srcString = "<Message>\n" +
                "    <sid>sid</sid>\n" +
                "    <packageID>pid</packageID>\n" +
                "    <msgs>\n" +
                "        <msg>\n" +
                "            <data>value1</data>\n" +
                "            <msgTime>Time1</msgTime>\n" +
                "        </msg>\n" +
                "        <msg>\n" +
                "            <data>value2</data>\n" +
                "            <msgTime>Time2</msgTime>\n" +
                "        </msg>\n" +
                "    </msgs>\n" +
                "</Message>";
        output = processor.transform(srcString, new HashMap<>());
        Assert.assertEquals(2, output.size());
        Assert.assertEquals(output.get(0), "sid|pid|value1|Time1");
        Assert.assertEquals(output.get(1), "sid|pid|value2|Time2");

        // case2
        xmlSource = new XmlSourceInfo("UTF-8", "Persons");
        csvSink = new CsvSinkInfo("UTF-8", '|', '\\', fields);
        transformSql = "select $root.sid,$root.packageID,$child.data,$child.habbies(2).name from source";
        config = new TransformConfig(transformSql);
        processor = TransformProcessor
                .create(config, SourceDecoderFactory.createXmlDecoder(xmlSource),
                        SinkEncoderFactory.createCsvEncoder(csvSink));
        srcString = "<Infomations>\n" +
                "    <sid>sid</sid>\n" +
                "    <packageID>pid</packageID>\n" +
                "    <Persons>\n" +
                "        <Person>\n" +
                "            <data>value1</data>\n" +
                "            <msgTime>time1</msgTime>\n" +
                "            <habbies>\n" +
                "                <habby>\n" +
                "                    <index>1</index>\n" +
                "                    <name>sing1</name>\n" +
                "                </habby>\n" +
                "                <habby>\n" +
                "                    <index>2</index>\n" +
                "                    <name>dance1</name>\n" +
                "                </habby>\n" +
                "                <habby>\n" +
                "                    <index>3</index>\n" +
                "                    <name>rap1</name>\n" +
                "                </habby>\n" +
                "            </habbies>\n" +
                "        </Person>\n" +
                "        <Person>\n" +
                "            <data>value2</data>\n" +
                "            <msgTime>time2</msgTime>\n" +
                "            <habbies>\n" +
                "                <habby>\n" +
                "                    <index>1</index>\n" +
                "                    <name>sing2</name>\n" +
                "                </habby>\n" +
                "                <habby>\n" +
                "                    <index>2</index>\n" +
                "                    <name>dance2</name>\n" +
                "                </habby>\n" +
                "                <habby>\n" +
                "                    <index>3</index>\n" +
                "                    <name>rap2</name>\n" +
                "                </habby>\n" +
                "            </habbies>\n" +
                "        </Person>\n" +
                "    </Persons>\n" +
                "</Infomations>";
        output = processor.transform(srcString, new HashMap<>());
        Assert.assertEquals(2, output.size());
        Assert.assertEquals("sid|pid|value1|rap1", output.get(0));
        Assert.assertEquals("sid|pid|value2|rap2", output.get(1));
    }

    @Test
    public void testXml2CsvForOne() throws Exception {
        List<FieldInfo> fields = null;
        XmlSourceInfo xmlSource = null;
        CsvSinkInfo csvSink = null;
        String transformSql = null;
        TransformConfig config = null;
        TransformProcessor<String, String> processor = null;
        String srcString = null;
        List<String> output = null;

        // case1
        fields = this.getTestFieldList("sid", "packageID", "msgTime", "msg");
        xmlSource = new XmlSourceInfo("UTF-8", "");
        csvSink = new CsvSinkInfo("UTF-8", '|', '\\', fields);
        transformSql = "select $root.sid,$root.packageID,$root.msgs(1).msgTime,$root.msgs(0).data from source";
        config = new TransformConfig(transformSql);
        processor = TransformProcessor
                .create(config, SourceDecoderFactory.createXmlDecoder(xmlSource),
                        SinkEncoderFactory.createCsvEncoder(csvSink));
        srcString = "<Message>\n" +
                "    <sid>sid</sid>\n" +
                "    <packageID>pid</packageID>\n" +
                "    <msgs>\n" +
                "        <msg>\n" +
                "            <data>value1</data>\n" +
                "            <msgTime>Time1</msgTime>\n" +
                "        </msg>\n" +
                "        <msg>\n" +
                "            <data>value2</data>\n" +
                "            <msgTime>Time2</msgTime>\n" +
                "        </msg>\n" +
                "    </msgs>\n" +
                "</Message>";
        output = processor.transform(srcString, new HashMap<>());
        Assert.assertEquals(1, output.size());
        Assert.assertEquals(output.get(0), "sid|pid|Time2|value1");
    }
}
