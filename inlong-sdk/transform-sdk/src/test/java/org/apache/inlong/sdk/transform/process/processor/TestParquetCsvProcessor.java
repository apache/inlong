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
import org.apache.inlong.sdk.transform.pojo.ParquetSourceInfo;
import org.apache.inlong.sdk.transform.pojo.TransformConfig;
import org.apache.inlong.sdk.transform.process.TransformProcessor;

import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.List;

public class TestParquetCsvProcessor extends AbstractProcessorTestBase {

    @Test
    public void testParquet2Csv() throws Exception {
        List<FieldInfo> fields = this.getTestFieldList("sid", "packageID", "msgTime", "msg");
        String parquetTestDescription = this.getParquetTestDescription();
        ParquetSourceInfo parquetSourceInfo =
                new ParquetSourceInfo("UTF-8", parquetTestDescription, "SdkDataRequest", "msgs");
        CsvSinkInfo csvSink = new CsvSinkInfo("UTF-8", '|', '\\', fields);
        String transformSql = "select $root.sid,$root.packageID,$child.msgTime,$child.msg from source";
        TransformConfig config = new TransformConfig(transformSql);
        // case1
        TransformProcessor<byte[], String> processor = TransformProcessor
                .create(config, SourceDecoderFactory.createParquetDecoder(parquetSourceInfo),
                        SinkEncoderFactory.createCsvEncoder(csvSink));
        byte[] srcBytes = this.getParquetTestData();
        List<String> output = processor.transform(srcBytes);
        Assert.assertEquals(2, output.size());
        Assert.assertEquals(output.get(0), "session_1|1001|1632999123|Hello World");
        Assert.assertEquals(output.get(1), "session_1|1001|1632999134|Bonjour");
    }

    @Test
    public void testParquet2CsvForOne() throws Exception {
        List<FieldInfo> fields = this.getTestFieldList("sid", "packageID", "msgTime", "msg");
        String parquetTestDescription = this.getParquetTestDescription();
        ParquetSourceInfo parquetSourceInfo =
                new ParquetSourceInfo("UTF-8", parquetTestDescription, "SdkDataRequest", null);
        CsvSinkInfo csvSink = new CsvSinkInfo("UTF-8", '|', '\\', fields);
        String transformSql = "select $root.sid,$root.packageID,$root.msgs(1).msgTime,$root.msgs(0).msg from source";
        TransformConfig config = new TransformConfig(transformSql);
        // case1
        TransformProcessor<byte[], String> processor = TransformProcessor
                .create(config, SourceDecoderFactory.createParquetDecoder(parquetSourceInfo),
                        SinkEncoderFactory.createCsvEncoder(csvSink));
        byte[] srcBytes = this.getParquetTestData();
        List<String> output = processor.transform(srcBytes, new HashMap<>());
        Assert.assertEquals(1, output.size());
        Assert.assertEquals(output.get(0), "session_1|1001|1632999134|Hello World");
    }

    @Test
    public void testParquet2CsvForAdd() throws Exception {
        List<FieldInfo> fields = this.getTestFieldList("sid", "packageID", "msgTime", "msg");
        String parquetTestDescription = this.getParquetTestDescription();
        ParquetSourceInfo parquetSourceInfo =
                new ParquetSourceInfo("UTF-8", parquetTestDescription, "SdkDataRequest", null);
        CsvSinkInfo csvSink = new CsvSinkInfo("UTF-8", '|', '\\', fields);

        String transformSql = "select $root.sid,"
                + "($root.msgs(1).msgTime-$root.msgs(0).msgTime+990)/$root.packageID field2,"
                + "$root.packageID*($root.msgs(0).msgTime*$root.packageID+$root.msgs(1).msgTime/($root.packageID - 1))"
                + "*$root.packageID field3,"
                + "$root.msgs(0).msg field4 from source "
                + "where $root.packageID<($root.msgs(0).msgTime+$root.msgs(1).msgTime"
                + "+$root.msgs(0).msgTime+$root.msgs(1).msgTime)";
        TransformConfig config = new TransformConfig(transformSql);
        // case1
        TransformProcessor<byte[], String> processor = TransformProcessor
                .create(config, SourceDecoderFactory.createParquetDecoder(parquetSourceInfo),
                        SinkEncoderFactory.createCsvEncoder(csvSink));
        byte[] srcBytes = this.getParquetTestData();
        List<String> output = processor.transform(srcBytes, new HashMap<>());
        Assert.assertEquals(1, output.size());
        Assert.assertEquals(output.get(0), "session_1|1|1637904657266133390.134|Hello World");
    }

    @Test
    public void testParquet2CsvForConcat() throws Exception {
        List<FieldInfo> fields = this.getTestFieldList("sid", "packageID", "msgTime", "msg");
        String parquetTestDescription = this.getParquetTestDescription();
        ParquetSourceInfo parquetSourceInfo =
                new ParquetSourceInfo("UTF-8", parquetTestDescription, "SdkDataRequest", "msgs");
        CsvSinkInfo csvSink = new CsvSinkInfo("UTF-8", '|', '\\', fields);
        String transformSql = "select $root.sid,$root.packageID,$child.msg Time,"
                + "concat($root.sid,$root.packageID,$child.msgTime,$child.msg) msg,$root.msgs.msgTime.msg from source";
        TransformConfig config = new TransformConfig(transformSql);
        // case1
        TransformProcessor<byte[], String> processor = TransformProcessor
                .create(config, SourceDecoderFactory.createParquetDecoder(parquetSourceInfo),
                        SinkEncoderFactory.createCsvEncoder(csvSink));
        byte[] srcBytes = this.getParquetTestData();
        List<String> output = processor.transform(srcBytes, new HashMap<>());
        Assert.assertTrue(output.size() == 2);
        Assert.assertEquals(output.get(0), "session_1|1001|Hello World|session_110011632999123Hello World");
        Assert.assertEquals(output.get(1), "session_1|1001|Bonjour|session_110011632999134Bonjour");
    }

    @Test
    public void testParquet2CsvForNow() throws Exception {
        List<FieldInfo> fields = this.getTestFieldList("sid", "packageID", "msgTime", "msg");
        String parquetTestDescription = this.getParquetTestDescription();
        ParquetSourceInfo parquetSourceInfo =
                new ParquetSourceInfo("UTF-8", parquetTestDescription, "SdkDataRequest", "msgs");
        CsvSinkInfo csvSink = new CsvSinkInfo("UTF-8", '|', '\\', fields);
        String transformSql = "select now() from source";
        TransformConfig config = new TransformConfig(transformSql);
        // case1
        TransformProcessor<byte[], String> processor = TransformProcessor
                .create(config, SourceDecoderFactory.createParquetDecoder(parquetSourceInfo),
                        SinkEncoderFactory.createCsvEncoder(csvSink));
        byte[] srcBytes = this.getParquetTestData();
        List<String> output = processor.transform(srcBytes, new HashMap<>());
        Assert.assertEquals(2, output.size());
    }
}
