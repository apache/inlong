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
import org.apache.inlong.sdk.transform.pojo.BsonSourceInfo;
import org.apache.inlong.sdk.transform.pojo.CsvSinkInfo;
import org.apache.inlong.sdk.transform.pojo.FieldInfo;
import org.apache.inlong.sdk.transform.pojo.TransformConfig;
import org.apache.inlong.sdk.transform.process.TransformProcessor;

import org.junit.Assert;
import org.junit.Test;

import java.util.Base64;
import java.util.HashMap;
import java.util.List;

public class TestBson2CsvProcessor extends AbstractProcessorTestBase {

    @Test
    public void testBson2Csv() throws Exception {
        List<FieldInfo> fields1 = this.getTestFieldList("sid", "packageID", "msgTime", "msg");
        BsonSourceInfo bsonSourceInfo1 = new BsonSourceInfo("UTF-8", "msgs");
        CsvSinkInfo csvSink1 = new CsvSinkInfo("UTF-8", '|', '\\', fields1);
        String transformSql1 = "select $root.sid,$root.packageID,$child.msgTime,$child.msg from source";
        TransformConfig config1 = new TransformConfig(transformSql1);
        // case1
        TransformProcessor<byte[], String> processor1 = TransformProcessor
                .create(config1, SourceDecoderFactory.createBsonDecoder(bsonSourceInfo1),
                        SinkEncoderFactory.createCsvEncoder(csvSink1));
        String srcBase64String1 = "lQAAAAdfaWQAZugcR0pfeo8607hFAnNpZAAHAAAAdmFsdWUxAAJ" +
                "wYWNrYWdlSUQABwAAAHZhbHVlMgAEbXNncwBTAAAAAzAAJgAAAAJtc2cABwAAAHZhbHVl" +
                "NAABbXNnVGltZQAAAOu4VO54QgADMQAiAAAAAm1zZwADAAAAdjQAAW1zZ1RpbWUAAADru" +
                "FTueEIAAAA=";
        byte[] srcByte1 = Base64.getDecoder().decode(srcBase64String1);
        List<String> output1 = processor1.transform(srcByte1, new HashMap<>());
        Assert.assertEquals(2, output1.size());
        Assert.assertEquals("value1|value2|1713243918000|value4", output1.get(0));
        Assert.assertEquals("value1|value2|1713243918000|v4", output1.get(1));

        // case2
        List<FieldInfo> fields2 = this.getTestFieldList("id", "itemId", "subItemId", "msg");
        BsonSourceInfo bsonSourceInfo2 = new BsonSourceInfo("UTF-8", "items");
        CsvSinkInfo csvSink2 = new CsvSinkInfo("UTF-8", '|', '\\', fields2);
        String transformSql2 =
                "select $root.id,$child.itemId,$child.subItems(0).subItemId,$child.subItems(1).msg from source";
        TransformConfig config2 = new TransformConfig(transformSql2);
        TransformProcessor<byte[], String> processor2 = TransformProcessor
                .create(config2, SourceDecoderFactory.createBsonDecoder(bsonSourceInfo2),
                        SinkEncoderFactory.createCsvEncoder(csvSink2));
        String srcBase64String2 = "SAEAAAdfaWQAZueeaQg0im27chcAAmlkAAcAAAB2YWx1ZTEAAm5hb" +
                "WUABwAAAHZhbHVlMgAEaXRlbXMACwEAAAMwAIAAAAACaXRlbUlkAAYAAABpdGVtMQAEc" +
                "3ViSXRlbXMAXwAAAAMwACoAAAACc3ViSXRlbUlkAAUAAAAxMDAxAAJtc2cACAAAADEwM" +
                "DFtc2cAAAMxACoAAAACc3ViSXRlbUlkAAUAAAAxMDAyAAJtc2cACAAAADEwMDJtc2cAA" +
                "AAAAzEAgAAAAAJpdGVtSWQABgAAAGl0ZW0yAARzdWJJdGVtcwBfAAAAAzAAKgAAAAJzd" +
                "WJJdGVtSWQABQAAADIwMDEAAm1zZwAIAAAAMjAwMW1zZwAAAzEAKgAAAAJzdWJJdGVtS" +
                "WQABQAAADIwMDIAAm1zZwAIAAAAMjAwMm1zZwAAAAAAAA==";
        byte[] srcByte2 = Base64.getDecoder().decode(srcBase64String2);
        List<String> output2 = processor2.transform(srcByte2, new HashMap<>());
        Assert.assertEquals(2, output2.size());
        Assert.assertEquals("value1|item1|1001|1002msg", output2.get(0));
        Assert.assertEquals("value1|item2|2001|2002msg", output2.get(1));

        // case3
        List<FieldInfo> fields3 = this.getTestFieldList("matrix(0,0)", "matrix(1,1)", "matrix(2,2)");
        BsonSourceInfo bsonSourceInfo3 = new BsonSourceInfo("UTF-8", "");
        CsvSinkInfo csvSink3 = new CsvSinkInfo("UTF-8", '|', '\\', fields3);
        String transformSql3 = "select $root.matrix(0, 0), $root.matrix(1, 1), $root.matrix(2, 2) from source";
        TransformConfig config3 = new TransformConfig(transformSql3);
        TransformProcessor<byte[], String> processor3 = TransformProcessor
                .create(config3, SourceDecoderFactory.createBsonDecoder(bsonSourceInfo3),
                        SinkEncoderFactory.createCsvEncoder(csvSink3));
        String srcBase64String3 = "ngAAAAdfaWQAZugj40pfeo8607hGBG1hdHJpeACAAAAABDAAJgAAAA" +
                "EwAAAAAAAAAPA/ATEAAAAAAAAAAEABMgAAAAAAAAAIQAAEMQAmAAAAATAAAAAAAAAAEEABMQ" +
                "AAAAAAAAAUQAEyAAAAAAAAABhAAAQyACYAAAABMAAAAAAAAAAcQAExAAAAAAAAACBAATIAAA" +
                "AAAAAAIkAAAAA=";
        byte[] srcByte3 = Base64.getDecoder().decode(srcBase64String3);
        List<String> output3 = processor3.transform(srcByte3, new HashMap<>());
        Assert.assertEquals(1, output3.size());
        Assert.assertEquals("1|5|9", output3.get(0));

        // case 4
        List<FieldInfo> fields4 = this.getTestFieldList("department_name", "course_id", "num");
        BsonSourceInfo bsonSourceInfo4 = new BsonSourceInfo("UTF-8", "");
        CsvSinkInfo csvSink4 = new CsvSinkInfo("UTF-8", '|', '\\', fields4);
        String transformSql4 =
                "select $root.departments(0).name, $root.departments(0).courses(0,1).courseId, sqrt($root.departments(0).courses(0,1).courseId - 2) from source";
        TransformConfig config4 = new TransformConfig(transformSql4);
        TransformProcessor<byte[], String> processor4 = TransformProcessor
                .create(config4, SourceDecoderFactory.createBsonDecoder(bsonSourceInfo4),
                        SinkEncoderFactory.createCsvEncoder(csvSink4));
        String srcBase64String4 = "KwEAAAdfaWQAZugoCEpfeo8607hHBGRlcGFydG1lbnRzAAgBAAADMAA" +
                "AAQAAAm5hbWUADAAAAE1hdGhlbWF0aWNzAARjb3Vyc2VzANwAAAAEMABnAAAAAzAALAAAAAJj" +
                "b3Vyc2VJZAAEAAAAMTAxAAJ0aXRsZQAKAAAAQ2FsY3VsdXNJAAADMQAwAAAAAmNvdXJzZUlkA" +
                "AQAAAAxMDIAAnRpdGxlAA4AAABMaW5lYXJBbGdlYnJhAAAABDEAagAAAAMwAC0AAAACY291cn" +
                "NlSWQABAAAADIwMQACdGl0bGUACwAAAENhbGN1bHVzSUkAAAMxADIAAAACY291cnNlSWQABAA" +
                "AADIwMgACdGl0bGUAEAAAAEFic3RyYWN0QWxnZWJyYQAAAAAAAAA=";
        byte[] srcByte4 = Base64.getDecoder().decode(srcBase64String4);
        List<String> output4 = processor4.transform(srcByte4, new HashMap<>());
        Assert.assertEquals(1, output4.size());
        Assert.assertEquals(output4.get(0), "Mathematics|102|10.0");
    }

    @Test
    public void testBson2CsvForOne() throws Exception {
        List<FieldInfo> fields = this.getTestFieldList("sid", "packageID", "msgTime", "msg");
        BsonSourceInfo bsonSourceInfo = new BsonSourceInfo("UTF-8", "");
        CsvSinkInfo csvSink = new CsvSinkInfo("UTF-8", '|', '\\', fields);
        String transformSql = "select $root.sid,$root.packageID,$root.msgs(1).msgTime,$root.msgs(0).msg from source";
        TransformConfig config = new TransformConfig(transformSql);
        // case1
        TransformProcessor<byte[], String> processor = TransformProcessor
                .create(config, SourceDecoderFactory.createBsonDecoder(bsonSourceInfo),
                        SinkEncoderFactory.createCsvEncoder(csvSink));
        String srcBase64String = "lQAAAAdfaWQAZugcR0pfeo8607hFAnNpZAAHAAAAdmFsdWUxAAJ" +
                "wYWNrYWdlSUQABwAAAHZhbHVlMgAEbXNncwBTAAAAAzAAJgAAAAJtc2cABwAAAHZhbHVl" +
                "NAABbXNnVGltZQAAAOu4VO54QgADMQAiAAAAAm1zZwADAAAAdjQAAW1zZ1RpbWUAAADru" +
                "FTueEIAAAA=";
        byte[] srcByte = Base64.getDecoder().decode(srcBase64String);
        List<String> output = processor.transform(srcByte, new HashMap<>());
        Assert.assertEquals(1, output.size());
        Assert.assertEquals(output.get(0), "value1|value2|1713243918000|value4");
    }
}
