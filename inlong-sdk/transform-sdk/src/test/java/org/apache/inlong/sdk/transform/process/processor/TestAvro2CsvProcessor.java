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
import org.apache.inlong.sdk.transform.pojo.AvroSourceInfo;
import org.apache.inlong.sdk.transform.pojo.CsvSinkInfo;
import org.apache.inlong.sdk.transform.pojo.FieldInfo;
import org.apache.inlong.sdk.transform.pojo.TransformConfig;
import org.apache.inlong.sdk.transform.process.TransformProcessor;

import org.junit.Assert;
import org.junit.Test;

import java.util.List;

public class TestAvro2CsvProcessor extends AbstractProcessorTestBase {

    @Test
    public void testAvro2Csv() throws Exception {
        List<FieldInfo> fields = this.getTestFieldList("sid", "packageID", "msgTime", "msg", "extinfo.k1");
        AvroSourceInfo avroSource = new AvroSourceInfo("UTF-8", "msgs");
        CsvSinkInfo csvSink = new CsvSinkInfo("UTF-8", '|', '\\', fields);
        String transformSql =
                "select $root.sid,$root.packageID,$child.msgTime,$child.msg, $child.extinfo.k1 from source";
        TransformConfig config = new TransformConfig(transformSql);
        // case1
        TransformProcessor<byte[], String> processor = TransformProcessor
                .create(config, SourceDecoderFactory.createAvroDecoder(avroSource),
                        SinkEncoderFactory.createCsvEncoder(csvSink));
        byte[] srcBytes = this.getAvroTestData();
        List<String> output = processor.transform(srcBytes);
        Assert.assertEquals(2, output.size());
        Assert.assertEquals(output.get(0), "sid1|123456|10011001|Apple|value1");
        Assert.assertEquals(output.get(1), "sid1|123456|20022002|Banana|value3");
    }
}
