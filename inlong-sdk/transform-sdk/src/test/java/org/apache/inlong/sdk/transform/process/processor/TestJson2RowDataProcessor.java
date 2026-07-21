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
import org.apache.inlong.sdk.transform.pojo.FieldInfo;
import org.apache.inlong.sdk.transform.pojo.JsonSourceInfo;
import org.apache.inlong.sdk.transform.pojo.RowDataSinkInfo;
import org.apache.inlong.sdk.transform.pojo.TransformConfig;
import org.apache.inlong.sdk.transform.process.TransformProcessor;

import org.apache.flink.table.data.RowData;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.List;

public class TestJson2RowDataProcessor extends AbstractProcessorTestBase {

    @Test
    public void testJson2RowData() throws Exception {
        List<FieldInfo> sinkFields = this.getTestFieldList("audit_data_time", "session_begin_time", "session_id",
                "business", "product_id", "channel",
                "agent_id", "archive_p1", "archive_p2",
                "archive_p3", "archive_p4", "array_field");
        // sql
        String transformSql = "select '' as audit_data_time,"
                + "$root.session_begin_time as session_begin_time,"
                + "$root.session_id as session_id,"
                + "$root.business as business,"
                + "$root.product_id as product_id,"
                + "$root.channel as channel,"
                + "$root.agent_id as agent_id,"
                + "$root.archive_p1 as archive_p1,"
                + "$root.archive_p2 as archive_p2,"
                + "$root.archive_p3 as archive_p3,"
                + "$root.archive_p4 as archive_p4,"
                + "$root.array_field as array_field from source";
        // case1
        TransformProcessor<String, RowData> processor = TransformProcessor.create(
                new TransformConfig(transformSql),
                SourceDecoderFactory.createJsonDecoder(new JsonSourceInfo("UTF-8", null)),
                SinkEncoderFactory.createRowEncoder(new RowDataSinkInfo("UTF-8", sinkFields)));
        String strJson =
                "{\"session_id\":\"1782780884\",\"session_begin_time\":\"2026-06-30 08:54:56\",\"business\":\"pay\","
                        + "\"product_id\":\"1314\",\"channel\":\"todo\",\"agent_id\":\"095d2\",\"archive_p1\":\"money\","
                        + "\"archive_p2\":\"product\",\"archive_p3\":\"short”\",\"archive_p4\":\"\",\"array_field\":[{\"isArray\":true}]}";
        List<RowData> output = processor.transform(strJson, new HashMap<>());
        Assert.assertEquals(1, output.size());
        Assert.assertEquals(output.get(0).getString(9).toString(), "short”");
        Assert.assertEquals(output.get(0).getString(11).toString(), "[{\"isArray\":true}]");
    }
}
