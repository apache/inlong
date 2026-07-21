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

import org.apache.inlong.common.pojo.sort.dataflow.field.format.LongFormatInfo;
import org.apache.inlong.common.pojo.sort.dataflow.field.format.TimestampFormatInfo;
import org.apache.inlong.sdk.transform.decode.SourceDecoderFactory;
import org.apache.inlong.sdk.transform.encode.SinkEncoderFactory;
import org.apache.inlong.sdk.transform.pojo.FieldInfo;
import org.apache.inlong.sdk.transform.pojo.KvSourceInfo;
import org.apache.inlong.sdk.transform.pojo.RowDataSinkInfo;
import org.apache.inlong.sdk.transform.pojo.TransformConfig;
import org.apache.inlong.sdk.transform.process.TransformProcessor;

import org.apache.flink.table.data.RowData;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.List;

public class TestKv2RowDataProcessor extends AbstractProcessorTestBase {

    @Test
    public void testKv2RowData() throws Exception {
        List<FieldInfo> sourceFields = this.getTestFieldList("audit_data_time", "uin", "uuid",
                "recotime", "abt", "ct",
                "cv", "from", "itemid",
                "songtime", "trace",
                "itemfeature", "userfeature",
                "extras", "pos");
        sourceFields.get(0).setFormatInfo(new LongFormatInfo());
        sourceFields.get(1).setFormatInfo(new LongFormatInfo());
        sourceFields.get(2).setFormatInfo(new LongFormatInfo());
        sourceFields.get(3).setFormatInfo(new LongFormatInfo());
        sourceFields.get(5).setFormatInfo(new LongFormatInfo());
        sourceFields.get(6).setFormatInfo(new LongFormatInfo());
        sourceFields.get(7).setFormatInfo(new LongFormatInfo());
        sourceFields.get(8).setFormatInfo(new LongFormatInfo());
        sourceFields.get(9).setFormatInfo(new LongFormatInfo());
        sourceFields.get(14).setFormatInfo(new LongFormatInfo());
        final KvSourceInfo kvSource = KvSourceInfo.builder().charset("UTF-8")
                .entryDelimiter('&')
                .kvDelimiter('=')
                .escapeChar('\\')
                .build();
        List<FieldInfo> sinkFields = this.getTestFieldList("audit_data_time",
                "tdbank_imp_date", "uin", "uuid",
                "recotime", "abt", "ct",
                "cv", "from", "itemid",
                "songtime", "trace",
                "itemfeature", "userfeature",
                "extras", "pos");
        sinkFields.get(0).setFormatInfo(new LongFormatInfo());
        sinkFields.get(1).setFormatInfo(new TimestampFormatInfo());
        sinkFields.get(2).setFormatInfo(new LongFormatInfo());
        sinkFields.get(3).setFormatInfo(new LongFormatInfo());
        sinkFields.get(4).setFormatInfo(new LongFormatInfo());
        sinkFields.get(6).setFormatInfo(new LongFormatInfo());
        sinkFields.get(7).setFormatInfo(new LongFormatInfo());
        sinkFields.get(8).setFormatInfo(new LongFormatInfo());
        sinkFields.get(9).setFormatInfo(new LongFormatInfo());
        sinkFields.get(10).setFormatInfo(new LongFormatInfo());
        sinkFields.get(15).setFormatInfo(new LongFormatInfo());
        // sql
        String transformSql =
                "select audit_data_time as audit_data_time,from_unix_time($ctx.msgTime/1000) as tdbank_imp_date,"
                + "uin as uin,uuid as uuid,recotime as recotime,abt as abt,ct as ct,cv as cv,`from` as `from`,"
                + "itemid as itemid,songtime as songtime,trace as trace,itemfeature as itemfeature,"
                + "userfeature as userfeature,extras as extras,pos as pos from source";
        // case1
        TransformProcessor<String, RowData> processor = TransformProcessor.create(
                new TransformConfig(transformSql),
                SourceDecoderFactory.createKvDecoder(kvSource),
                SinkEncoderFactory.createRowEncoder(new RowDataSinkInfo("UTF-8", sinkFields)));
        String strCsv =
                "pos=19&itemfeature=itemfeature1&uin=1370000000&userfeature=&itemid=620000000&uuid=5800000000"
                + "&recotime=1780000000&extras=recall&ct=1&abt=abt1&trace=trace1&cv=200600&from=2&songtime=209";
        HashMap<String, Object> extParams = new HashMap<>();
        extParams.put("msgTime", 1784030508000L);
        List<RowData> output = processor.transform(strCsv, extParams);
        Assert.assertEquals(1, output.size());
    }
}
