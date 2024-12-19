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
import org.apache.inlong.sdk.transform.pojo.RowDataSinkInfo;
import org.apache.inlong.sdk.transform.pojo.RowDataSourceInfo;
import org.apache.inlong.sdk.transform.pojo.TransformConfig;
import org.apache.inlong.sdk.transform.process.TransformProcessor;

import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

public class TestRowData2RowDataProcessor extends AbstractProcessorTestBase {

    @Test
    public void testRowData2RowData() throws Exception {
        List<FieldInfo> fields1 = this.getTestFieldList("sid", "packageID", "msgTime", "msg");
        RowDataSourceInfo sourceInfo = new RowDataSourceInfo("utf-8", fields1);
        List<FieldInfo> fields2 = this.getTestFieldList("f1", "f2", "f3", "f4");
        RowDataSinkInfo sinkInfo = new RowDataSinkInfo("utf-8", fields2);

        String transformSql = "select msgTime ,msg, packageID, sid";
        TransformConfig config = new TransformConfig(transformSql);
        TransformProcessor<RowData, RowData> processor =
                TransformProcessor.create(
                        config,
                        SourceDecoderFactory.createRowDecoder(sourceInfo),
                        SinkEncoderFactory.createRowEncoder(sinkInfo));

        RowData sourceRow = createRowData();

        List<RowData> sinkRow = processor.transform(sourceRow);
        RowData expectedRow = sinkRow.get(0);
        Assert.assertEquals("2024-12-19T11:00:55.212", expectedRow.getString(0).toString());
        Assert.assertEquals("msg111", expectedRow.getString(1).toString());
        Assert.assertEquals("pack123", expectedRow.getString(2).toString());
        Assert.assertEquals("s123", expectedRow.getString(3).toString());

    }

    private RowData createRowData() {
        GenericRowData rowData = new GenericRowData(4);
        rowData.setField(0, StringData.fromString("s123"));
        rowData.setField(1, StringData.fromString("pack123"));
        rowData.setField(2, StringData.fromString("2024-12-19T11:00:55.212"));
        rowData.setField(3, StringData.fromString("msg111"));
        return rowData;
    }
}
