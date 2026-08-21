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
import org.apache.inlong.sdk.transform.pojo.CsvSourceInfo;
import org.apache.inlong.sdk.transform.pojo.FieldInfo;
import org.apache.inlong.sdk.transform.pojo.RowDataSinkInfo;
import org.apache.inlong.sdk.transform.pojo.TransformConfig;
import org.apache.inlong.sdk.transform.process.TransformProcessor;

import org.apache.flink.table.data.RowData;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.List;

/**
 * TestCsv2RowDataProcessor
 * <p>
 * Verifies that CSV source data can be transformed into {@link RowData} via SQL that:
 * <ul>
 *   <li>selects a small set of source columns as-is;</li>
 *   <li>URL-decodes the {@code event_value} column and turns it into a KV map with
 *       {@code STR_TO_MAP(URL_DECODE(event_value), '&', '=')}, then extracts the
 *       {@code HY50} entry via bracket key access;</li>
 *   <li>filters rows with {@code HY50 = 'welfare_milestone_operations'} and
 *       {@code event_code = 'OnPageEnter'}.</li>
 * </ul>
 * The supplied test payload has {@code event_code=OnPageMod} and HY50 value
 * {@code ActivityPage}, so the WHERE clause filters the record out and the
 * sink produces zero rows.
 */
public class TestCsv2RowDataProcessor extends AbstractProcessorTestBase {
    @Test
    public void testCsv2RowDataHitHY50AndEventCode() throws Exception {
        List<FieldInfo> sourceFields = this.getTestFieldList(
                "ftime", "extinfo", "event_code", "event_value");
        List<FieldInfo> sinkFields = this.getTestFieldList(
                "ftime", "extinfo", "event_code", "HY50");

        CsvSourceInfo csvSource = new CsvSourceInfo("UTF-8", '|', '\\', sourceFields);
        RowDataSinkInfo rowSink = new RowDataSinkInfo("UTF-8", sinkFields);

        String transformSql = "select "
                + "ftime as ftime,"
                + "extinfo as extinfo,"
                + "event_code as event_code,"
                + "STR_TO_MAP(URL_DECODE(event_value), '&', '=')['HY50'] as HY50 "
                + "from source "
                + "WHERE  STR_TO_MAP(URL_DECODE(event_value), '&', '=')['HY50'] = 'welfare_milestone_operations' "
                + "AND event_code = 'OnPageEnter'";

        TransformProcessor<String, RowData> processor = TransformProcessor.create(
                new TransformConfig(transformSql),
                SourceDecoderFactory.createCsvDecoder(csvSource),
                SinkEncoderFactory.createRowEncoder(rowSink));

        // Positive sample: satisfies BOTH WHERE conditions.
        String testData = "2026-08-20 12:48:56.929"
                + "|extinfo=127.0.0.1"
                + "|OnPageEnter"
                + "|HY50%3Dwelfare_milestone_operations";

        List<RowData> output = processor.transform(testData, new HashMap<>());
        Assert.assertEquals(1, output.size());
        Assert.assertEquals("2026-08-20 12:48:56.929", output.get(0).getString(0).toString());
        Assert.assertEquals("extinfo=127.0.0.1",      output.get(0).getString(1).toString());
        Assert.assertEquals("OnPageEnter",            output.get(0).getString(2).toString());
        Assert.assertEquals("welfare_milestone_operations", output.get(0).getString(3).toString());
    }

    @Test
    public void testCsv2RowDataFilteredByHY50AndEventCode() throws Exception {
        // ==== source fields: 4 columns aligned with the pipe-delimited payload ====
        List<FieldInfo> sourceFields = this.getTestFieldList(
                "ftime", "extinfo", "event_code", "event_value");

        // ==== sink fields: 3 pass-through columns + extracted HY50 ====
        List<FieldInfo> sinkFields = this.getTestFieldList(
                "ftime", "extinfo", "event_code", "HY50");

        CsvSourceInfo csvSource = new CsvSourceInfo("UTF-8", '|', '\\', sourceFields);
        RowDataSinkInfo rowSink = new RowDataSinkInfo("UTF-8", sinkFields);

        String transformSql = "select "
                + "ftime as ftime,"
                + "extinfo as extinfo,"
                + "event_code as event_code,"
                + "STR_TO_MAP(URL_DECODE(event_value), '&', '=')['HY50'] as HY50 "
                + "from source "
                + "WHERE  STR_TO_MAP(URL_DECODE(event_value), '&', '=')['HY50'] = 'welfare_milestone_operations' "
                + "AND event_code = 'OnPageEnter'";

        TransformConfig config = new TransformConfig(transformSql);
        TransformProcessor<String, RowData> processor = TransformProcessor.create(
                config,
                SourceDecoderFactory.createCsvDecoder(csvSource),
                SinkEncoderFactory.createRowEncoder(rowSink));

        // Note: this record does NOT satisfy the WHERE clause:
        //  - event_code = 'OnPageMod'  (not 'OnPageEnter')
        //  - HY50 in the decoded map = 'ActivityPage' (not 'welfare_milestone_operations')
        // event_value only carries a single URL-encoded HY50 KV entry.
        String testData = "2026-08-20 12:48:56.929"
                + "|extinfo=127.0.0.1"
                + "|OnPageMod"
                + "|HY50%3DActivityPage";

        List<RowData> output = processor.transform(testData, new HashMap<>());

        // The record fails BOTH WHERE conditions, so nothing is emitted.
        Assert.assertEquals(0, output.size());
    }
}
