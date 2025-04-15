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

package org.apache.inlong.sort.formats.inlongmsgtlogkv;

import org.apache.inlong.common.msg.InLongMsg;
import org.apache.inlong.common.pojo.sort.dataflow.field.format.FormatInfo;
import org.apache.inlong.common.pojo.sort.dataflow.field.format.IntFormatInfo;
import org.apache.inlong.common.pojo.sort.dataflow.field.format.RowFormatInfo;
import org.apache.inlong.common.pojo.sort.dataflow.field.format.StringFormatInfo;
import org.apache.inlong.sort.formats.base.FieldToRowDataConverters;
import org.apache.inlong.sort.formats.inlongmsg.FailureHandler;
import org.apache.inlong.sort.formats.inlongmsg.InLongMsgBody;
import org.apache.inlong.sort.formats.inlongmsg.InLongMsgHead;

import org.apache.flink.api.common.functions.util.ListCollector;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.util.Collector;
import org.junit.Test;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.flink.table.api.DataTypes.MAP;
import static org.apache.flink.table.api.DataTypes.STRING;
import static org.apache.inlong.sort.formats.base.TableFormatConstants.DEFAULT_DELIMITER;
import static org.apache.inlong.sort.formats.base.TableFormatConstants.DEFAULT_ENTRY_DELIMITER;
import static org.apache.inlong.sort.formats.base.TableFormatConstants.DEFAULT_KV_DELIMITER;
import static org.apache.inlong.sort.formats.base.TableFormatConstants.DEFAULT_LINE_DELIMITER;
import static org.apache.inlong.sort.formats.inlongmsg.InLongMsgUtils.DEFAULT_ATTRIBUTES_FIELD_NAME;
import static org.apache.inlong.sort.formats.inlongmsg.InLongMsgUtils.DEFAULT_IS_DELETE_ESCAPE_CHAR;
import static org.apache.inlong.sort.formats.inlongmsg.InLongMsgUtils.DEFAULT_TIME_FIELD_NAME;
import static org.apache.inlong.sort.formats.inlongmsgtlogkv.InLongMsgTlogKvUtils.DEFAULT_INLONGMSG_TLOGKV_CHARSET;
import static org.junit.Assert.assertEquals;

/**
 * Tests for {@link InLongMsgTlogKvFormatDeserializer}.
 */
public class InLongMsgTlogKvFormatDeserializerTest {

    private final FieldToRowDataConverters.FieldToRowDataConverter mapConvert =
            FieldToRowDataConverters.createConverter(MAP(STRING(), STRING()).getLogicalType());

    private static final RowFormatInfo TEST_ROW_INFO =
            new RowFormatInfo(
                    new String[]{"pf1", "pf2", "f1", "f2", "f3"},
                    new FormatInfo[]{
                            IntFormatInfo.INSTANCE,
                            IntFormatInfo.INSTANCE,
                            StringFormatInfo.INSTANCE,
                            StringFormatInfo.INSTANCE,
                            StringFormatInfo.INSTANCE
                    });

    @Test
    public void testExceptionHandler() throws Exception {
        TestFailureHandler errorHandler = new TestFailureHandler();
        InLongMsgTlogKvFormatDeserializer deserializer =
                new InLongMsgTlogKvFormatDeserializer(
                        TEST_ROW_INFO,
                        DEFAULT_TIME_FIELD_NAME,
                        DEFAULT_ATTRIBUTES_FIELD_NAME,
                        DEFAULT_INLONGMSG_TLOGKV_CHARSET,
                        DEFAULT_DELIMITER,
                        DEFAULT_ENTRY_DELIMITER,
                        DEFAULT_KV_DELIMITER,
                        null,
                        null,
                        DEFAULT_LINE_DELIMITER,
                        null,
                        DEFAULT_IS_DELETE_ESCAPE_CHAR,
                        errorHandler);

        InLongMsg inLongMsg1 = InLongMsg.newInLongMsg(true);
        String attrs = "m=0&t=20200322&__addcol1_=1&__addcol2_=tes";
        String body1 = "testInterfaceId1,f1=field1&f2=field2&f3=field3";
        String body2 = "f1=field1&f2=field2&f3=field3";
        inLongMsg1.addMsg(attrs, body1.getBytes());
        inLongMsg1.addMsg(attrs, body2.getBytes());

        List<RowData> actualRowDatas = new ArrayList<>();
        Collector<RowData> collector = new ListCollector<>(actualRowDatas);
        deserializer.flatMap(inLongMsg1.buildArray(), collector);
        assertEquals(2, errorHandler.getRowCount());

        InLongMsg inLongMsg1Head = InLongMsg.newInLongMsg();
        String abNormalAttrs = "m=0&__addcol1_=1&__addcol2_=tes";
        inLongMsg1Head.addMsg(abNormalAttrs, body1.getBytes());
        inLongMsg1Head.addMsg(abNormalAttrs, body2.getBytes());
        deserializer.flatMap(inLongMsg1Head.buildArray(), collector);
        assertEquals(1, errorHandler.getHeadCount());
    }

    @Test
    public void testDeserialize() throws Exception {
        InLongMsg inLongMsg1 = InLongMsg.newInLongMsg(true);

        String attrs = "m=0&t=20200322&__addcol1_=1&__addcol2_=2";
        String body1 = "testInterfaceId1,f1=field11&f2=field12&f3=field13";
        String body2 = "f1=field21&f2=field22&f3=field23";
        String body3 = "f1=field3&f2=field2,f1=field31&f2=field32&f3=field33";
        String body4 = ",testInterfaceId1,f1=field41&f2=field42&f3=field43";

        inLongMsg1.addMsg(attrs, body1.getBytes());
        inLongMsg1.addMsg(attrs, body2.getBytes());
        inLongMsg1.addMsg(attrs, body3.getBytes());
        inLongMsg1.addMsg(attrs, body4.getBytes());

        Map<String, String> expectedAttributes = new HashMap<>();
        expectedAttributes.put("m", "0");
        expectedAttributes.put("t", "20200322");
        expectedAttributes.put("__addcol1_", "1");
        expectedAttributes.put("__addcol2_", "2");

        GenericRowData expectRowData1 = new GenericRowData(7);
        expectRowData1.setField(0, TimestampData.fromTimestamp(Timestamp.valueOf("2020-03-22 00:00:00")));
        expectRowData1.setField(1, mapConvert.convert(expectedAttributes));
        expectRowData1.setField(2, 1);
        expectRowData1.setField(3, 2);
        expectRowData1.setField(4, StringData.fromString("field11"));
        expectRowData1.setField(5, StringData.fromString("field12"));
        expectRowData1.setField(6, StringData.fromString("field13"));

        GenericRowData expectRowData2 = new GenericRowData(7);
        expectRowData2.setField(0, TimestampData.fromTimestamp(Timestamp.valueOf("2020-03-22 00:00:00")));
        expectRowData2.setField(1, mapConvert.convert(expectedAttributes));
        expectRowData2.setField(2, 1);
        expectRowData2.setField(3, 2);
        expectRowData2.setField(4, null);
        expectRowData2.setField(5, null);
        expectRowData2.setField(6, null);

        GenericRowData expectRowData3 = new GenericRowData(7);
        expectRowData3.setField(0, TimestampData.fromTimestamp(Timestamp.valueOf("2020-03-22 00:00:00")));
        expectRowData3.setField(1, mapConvert.convert(expectedAttributes));
        expectRowData3.setField(2, 1);
        expectRowData3.setField(3, 2);
        expectRowData3.setField(4, StringData.fromString("field31"));
        expectRowData3.setField(5, StringData.fromString("field32"));
        expectRowData3.setField(6, StringData.fromString("field33"));

        GenericRowData expectRowData4 = new GenericRowData(7);
        expectRowData4.setField(0, TimestampData.fromTimestamp(Timestamp.valueOf("2020-03-22 00:00:00")));
        expectRowData4.setField(1, mapConvert.convert(expectedAttributes));
        expectRowData4.setField(2, 1);
        expectRowData4.setField(3, 2);
        expectRowData4.setField(4, StringData.fromString("field41"));
        expectRowData4.setField(5, StringData.fromString("field42"));
        expectRowData4.setField(6, StringData.fromString("field43"));

        testRowDataDeserialization(inLongMsg1.buildArray(),
                Arrays.asList(expectRowData1, expectRowData2, expectRowData3, expectRowData4));
    }

    private void testRowDataDeserialization(
            byte[] bytes,
            List<RowData> expectedRows) throws Exception {
        InLongMsgTlogKvFormatDeserializer deserializer =
                new InLongMsgTlogKvFormatDeserializer.Builder(TEST_ROW_INFO)
                        .setTimeFieldName("inlongmsg_time")
                        .setAttributesFieldName("inlongmsg_attributes")
                        .build();

        List<RowData> actualRowDatas = new ArrayList<>();
        Collector<RowData> collector = new ListCollector<>(actualRowDatas);

        deserializer.flatMap(bytes, collector);

        assertEquals(expectedRows, actualRowDatas);
    }

    private static class TestFailureHandler implements FailureHandler {

        private int headCount = 0;
        private int bodyCount = 0;
        private int rowCount = 0;

        public int getHeadCount() {
            return headCount;
        }

        public int getBodyCount() {
            return bodyCount;
        }

        public int getRowCount() {
            return rowCount;
        }

        @Override
        public void onParsingHeadFailure(String attribute, Exception exception) throws Exception {
            headCount++;
        }

        @Override
        public void onParsingBodyFailure(InLongMsgHead head, byte[] body, Exception exception) throws Exception {
            bodyCount++;
        }

        @Override
        public void onConvertingRowFailure(InLongMsgHead head,
                InLongMsgBody body, Exception exception) throws Exception {
            rowCount++;
        }

        @Override
        public void onConvertingFieldFailure(String fieldName, String fieldText, FormatInfo formatInfo,
                Exception exception) throws Exception {
            rowCount++;
        }
    }
}
