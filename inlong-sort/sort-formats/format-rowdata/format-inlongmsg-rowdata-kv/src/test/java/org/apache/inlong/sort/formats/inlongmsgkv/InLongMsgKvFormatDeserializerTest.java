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

package org.apache.inlong.sort.formats.inlongmsgkv;

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
import static org.apache.inlong.sort.formats.base.TableFormatConstants.DEFAULT_CHARSET;
import static org.apache.inlong.sort.formats.base.TableFormatConstants.DEFAULT_ENTRY_DELIMITER;
import static org.apache.inlong.sort.formats.base.TableFormatConstants.DEFAULT_KV_DELIMITER;
import static org.junit.Assert.assertEquals;

/**
 * Unit tests for {@link InLongMsgKvFormatDeserializer}.
 */
public class InLongMsgKvFormatDeserializerTest {

    private final FieldToRowDataConverters.FieldToRowDataConverter mapConvert =
            FieldToRowDataConverters.createConverter(MAP(STRING(), STRING()).getLogicalType());

    private static final RowFormatInfo TEST_ROW_INFO =
            new RowFormatInfo(
                    new String[]{"pf1", "pf2", "f1", "f2", "f3", "f4"},
                    new FormatInfo[]{
                            IntFormatInfo.INSTANCE,
                            IntFormatInfo.INSTANCE,
                            IntFormatInfo.INSTANCE,
                            StringFormatInfo.INSTANCE,
                            StringFormatInfo.INSTANCE,
                            StringFormatInfo.INSTANCE
                    });

    @Test
    public void testExceptionHandler() throws Exception {
        TestFailureHandler errorHandler = new TestFailureHandler();
        InLongMsgKvFormatDeserializer deserializer =
                new InLongMsgKvFormatDeserializer(
                        TEST_ROW_INFO,
                        "inlongmsg_time",
                        "inlongmsg_attributes",
                        DEFAULT_CHARSET,
                        DEFAULT_ENTRY_DELIMITER,
                        DEFAULT_KV_DELIMITER,
                        null,
                        null,
                        null,
                        null,
                        true,
                        errorHandler);

        InLongMsg inLongMsg = InLongMsg.newInLongMsg();
        String attrs = "m=0&iname=testInterfaceId&t=20200322&__addcol1__=1&__addcol2__=2";
        String body1 = "f1=123&f2=field11&f3=field12&f4=field13";
        String body2 = "f1=errormsg&f2=field21&f3=field22&f4=field23";
        inLongMsg.addMsg(attrs, body1.getBytes());
        inLongMsg.addMsg(attrs, body2.getBytes());

        List<RowData> actualRows = new ArrayList<>();
        Collector<RowData> collector = new ListCollector<>(actualRows);
        deserializer.flatMap(inLongMsg.buildArray(), collector);
        assertEquals(1, errorHandler.getRowCount());

        InLongMsg inLongMsg1 = InLongMsg.newInLongMsg();
        String abNormalAttrs = "m=0&iname=testInterfaceId&__addcol1__=1&__addcol2__=2";
        inLongMsg1.addMsg(abNormalAttrs, body1.getBytes());
        inLongMsg1.addMsg(abNormalAttrs, body2.getBytes());
        deserializer.flatMap(inLongMsg1.buildArray(), collector);
        assertEquals(1, errorHandler.getHeadCount());
    }

    @Test
    public void testNormal() throws Exception {

        InLongMsg inLongMsg = InLongMsg.newInLongMsg();
        String attrs = "m=0&iname=testInterfaceId&t=20200322&__addcol1__=1&__addcol2__=2";
        String body1 = "f1=123&f2=field11&f3=field12&f4=field13";
        String body2 = "f1=123&f2=field21&f3=field22&f4=field23";
        inLongMsg.addMsg(attrs, body1.getBytes());
        inLongMsg.addMsg(attrs, body2.getBytes());

        Map<String, String> expectedAttributes = new HashMap<>();
        expectedAttributes.put("m", "0");
        expectedAttributes.put("iname", "testInterfaceId");
        expectedAttributes.put("t", "20200322");
        expectedAttributes.put("__addcol1__", "1");
        expectedAttributes.put("__addcol2__", "2");

        GenericRowData expectRowData1 = new GenericRowData(8);
        expectRowData1.setField(0, TimestampData.fromTimestamp(Timestamp.valueOf("2020-03-22 00:00:00")));
        expectRowData1.setField(1, mapConvert.convert(expectedAttributes));
        expectRowData1.setField(2, 1);
        expectRowData1.setField(3, 2);
        expectRowData1.setField(4, 123);
        expectRowData1.setField(5, StringData.fromString("field11"));
        expectRowData1.setField(6, StringData.fromString("field12"));
        expectRowData1.setField(7, StringData.fromString("field13"));

        GenericRowData expectRowData2 = new GenericRowData(8);
        expectRowData2.setField(0, TimestampData.fromTimestamp(Timestamp.valueOf("2020-03-22 00:00:00")));
        expectRowData2.setField(1, mapConvert.convert(expectedAttributes));
        expectRowData2.setField(2, 1);
        expectRowData2.setField(3, 2);
        expectRowData2.setField(4, 123);
        expectRowData2.setField(5, StringData.fromString("field21"));
        expectRowData2.setField(6, StringData.fromString("field22"));
        expectRowData2.setField(7, StringData.fromString("field23"));

        InLongMsgKvFormatDeserializer deserializer =
                new InLongMsgKvFormatDeserializer.Builder(TEST_ROW_INFO)
                        .setTimeFieldName("inlongmsg_time")
                        .setAttributesFieldName("inlongmsg_attributes")
                        .build();
        testRowDeserialization(
                deserializer,
                inLongMsg.buildArray(),
                Arrays.asList(expectRowData1, expectRowData2));
    }

    @Test
    public void testNullField() throws Exception {

        InLongMsg inLongMsg = InLongMsg.newInLongMsg();
        String attrs = "m=0&iname=testInterfaceId&t=20200322&__addcol1__=1&__addcol2__=2";
        String body1 = "f1=123&f2=field11&f3=field12";
        String body2 = "f1=123&f2=field21&f4=field23&f5=field24&=";
        inLongMsg.addMsg(attrs, body1.getBytes());
        inLongMsg.addMsg(attrs, body2.getBytes());

        Map<String, String> expectedAttributes = new HashMap<>();
        expectedAttributes.put("m", "0");
        expectedAttributes.put("iname", "testInterfaceId");
        expectedAttributes.put("t", "20200322");
        expectedAttributes.put("__addcol1__", "1");
        expectedAttributes.put("__addcol2__", "2");

        GenericRowData expectRowData1 = new GenericRowData(8);
        expectRowData1.setField(0, TimestampData.fromTimestamp(Timestamp.valueOf("2020-03-22 00:00:00")));
        expectRowData1.setField(1, mapConvert.convert(expectedAttributes));
        expectRowData1.setField(2, 1);
        expectRowData1.setField(3, 2);
        expectRowData1.setField(4, 123);
        expectRowData1.setField(5, StringData.fromString("field11"));
        expectRowData1.setField(6, StringData.fromString("field12"));
        expectRowData1.setField(7, null);

        GenericRowData expectRowData2 = new GenericRowData(8);
        expectRowData2.setField(0, TimestampData.fromTimestamp(Timestamp.valueOf("2020-03-22 00:00:00")));
        expectRowData2.setField(1, mapConvert.convert(expectedAttributes));
        expectRowData2.setField(2, 1);
        expectRowData2.setField(3, 2);
        expectRowData2.setField(4, 123);
        expectRowData2.setField(5, StringData.fromString("field21"));
        expectRowData2.setField(6, null);
        expectRowData2.setField(7, StringData.fromString("field23"));

        InLongMsgKvFormatDeserializer deserializer =
                new InLongMsgKvFormatDeserializer.Builder(TEST_ROW_INFO)
                        .setTimeFieldName("inlongmsg_time")
                        .setAttributesFieldName("inlongmsg_attributes")
                        .build();
        testRowDeserialization(
                deserializer,
                inLongMsg.buildArray(),
                Arrays.asList(expectRowData1, expectRowData2));
    }

    @Test
    public void testNullField1() throws Exception {

        InLongMsg inLongMsg = InLongMsg.newInLongMsg();
        String attrs = "m=0&iname=testInterfaceId&t=20200322&__addcol1__=1&__addcol2__=2";
        String body1 = "f1=123&f2=field11&f3=field12";
        String body2 = "f1=123&f2=field21&f4=field23&f5=field24&=";
        inLongMsg.addMsg(attrs, body1.getBytes());
        inLongMsg.addMsg(attrs, body2.getBytes());

        Map<String, String> expectedAttributes = new HashMap<>();
        expectedAttributes.put("m", "0");
        expectedAttributes.put("iname", "testInterfaceId");
        expectedAttributes.put("t", "20200322");
        expectedAttributes.put("__addcol1__", "1");
        expectedAttributes.put("__addcol2__", "2");

        GenericRowData expectRowData1 = new GenericRowData(8);
        expectRowData1.setField(0, TimestampData.fromTimestamp(Timestamp.valueOf("2020-03-22 00:00:00")));
        expectRowData1.setField(1, mapConvert.convert(expectedAttributes));
        expectRowData1.setField(2, null);
        expectRowData1.setField(3, null);
        expectRowData1.setField(4, 123);
        expectRowData1.setField(5, StringData.fromString("field11"));
        expectRowData1.setField(6, StringData.fromString("field12"));
        expectRowData1.setField(7, null);

        GenericRowData expectRowData2 = new GenericRowData(8);
        expectRowData2.setField(0, TimestampData.fromTimestamp(Timestamp.valueOf("2020-03-22 00:00:00")));
        expectRowData2.setField(1, mapConvert.convert(expectedAttributes));
        expectRowData2.setField(2, null);
        expectRowData2.setField(3, null);
        expectRowData2.setField(4, 123);
        expectRowData2.setField(5, StringData.fromString("field21"));
        expectRowData2.setField(6, null);
        expectRowData2.setField(7, StringData.fromString("field23"));

        InLongMsgKvFormatDeserializer deserializer =
                new InLongMsgKvFormatDeserializer.Builder(TEST_ROW_INFO)
                        .setTimeFieldName("inlongmsg_time")
                        .setAttributesFieldName("inlongmsg_attributes")
                        .setRetainPredefinedField(false)
                        .build();
        testRowDeserialization(
                deserializer,
                inLongMsg.buildArray(),
                Arrays.asList(expectRowData1, expectRowData2));
    }

    @Test
    public void testLineDelimiter() throws Exception {
        InLongMsg inLongMsg = InLongMsg.newInLongMsg();
        String attrs = "m=0&iname=testInterfaceId&t=20200322&__addcol1__=1&__addcol2__=2";
        String body = "f1=123&f2=field11\nf1=1&f2=2";
        inLongMsg.addMsg(attrs, body.getBytes());

        Map<String, String> expectedAttributes = new HashMap<>();
        expectedAttributes.put("m", "0");
        expectedAttributes.put("iname", "testInterfaceId");
        expectedAttributes.put("t", "20200322");
        expectedAttributes.put("__addcol1__", "1");
        expectedAttributes.put("__addcol2__", "2");

        GenericRowData expectRowData1 = new GenericRowData(8);
        expectRowData1.setField(0, TimestampData.fromTimestamp(Timestamp.valueOf("2020-03-22 00:00:00")));
        expectRowData1.setField(1, mapConvert.convert(expectedAttributes));
        expectRowData1.setField(2, 1);
        expectRowData1.setField(3, 2);
        expectRowData1.setField(4, 123);
        expectRowData1.setField(5, StringData.fromString("field11"));
        expectRowData1.setField(6, null);
        expectRowData1.setField(7, null);

        GenericRowData expectRowData2 = new GenericRowData(8);
        expectRowData2.setField(0, TimestampData.fromTimestamp(Timestamp.valueOf("2020-03-22 00:00:00")));
        expectRowData2.setField(1, mapConvert.convert(expectedAttributes));
        expectRowData2.setField(2, 1);
        expectRowData2.setField(3, 2);
        expectRowData2.setField(4, 1);
        expectRowData2.setField(5, StringData.fromString("2"));
        expectRowData2.setField(6, null);
        expectRowData2.setField(7, null);

        InLongMsgKvFormatDeserializer deserializer =
                new InLongMsgKvFormatDeserializer.Builder(TEST_ROW_INFO)
                        .setTimeFieldName("inlongmsg_time")
                        .setAttributesFieldName("inlongmsg_attributes")
                        .setLineDelimiter('\n')
                        .build();

        testRowDeserialization(
                deserializer,
                inLongMsg.buildArray(),
                Arrays.asList(expectRowData1, expectRowData2));
    }

    private void testRowDeserialization(
            InLongMsgKvFormatDeserializer deserializer,
            byte[] bytes,
            List<RowData> expectedRows) throws Exception {
        List<RowData> actualRows = new ArrayList<>();
        Collector<RowData> collector = new ListCollector<>(actualRows);
        deserializer.flatMap(bytes, collector);
        assertEquals(expectedRows, actualRows);
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
