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

package org.apache.inlong.sort.formats.inlongmsgcsv;

import org.apache.inlong.common.msg.InLongMsg;
import org.apache.inlong.common.pojo.sort.dataflow.field.format.FormatInfo;
import org.apache.inlong.common.pojo.sort.dataflow.field.format.IntFormatInfo;
import org.apache.inlong.common.pojo.sort.dataflow.field.format.RowFormatInfo;
import org.apache.inlong.common.pojo.sort.dataflow.field.format.StringFormatInfo;
import org.apache.inlong.sort.formats.inlongmsg.FailureHandler;
import org.apache.inlong.sort.formats.inlongmsg.InLongMsgBody;
import org.apache.inlong.sort.formats.inlongmsg.InLongMsgHead;

import org.apache.flink.api.common.functions.util.ListCollector;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.junit.Test;

import java.nio.charset.Charset;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.inlong.sort.formats.base.TableFormatConstants.DEFAULT_CHARSET;
import static org.apache.inlong.sort.formats.base.TableFormatConstants.DEFAULT_DELIMITER;
import static org.apache.inlong.sort.formats.inlongmsg.row.InLongMsgUtils.DEFAULT_ATTRIBUTES_FIELD_NAME;
import static org.apache.inlong.sort.formats.inlongmsg.row.InLongMsgUtils.DEFAULT_TIME_FIELD_NAME;
import static org.apache.inlong.sort.formats.inlongmsgcsv.InLongMsgCsvUtils.DEFAULT_DELETE_HEAD_DELIMITER;
import static org.junit.Assert.assertEquals;

/**
 * Unit tests for {@link InLongMsgCsvFormatDeserializer}.
 */
public class InLongMsgCsvFormatDeserializerTest {

    private static final RowFormatInfo TEST_ROW_INFO =
            new RowFormatInfo(
                    new String[]{"f1", "f2", "f3", "f4", "f5", "f6"},
                    new FormatInfo[]{
                            IntFormatInfo.INSTANCE,
                            IntFormatInfo.INSTANCE,
                            IntFormatInfo.INSTANCE,
                            StringFormatInfo.INSTANCE,
                            StringFormatInfo.INSTANCE,
                            StringFormatInfo.INSTANCE
                    });

    @Test
    public void testRowType() {
        InLongMsgCsvFormatDeserializer deserializer =
                new InLongMsgCsvFormatDeserializer.Builder(TEST_ROW_INFO).build();

        TypeInformation<Row> expectedRowType =
                Types.ROW_NAMED(
                        new String[]{
                                DEFAULT_TIME_FIELD_NAME,
                                DEFAULT_ATTRIBUTES_FIELD_NAME,
                                "f1",
                                "f2",
                                "f3",
                                "f4",
                                "f5",
                                "f6"
                        },
                        Types.SQL_TIMESTAMP,
                        Types.MAP(Types.STRING, Types.STRING),
                        Types.INT,
                        Types.INT,
                        Types.INT,
                        Types.STRING,
                        Types.STRING,
                        Types.STRING);

        assertEquals(expectedRowType, deserializer.getProducedType());
    }

    @Test
    public void testRowTypeWithHeadFields() {
        InLongMsgCsvFormatDeserializer deserializer =
                new InLongMsgCsvFormatDeserializer.Builder(TEST_ROW_INFO)
                        .setTimeFieldName("inlongmsg_time")
                        .setAttributesFieldName("inlongmsg_attributes")
                        .build();

        TypeInformation<Row> expectedRowType =
                Types.ROW_NAMED(
                        new String[]{
                                "inlongmsg_time",
                                "inlongmsg_attributes",
                                "f1",
                                "f2",
                                "f3",
                                "f4",
                                "f5",
                                "f6"
                        },
                        Types.SQL_TIMESTAMP,
                        Types.MAP(Types.STRING, Types.STRING),
                        Types.INT,
                        Types.INT,
                        Types.INT,
                        Types.STRING,
                        Types.STRING,
                        Types.STRING);

        assertEquals(expectedRowType, deserializer.getProducedType());
    }

    @Test
    public void testNormal() throws Exception {

        InLongMsgCsvFormatDeserializer deserializer =
                new InLongMsgCsvFormatDeserializer.Builder(TEST_ROW_INFO)
                        .setTimeFieldName("inlongmsg_time")
                        .setAttributesFieldName("inlongmsg_attributes")
                        .build();

        InLongMsg inLongMsg = InLongMsg.newInLongMsg();
        String attrs = "m=0&streamId=testInterfaceId&t=20200322&__addcol1__=1&__addcol2__=2";
        String body1 = "123,field11,field12,field13";
        String body2 = "123,field21,field22,field23";
        inLongMsg.addMsg(attrs, body1.getBytes());
        inLongMsg.addMsg(attrs, body2.getBytes());

        Map<String, String> expectedAttributes = new HashMap<>();
        expectedAttributes.put("m", "0");
        expectedAttributes.put("streamId", "testInterfaceId");
        expectedAttributes.put("t", "20200322");
        expectedAttributes.put("__addcol1__", "1");
        expectedAttributes.put("__addcol2__", "2");

        Row expectedRow1 = Row.of(
                Timestamp.valueOf("2020-03-22 00:00:00"),
                expectedAttributes,
                1,
                2,
                123,
                "field11",
                "field12",
                "field13");

        Row expectedRow2 = Row.of(
                Timestamp.valueOf("2020-03-22 00:00:00"),
                expectedAttributes,
                1,
                2,
                123,
                "field21",
                "field22",
                "field23");

        testRowDeserialization(
                deserializer,
                inLongMsg.buildArray(),
                Arrays.asList(expectedRow1, expectedRow2));
    }

    @Test
    public void testExceptionHandler() throws Exception {
        TestFailureHandler errorHandler = new TestFailureHandler();
        InLongMsgCsvFormatDeserializer deserializer =
                new InLongMsgCsvFormatDeserializer(
                        TEST_ROW_INFO,
                        "inlongmsg_time",
                        "inlongmsg_attributes",
                        DEFAULT_CHARSET,
                        DEFAULT_DELIMITER,
                        null,
                        null,
                        null,
                        null,
                        DEFAULT_DELETE_HEAD_DELIMITER,
                        errorHandler);

        InLongMsg inLongMsg = InLongMsg.newInLongMsg();
        String attrs = "m=0&streamId=testInterfaceId&t=20200322&__addcol1__=1&__addcol2__=2";
        String body1 = "test,field11,field12,field13";
        String body2 = "123,field21,field22,field23";
        inLongMsg.addMsg(attrs, body1.getBytes());
        inLongMsg.addMsg(attrs, body2.getBytes());

        List<Row> actualRows = new ArrayList<>();
        Collector<Row> collector = new ListCollector<>(actualRows);
        deserializer.flatMap(inLongMsg.buildArray(), collector);
        assertEquals(0, errorHandler.getRowCount());

        InLongMsg inLongMsg2 = InLongMsg.newInLongMsg();
        String abNormalAttrs = "m=0&streamId=testInterfaceId&__addcol1__=1&__addcol2__=2";
        inLongMsg2.addMsg(abNormalAttrs, body1.getBytes());
        inLongMsg2.addMsg(abNormalAttrs, body2.getBytes());
        deserializer.flatMap(inLongMsg2.buildArray(), collector);
        assertEquals(1, errorHandler.getHeadCount());
    }

    @Test
    public void testEmptyField() throws Exception {

        InLongMsgCsvFormatDeserializer deserializer =
                new InLongMsgCsvFormatDeserializer.Builder(TEST_ROW_INFO)
                        .setTimeFieldName("inlongmsg_time")
                        .setAttributesFieldName("inlongmsg_attributes")
                        .build();

        InLongMsg inLongMsg1 = InLongMsg.newInLongMsg();
        String attrs = "m=0&streamId=testInterfaceId&t=20200322&__addcol1__=1&__addcol2__=2";
        String body1 = "123,field11,field12,";
        String body2 = "123,field21,,field23";
        inLongMsg1.addMsg(attrs, body1.getBytes());
        inLongMsg1.addMsg(attrs, body2.getBytes());

        Map<String, String> expectedAttributes = new HashMap<>();
        expectedAttributes.put("m", "0");
        expectedAttributes.put("streamId", "testInterfaceId");
        expectedAttributes.put("t", "20200322");
        expectedAttributes.put("__addcol1__", "1");
        expectedAttributes.put("__addcol2__", "2");

        Row expectedRow1 = Row.of(
                Timestamp.valueOf("2020-03-22 00:00:00"),
                expectedAttributes,
                1,
                2,
                123,
                "field11",
                "field12",
                "");

        Row expectedRow2 = Row.of(
                Timestamp.valueOf("2020-03-22 00:00:00"),
                expectedAttributes,
                1,
                2,
                123,
                "field21",
                "",
                "field23");

        testRowDeserialization(
                deserializer,
                inLongMsg1.buildArray(),
                Arrays.asList(expectedRow1, expectedRow2));
    }

    @Test
    public void testNoPredefinedFields() throws Exception {

        InLongMsgCsvFormatDeserializer deserializer =
                new InLongMsgCsvFormatDeserializer.Builder(TEST_ROW_INFO)
                        .setTimeFieldName("inlongmsg_time")
                        .setAttributesFieldName("inlongmsg_attributes")
                        .build();

        InLongMsg inLongMsg1 = InLongMsg.newInLongMsg();
        String attrs = "m=0&streamId=testInterfaceId&t=20200322";
        String body1 = "1,2,123,field11,field12,";
        String body2 = "1,2,123,field21,,field23";
        inLongMsg1.addMsg(attrs, body1.getBytes());
        inLongMsg1.addMsg(attrs, body2.getBytes());

        Map<String, String> expectedAttributes = new HashMap<>();
        expectedAttributes.put("m", "0");
        expectedAttributes.put("streamId", "testInterfaceId");
        expectedAttributes.put("t", "20200322");

        Row expectedRow1 = Row.of(
                Timestamp.valueOf("2020-03-22 00:00:00"),
                expectedAttributes,
                1,
                2,
                123,
                "field11",
                "field12",
                "");

        Row expectedRow2 = Row.of(
                Timestamp.valueOf("2020-03-22 00:00:00"),
                expectedAttributes,
                1,
                2,
                123,
                "field21",
                "",
                "field23");

        testRowDeserialization(
                deserializer,
                inLongMsg1.buildArray(),
                Arrays.asList(expectedRow1, expectedRow2));
    }

    @Test
    public void testPredefinedFieldWithFlagOn() throws Exception {

        InLongMsgCsvFormatDeserializer deserializer =
                new InLongMsgCsvFormatDeserializer.Builder(TEST_ROW_INFO)
                        .setTimeFieldName("inlongmsg_time")
                        .setAttributesFieldName("inlongmsg_attributes")
                        .setRetainPredefinedField(true)
                        .build();

        InLongMsg inLongMsg1 = InLongMsg.newInLongMsg();
        String attrs = "m=0&streamId=testInterfaceId&t=20200322" +
                "&__addcol1__repdate=20220224&__addcol2__hour=1517";
        String body1 = "123,field11,field12,";
        String body2 = "123,field21,,field23";
        inLongMsg1.addMsg(attrs, body1.getBytes());
        inLongMsg1.addMsg(attrs, body2.getBytes());

        Map<String, String> expectedAttributes = new HashMap<>();
        expectedAttributes.put("m", "0");
        expectedAttributes.put("streamId", "testInterfaceId");
        expectedAttributes.put("t", "20200322");
        expectedAttributes.put("__addcol1__repdate", "20220224");
        expectedAttributes.put("__addcol2__hour", "1517");

        Row expectedRow1 = Row.of(
                Timestamp.valueOf("2020-03-22 00:00:00"),
                expectedAttributes,
                20220224,
                1517,
                123,
                "field11",
                "field12",
                "");

        Row expectedRow2 = Row.of(
                Timestamp.valueOf("2020-03-22 00:00:00"),
                expectedAttributes,
                20220224,
                1517,
                123,
                "field21",
                "",
                "field23");

        testRowDeserialization(
                deserializer,
                inLongMsg1.buildArray(),
                Arrays.asList(expectedRow1, expectedRow2));
    }

    @Test
    public void testPredefinedFieldWithFlagOff() throws Exception {

        InLongMsgCsvFormatDeserializer deserializer =
                new InLongMsgCsvFormatDeserializer.Builder(TEST_ROW_INFO)
                        .setTimeFieldName("inlongmsg_time")
                        .setAttributesFieldName("inlongmsg_attributes")
                        .setRetainPredefinedField(false)
                        .build();

        InLongMsg inLongMsg1 = InLongMsg.newInLongMsg();
        String attrs = "m=0&streamId=testInterfaceId&t=20200322" +
                "&__addcol1__repdate=20220224&__addcol2__hour=1517";
        String body1 = "1,2,123,field11,field12,";
        String body2 = "1,2,123,field21,,field23";
        inLongMsg1.addMsg(attrs, body1.getBytes());
        inLongMsg1.addMsg(attrs, body2.getBytes());

        Map<String, String> expectedAttributes = new HashMap<>();
        expectedAttributes.put("m", "0");
        expectedAttributes.put("streamId", "testInterfaceId");
        expectedAttributes.put("t", "20200322");
        expectedAttributes.put("__addcol1__repdate", "20220224");
        expectedAttributes.put("__addcol2__hour", "1517");

        Row expectedRow1 = Row.of(
                Timestamp.valueOf("2020-03-22 00:00:00"),
                expectedAttributes,
                1,
                2,
                123,
                "field11",
                "field12",
                "");

        Row expectedRow2 = Row.of(
                Timestamp.valueOf("2020-03-22 00:00:00"),
                expectedAttributes,
                1,
                2,
                123,
                "field21",
                "",
                "field23");

        testRowDeserialization(
                deserializer,
                inLongMsg1.buildArray(),
                Arrays.asList(expectedRow1, expectedRow2));
    }

    @Test
    public void testIgnoreAttributeErrors() throws Exception {
        InLongMsgCsvFormatDeserializer deserializer =
                new InLongMsgCsvFormatDeserializer(
                        TEST_ROW_INFO,
                        DEFAULT_TIME_FIELD_NAME,
                        DEFAULT_ATTRIBUTES_FIELD_NAME,
                        Charset.defaultCharset().name(),
                        DEFAULT_DELIMITER,
                        null,
                        null,
                        null,
                        null,
                        true,
                        true);

        InLongMsg inLongMsg1 = InLongMsg.newInLongMsg();
        String attrs = "m=0&&&&";
        String body1 = "123,field11,field12,field13";
        inLongMsg1.addMsg(attrs, body1.getBytes());

        testRowDeserialization(
                deserializer,
                inLongMsg1.buildArray(),
                Collections.emptyList());
    }

    @Test
    public void testIgnoreBodyErrors() throws Exception {
        InLongMsgCsvFormatDeserializer deserializer =
                new InLongMsgCsvFormatDeserializer.Builder(TEST_ROW_INFO)
                        .setTimeFieldName("inlongmsg_time")
                        .setAttributesFieldName("inlongmsg_attributes")
                        .setIgnoreErrors(true)
                        .build();

        InLongMsg inLongMsg1 = InLongMsg.newInLongMsg();
        String attrs = "m=0&streamId=testInterfaceId&t=20200322&__addcol1__=1&__addcol2__=2";
        String body1 = "aaa,field11,field12,field13";
        String body2 = "123,field21,field22,field23";
        inLongMsg1.addMsg(attrs, body1.getBytes());
        inLongMsg1.addMsg(attrs, body2.getBytes());

        Map<String, String> expectedAttributes = new HashMap<>();
        expectedAttributes.put("m", "0");
        expectedAttributes.put("streamId", "testInterfaceId");
        expectedAttributes.put("t", "20200322");
        expectedAttributes.put("__addcol1__", "1");
        expectedAttributes.put("__addcol2__", "2");

        Row expectedRow1 = Row.of(
                Timestamp.valueOf("2020-03-22 00:00:00"),
                expectedAttributes,
                1,
                2,
                null,
                "field11",
                "field12",
                "field13");

        Row expectedRow2 = Row.of(
                Timestamp.valueOf("2020-03-22 00:00:00"),
                expectedAttributes,
                1,
                2,
                123,
                "field21",
                "field22",
                "field23");

        List list = new ArrayList();
        list.add(expectedRow1);
        list.add(expectedRow2);
        testRowDeserialization(deserializer, inLongMsg1.buildArray(), list);
    }

    @Test
    public void testDeleteHeadDelimiter() throws Exception {
        InLongMsgCsvFormatDeserializer deserializer =
                new InLongMsgCsvFormatDeserializer.Builder(TEST_ROW_INFO)
                        .setTimeFieldName("inlongmsg_time")
                        .setAttributesFieldName("inlongmsg_attributes")
                        .setDeleteHeadDelimiter(true)
                        .build();

        InLongMsg inLongMsg1 = InLongMsg.newInLongMsg();
        String attrs = "m=0&streamId=testInterfaceId&t=20200322";
        String body = ",1,2,3,field1,field2,field3";

        inLongMsg1.addMsg(attrs, body.getBytes());

        Map<String, String> expectedAttributes = new HashMap<>();
        expectedAttributes.put("m", "0");
        expectedAttributes.put("streamId", "testInterfaceId");
        expectedAttributes.put("t", "20200322");

        Row expectedRow = Row.of(
                Timestamp.valueOf("2020-03-22 00:00:00"),
                expectedAttributes,
                1,
                2,
                3,
                "field1",
                "field2",
                "field3");

        testRowDeserialization(
                deserializer,
                inLongMsg1.buildArray(),
                Collections.singletonList(expectedRow));
    }

    @Test
    public void testRetainHeadDelimiter() throws Exception {
        InLongMsgCsvFormatDeserializer deserializer =
                new InLongMsgCsvFormatDeserializer.Builder(TEST_ROW_INFO)
                        .setTimeFieldName("inlongmsg_time")
                        .setAttributesFieldName("inlongmsg_attributes")
                        .setDeleteHeadDelimiter(false)
                        .build();

        InLongMsg inLongMsg1 = InLongMsg.newInLongMsg();
        String attrs = "m=0&streamId=testInterfaceId&t=20200322";
        String body = ",1,2,field1,field2,field3";

        inLongMsg1.addMsg(attrs, body.getBytes());

        Map<String, String> expectedAttributes = new HashMap<>();
        expectedAttributes.put("m", "0");
        expectedAttributes.put("streamId", "testInterfaceId");
        expectedAttributes.put("t", "20200322");

        Row expectedRow = Row.of(
                Timestamp.valueOf("2020-03-22 00:00:00"),
                expectedAttributes,
                null,
                1,
                2,
                "field1",
                "field2",
                "field3");

        testRowDeserialization(
                deserializer,
                inLongMsg1.buildArray(),
                Collections.singletonList(expectedRow));
    }

    @Test
    public void testUnmatchedFields1() throws Exception {
        InLongMsgCsvFormatDeserializer deserializer =
                new InLongMsgCsvFormatDeserializer.Builder(TEST_ROW_INFO)
                        .setTimeFieldName("inlongmsg_time")
                        .setAttributesFieldName("inlongmsg_attributes")
                        .build();

        InLongMsg inLongMsg1 = InLongMsg.newInLongMsg();
        String attrs = "m=0&streamId=testInterfaceId&t=20200322&__addcol1__=1&__addcol2__=2";
        String body1 = "123,field11,field12";
        String body2 = "123,field21,field22,field23,field24";
        inLongMsg1.addMsg(attrs, body1.getBytes());
        inLongMsg1.addMsg(attrs, body2.getBytes());

        Map<String, String> expectedAttributes = new HashMap<>();
        expectedAttributes.put("m", "0");
        expectedAttributes.put("streamId", "testInterfaceId");
        expectedAttributes.put("t", "20200322");
        expectedAttributes.put("__addcol1__", "1");
        expectedAttributes.put("__addcol2__", "2");

        Row expectedRow1 = Row.of(
                Timestamp.valueOf("2020-03-22 00:00:00"),
                expectedAttributes,
                1,
                2,
                123,
                "field11",
                "field12",
                null);

        Row expectedRow2 = Row.of(
                Timestamp.valueOf("2020-03-22 00:00:00"),
                expectedAttributes,
                1,
                2,
                123,
                "field21",
                "field22",
                "field23");

        testRowDeserialization(
                deserializer,
                inLongMsg1.buildArray(),
                Arrays.asList(expectedRow1, expectedRow2));
    }

    @Test
    public void testUnmatchedFields2() throws Exception {
        InLongMsgCsvFormatDeserializer deserializer =
                new InLongMsgCsvFormatDeserializer.Builder(TEST_ROW_INFO)
                        .setTimeFieldName("inlongmsg_time")
                        .setAttributesFieldName("inlongmsg_attributes")
                        .build();

        InLongMsg inLongMsg1 = InLongMsg.newInLongMsg();
        String attrs = "m=0&streamId=testInterfaceId&t=20200322&__addcol1__=1&" +
                "__addcol2__=2&__addcol3__=3&__addcol4__=4&__addcol5__=5&" +
                "__addcol6__=6&__addcol7__=7";
        String body = "field11,field12";
        inLongMsg1.addMsg(attrs, body.getBytes());

        Map<String, String> expectedAttributes = new HashMap<>();
        expectedAttributes.put("m", "0");
        expectedAttributes.put("streamId", "testInterfaceId");
        expectedAttributes.put("t", "20200322");
        expectedAttributes.put("__addcol1__", "1");
        expectedAttributes.put("__addcol2__", "2");
        expectedAttributes.put("__addcol3__", "3");
        expectedAttributes.put("__addcol4__", "4");
        expectedAttributes.put("__addcol5__", "5");
        expectedAttributes.put("__addcol6__", "6");
        expectedAttributes.put("__addcol7__", "7");

        Row expectedRow = Row.of(
                Timestamp.valueOf("2020-03-22 00:00:00"),
                expectedAttributes,
                1,
                2,
                3,
                "4",
                "5",
                "6");

        testRowDeserialization(
                deserializer,
                inLongMsg1.buildArray(),
                Collections.singletonList(expectedRow));
    }

    @Test
    public void testLineDelimiter() throws Exception {

        InLongMsgCsvFormatDeserializer deserializer =
                new InLongMsgCsvFormatDeserializer.Builder(TEST_ROW_INFO)
                        .setTimeFieldName("inlongmsg_time")
                        .setAttributesFieldName("inlongmsg_attributes")
                        .setLineDelimiter('\n')
                        .build();

        InLongMsg inLongMsg1 = InLongMsg.newInLongMsg();
        String attrs = "m=0&streamId=testInterfaceId&t=20200322&__addcol1__=1&__addcol2__=2";
        String body = "123,field11,field12,field13\n123,field21,field22,field23";
        inLongMsg1.addMsg(attrs, body.getBytes());

        Map<String, String> expectedAttributes = new HashMap<>();
        expectedAttributes.put("m", "0");
        expectedAttributes.put("streamId", "testInterfaceId");
        expectedAttributes.put("t", "20200322");
        expectedAttributes.put("__addcol1__", "1");
        expectedAttributes.put("__addcol2__", "2");

        Row expectedRow1 = Row.of(
                Timestamp.valueOf("2020-03-22 00:00:00"),
                expectedAttributes,
                1,
                2,
                123,
                "field11",
                "field12",
                "field13");

        Row expectedRow2 = Row.of(
                Timestamp.valueOf("2020-03-22 00:00:00"),
                expectedAttributes,
                1,
                2,
                123,
                "field21",
                "field22",
                "field23");

        testRowDeserialization(
                deserializer,
                inLongMsg1.buildArray(),
                Arrays.asList(expectedRow1, expectedRow2));
    }

    private void testRowDeserialization(
            InLongMsgCsvFormatDeserializer deserializer,
            byte[] bytes,
            List<Row> expectedRows) throws Exception {

        List<Row> actualRows = new ArrayList<>();
        Collector<Row> collector = new ListCollector<>(actualRows);

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