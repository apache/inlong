/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.inlong.sort.formats.tdmsgcsv;

import static org.apache.inlong.sort.formats.tdmsg.TDMsgUtils.DEFAULT_ATTRIBUTES_FIELD_NAME;
import static org.apache.inlong.sort.formats.tdmsg.TDMsgUtils.DEFAULT_TIME_FIELD_NAME;
import static org.apache.inlong.sort.formats.tdmsg.TDMsgUtils.TDMSG_ATTR_STREAM_ID;
import static org.junit.Assert.assertEquals;

import java.nio.charset.Charset;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.flink.api.common.functions.util.ListCollector;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.apache.inlong.commons.msg.TDMsg1;
import org.apache.inlong.sort.formats.base.TableFormatConstants;
import org.apache.inlong.sort.formats.common.FormatInfo;
import org.apache.inlong.sort.formats.common.IntFormatInfo;
import org.apache.inlong.sort.formats.common.RowFormatInfo;
import org.apache.inlong.sort.formats.common.StringFormatInfo;
import org.junit.Test;

/**
 * Unit tests for {@link TDMsgCsvFormatDeserializer}.
 */
public class TDMsgCsvFormatDeserializerTest {

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
                    }
            );

    @Test
    public void testRowType() {
        TDMsgCsvFormatDeserializer deserializer =
                new TDMsgCsvFormatDeserializer(TEST_ROW_INFO);

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
                        Types.STRING
                );

        assertEquals(expectedRowType, deserializer.getProducedType());
    }

    @Test
    public void testNormal() throws Exception {

        final TDMsgCsvFormatDeserializer deserializer =
                new TDMsgCsvFormatDeserializer(TEST_ROW_INFO);

        TDMsg1 tdMsg1 = TDMsg1.newTDMsg();
        String attrs = "m=0&" + TDMSG_ATTR_STREAM_ID + "=testInterfaceId&t=20200322&__addcol1__=1&__addcol2__=2";
        String body1 = "123,field11,field12,field13";
        String body2 = "123,field21,field22,field23";
        tdMsg1.addMsg(attrs, body1.getBytes());
        tdMsg1.addMsg(attrs, body2.getBytes());

        Map<String, String> expectedAttributes = new HashMap<>();
        expectedAttributes.put("m", "0");
        expectedAttributes.put(TDMSG_ATTR_STREAM_ID, "testInterfaceId");
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
                "field13"
        );

        Row expectedRow2 = Row.of(
                Timestamp.valueOf("2020-03-22 00:00:00"),
                expectedAttributes,
                1,
                2,
                123,
                "field21",
                "field22",
                "field23"
        );

        testRowDeserialization(
                deserializer,
                tdMsg1.buildArray(),
                Arrays.asList(expectedRow1, expectedRow2)
        );
    }

    @Test
    public void testEmptyField() throws Exception {

        final TDMsgCsvFormatDeserializer deserializer =
                new TDMsgCsvFormatDeserializer(TEST_ROW_INFO);

        TDMsg1 tdMsg1 = TDMsg1.newTDMsg();
        String attrs = "m=0&" + TDMSG_ATTR_STREAM_ID + "=testInterfaceId&t=20200322&__addcol1__=1&__addcol2__=2";
        String body1 = "123,field11,field12,";
        String body2 = "123,field21,,field23";
        tdMsg1.addMsg(attrs, body1.getBytes());
        tdMsg1.addMsg(attrs, body2.getBytes());

        Map<String, String> expectedAttributes = new HashMap<>();
        expectedAttributes.put("m", "0");
        expectedAttributes.put(TDMSG_ATTR_STREAM_ID, "testInterfaceId");
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
                ""
        );

        Row expectedRow2 = Row.of(
                Timestamp.valueOf("2020-03-22 00:00:00"),
                expectedAttributes,
                1,
                2,
                123,
                "field21",
                "",
                "field23"
        );

        testRowDeserialization(
                deserializer,
                tdMsg1.buildArray(),
                Arrays.asList(expectedRow1, expectedRow2)
        );
    }

    @Test
    public void testNoPredefinedFields() throws Exception {

        final TDMsgCsvFormatDeserializer deserializer =
                new TDMsgCsvFormatDeserializer(TEST_ROW_INFO);

        TDMsg1 tdMsg1 = TDMsg1.newTDMsg();
        String attrs = "m=0&" + TDMSG_ATTR_STREAM_ID + "=testInterfaceId&t=20200322";
        String body1 = "1,2,123,field11,field12,";
        String body2 = "1,2,123,field21,,field23";
        tdMsg1.addMsg(attrs, body1.getBytes());
        tdMsg1.addMsg(attrs, body2.getBytes());

        Map<String, String> expectedAttributes = new HashMap<>();
        expectedAttributes.put("m", "0");
        expectedAttributes.put(TDMSG_ATTR_STREAM_ID, "testInterfaceId");
        expectedAttributes.put("t", "20200322");

        Row expectedRow1 = Row.of(
                Timestamp.valueOf("2020-03-22 00:00:00"),
                expectedAttributes,
                1,
                2,
                123,
                "field11",
                "field12",
                ""
        );

        Row expectedRow2 = Row.of(
                Timestamp.valueOf("2020-03-22 00:00:00"),
                expectedAttributes,
                1,
                2,
                123,
                "field21",
                "",
                "field23"
        );

        testRowDeserialization(
                deserializer,
                tdMsg1.buildArray(),
                Arrays.asList(expectedRow1, expectedRow2)
        );
    }

    @Test
    public void testIgnoreAttributeErrors() throws Exception {
        TDMsgCsvFormatDeserializer deserializer =
                new TDMsgCsvFormatDeserializer(
                        TEST_ROW_INFO,
                        DEFAULT_TIME_FIELD_NAME,
                        DEFAULT_ATTRIBUTES_FIELD_NAME,
                        Charset.defaultCharset().name(),
                        TableFormatConstants.DEFAULT_DELIMITER,
                        null,
                        null,
                        null,
                        true,
                        true
                );

        TDMsg1 tdMsg1 = TDMsg1.newTDMsg();
        String attrs = "m=0&&&&";
        String body1 = "123,field11,field12,field13";
        tdMsg1.addMsg(attrs, body1.getBytes());

        testRowDeserialization(
                deserializer,
                tdMsg1.buildArray(),
                Collections.emptyList()
        );
    }

    @Test
    public void testIgnoreBodyErrors() throws Exception {
        final TDMsgCsvFormatDeserializer deserializer =
                new TDMsgCsvFormatDeserializer(
                        TEST_ROW_INFO,
                        DEFAULT_TIME_FIELD_NAME,
                        DEFAULT_ATTRIBUTES_FIELD_NAME,
                        Charset.defaultCharset().name(),
                        TableFormatConstants.DEFAULT_DELIMITER,
                        null,
                        null,
                        null,
                        true,
                        true
                );

        TDMsg1 tdMsg1 = TDMsg1.newTDMsg();
        String attrs = "m=0&" + TDMSG_ATTR_STREAM_ID + "=testInterfaceId&t=20200322&__addcol1__=1&__addcol2__=2";
        String body1 = "aaa,field11,field12,field13";
        String body2 = "123,field21,field22,field23";
        tdMsg1.addMsg(attrs, body1.getBytes());
        tdMsg1.addMsg(attrs, body2.getBytes());

        Map<String, String> expectedAttributes = new HashMap<>();
        expectedAttributes.put("m", "0");
        expectedAttributes.put(TDMSG_ATTR_STREAM_ID, "testInterfaceId");
        expectedAttributes.put("t", "20200322");
        expectedAttributes.put("__addcol1__", "1");
        expectedAttributes.put("__addcol2__", "2");

        Row expectedRow2 = Row.of(
                Timestamp.valueOf("2020-03-22 00:00:00"),
                expectedAttributes,
                1,
                2,
                123,
                "field21",
                "field22",
                "field23"
        );

        testRowDeserialization(
                deserializer,
                tdMsg1.buildArray(),
                Collections.singletonList(expectedRow2)
        );
    }

    @Test
    public void testDeleteHeadDelimiter() throws Exception {
        final TDMsgCsvFormatDeserializer deserializer =
                new TDMsgCsvFormatDeserializer(
                        TEST_ROW_INFO,
                        DEFAULT_TIME_FIELD_NAME,
                        DEFAULT_ATTRIBUTES_FIELD_NAME,
                        Charset.defaultCharset().name(),
                        TableFormatConstants.DEFAULT_DELIMITER,
                        null,
                        null,
                        null,
                        true,
                        true
                );

        TDMsg1 tdMsg1 = TDMsg1.newTDMsg();
        String attrs = "m=0&" + TDMSG_ATTR_STREAM_ID + "=testInterfaceId&t=20200322";
        String body = ",1,2,3,field1,field2,field3";

        tdMsg1.addMsg(attrs, body.getBytes());

        Map<String, String> expectedAttributes = new HashMap<>();
        expectedAttributes.put("m", "0");
        expectedAttributes.put(TDMSG_ATTR_STREAM_ID, "testInterfaceId");
        expectedAttributes.put("t", "20200322");

        Row expectedRow = Row.of(
                Timestamp.valueOf("2020-03-22 00:00:00"),
                expectedAttributes,
                1,
                2,
                3,
                "field1",
                "field2",
                "field3"
        );

        testRowDeserialization(
                deserializer,
                tdMsg1.buildArray(),
                Collections.singletonList(expectedRow)
        );
    }

    @Test
    public void testRetainHeadDelimiter() throws Exception {
        final TDMsgCsvFormatDeserializer deserializer =
                new TDMsgCsvFormatDeserializer(
                        TEST_ROW_INFO,
                        DEFAULT_TIME_FIELD_NAME,
                        DEFAULT_ATTRIBUTES_FIELD_NAME,
                        Charset.defaultCharset().name(),
                        TableFormatConstants.DEFAULT_DELIMITER,
                        null,
                        null,
                        null,
                        false,
                        false
                );

        TDMsg1 tdMsg1 = TDMsg1.newTDMsg();
        String attrs = "m=0&" + TDMSG_ATTR_STREAM_ID + "=testInterfaceId&t=20200322";
        String body = ",1,2,field1,field2,field3";

        tdMsg1.addMsg(attrs, body.getBytes());

        Map<String, String> expectedAttributes = new HashMap<>();
        expectedAttributes.put("m", "0");
        expectedAttributes.put(TDMSG_ATTR_STREAM_ID, "testInterfaceId");
        expectedAttributes.put("t", "20200322");

        Row expectedRow = Row.of(
                Timestamp.valueOf("2020-03-22 00:00:00"),
                expectedAttributes,
                null,
                1,
                2,
                "field1",
                "field2",
                "field3"
        );

        testRowDeserialization(
                deserializer,
                tdMsg1.buildArray(),
                Collections.singletonList(expectedRow)
        );
    }

    @Test
    public void testUnmatchedFields1() throws Exception {
        final TDMsgCsvFormatDeserializer deserializer =
                new TDMsgCsvFormatDeserializer(TEST_ROW_INFO);

        TDMsg1 tdMsg1 = TDMsg1.newTDMsg();
        String attrs = "m=0&" + TDMSG_ATTR_STREAM_ID + "=testInterfaceId&t=20200322&__addcol1__=1&__addcol2__=2";
        String body1 = "123,field11,field12";
        String body2 = "123,field21,field22,field23,field24";
        tdMsg1.addMsg(attrs, body1.getBytes());
        tdMsg1.addMsg(attrs, body2.getBytes());

        Map<String, String> expectedAttributes = new HashMap<>();
        expectedAttributes.put("m", "0");
        expectedAttributes.put(TDMSG_ATTR_STREAM_ID, "testInterfaceId");
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
                null
        );

        Row expectedRow2 = Row.of(
                Timestamp.valueOf("2020-03-22 00:00:00"),
                expectedAttributes,
                1,
                2,
                123,
                "field21",
                "field22",
                "field23"
        );

        testRowDeserialization(
                deserializer,
                tdMsg1.buildArray(),
                Arrays.asList(expectedRow1, expectedRow2)
        );
    }

    @Test
    public void testUnmatchedFields2() throws Exception {
        final TDMsgCsvFormatDeserializer deserializer =
                new TDMsgCsvFormatDeserializer(TEST_ROW_INFO);

        TDMsg1 tdMsg1 = TDMsg1.newTDMsg();
        String attrs = "m=0&" + TDMSG_ATTR_STREAM_ID + "=testInterfaceId&t=20200322&__addcol1__=1&"
               + "__addcol2__=2&__addcol3__=3&__addcol4__=4&__addcol5__=5&__addcol6__=6&__addcol7__=7";
        String body = "field11,field12";
        tdMsg1.addMsg(attrs, body.getBytes());

        Map<String, String> expectedAttributes = new HashMap<>();
        expectedAttributes.put("m", "0");
        expectedAttributes.put(TDMSG_ATTR_STREAM_ID, "testInterfaceId");
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
                "6"
        );

        testRowDeserialization(
                deserializer,
                tdMsg1.buildArray(),
                Collections.singletonList(expectedRow)
        );
    }

    private void testRowDeserialization(
            TDMsgCsvFormatDeserializer deserializer,
            byte[] bytes,
            List<Row> expectedRows
    ) throws Exception {

        List<Row> actualRows = new ArrayList<>();
        Collector<Row> collector = new ListCollector<>(actualRows);

        deserializer.flatMap(bytes, collector);

        assertEquals(expectedRows, actualRows);
    }
}
