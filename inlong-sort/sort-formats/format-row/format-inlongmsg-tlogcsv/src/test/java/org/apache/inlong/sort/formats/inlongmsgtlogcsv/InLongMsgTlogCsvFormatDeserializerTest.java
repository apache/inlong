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

package org.apache.inlong.sort.formats.inlongmsgtlogcsv;

import org.apache.inlong.common.msg.InLongMsg;
import org.apache.inlong.common.pojo.sort.dataflow.field.format.FormatInfo;
import org.apache.inlong.common.pojo.sort.dataflow.field.format.IntFormatInfo;
import org.apache.inlong.common.pojo.sort.dataflow.field.format.RowFormatInfo;
import org.apache.inlong.common.pojo.sort.dataflow.field.format.StringFormatInfo;
import org.apache.inlong.sort.formats.inlongmsg.FailureHandler;
import org.apache.inlong.sort.formats.inlongmsg.InLongMsgBody;
import org.apache.inlong.sort.formats.inlongmsg.InLongMsgHead;

import org.apache.flink.api.common.functions.util.ListCollector;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.junit.Test;

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
import static org.junit.Assert.assertEquals;

/**
 * Tests for {@link InLongMsgTlogCsvFormatDeserializer}.
 */
public class InLongMsgTlogCsvFormatDeserializerTest {

    private static final RowFormatInfo TEST_ROW_INFO =
            new RowFormatInfo(
                    new String[]{"__addcol1_", "__addcol2_", "f1", "f2", "f3"},
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
        InLongMsgTlogCsvFormatDeserializer deserializer =
                new InLongMsgTlogCsvFormatDeserializer(
                        TEST_ROW_INFO,
                        DEFAULT_TIME_FIELD_NAME,
                        DEFAULT_ATTRIBUTES_FIELD_NAME,
                        DEFAULT_CHARSET,
                        DEFAULT_DELIMITER,
                        null,
                        null,
                        null,
                        false,
                        errorHandler);

        InLongMsg inLongMsg1 = InLongMsg.newInLongMsg(true);
        String attrs = "m=0&dt=1584806400000&__addcol1_=1&__addcol2_=test";
        String body1 = "interfaceId1,field1,field2,field3";
        String body2 = "interfaceId2,field1,field2,field3";
        inLongMsg1.addMsg(attrs, body1.getBytes());
        inLongMsg1.addMsg(attrs, body2.getBytes());

        List<Row> actualRows = new ArrayList<>();
        Collector<Row> collector = new ListCollector<>(actualRows);
        deserializer.flatMap(inLongMsg1.buildArray(), collector);
        assertEquals(0, errorHandler.getRowCount());

        InLongMsg inLongMsg1Head = InLongMsg.newInLongMsg();
        String abNormalAttrs = "m=0&__addcol1_=1&__addcol2_=test";
        inLongMsg1Head.addMsg(abNormalAttrs, body1.getBytes());
        inLongMsg1Head.addMsg(abNormalAttrs, body2.getBytes());
        deserializer.flatMap(inLongMsg1Head.buildArray(), collector);
        assertEquals(1, errorHandler.getHeadCount());
    }

    @Test
    public void testNormal() throws Exception {
        InLongMsg inLongMsg1 = InLongMsg.newInLongMsg(true);
        String attrs = "m=0&dt=1584806400000&__addcol1_=1&__addcol2_=2";
        String body1 = "interfaceId1,field1,field2,field3";
        String body2 = "interfaceId2,field1,field2,field3";
        inLongMsg1.addMsg(attrs, body1.getBytes());
        inLongMsg1.addMsg(attrs, body2.getBytes());

        Map<String, String> expectedAttributes = new HashMap<>();
        expectedAttributes.put("m", "0");
        expectedAttributes.put("dt", "1584806400000");
        expectedAttributes.put("__addcol1_", "1");
        expectedAttributes.put("__addcol2_", "2");

        Row expectedRow1 = Row.of(
                new Timestamp(1584806400000L),
                expectedAttributes,
                1,
                2,
                "field1",
                "field2",
                "field3");

        Row expectedRow2 = Row.of(
                new Timestamp(1584806400000L),
                expectedAttributes,
                1,
                2,
                "field1",
                "field2",
                "field3");

        testRowDeserialization(
                inLongMsg1.buildArray(),
                Arrays.asList(expectedRow1, expectedRow2));
    }

    @Test
    public void testDeleteHeadDelimiter() throws Exception {
        InLongMsg inLongMsg1 = InLongMsg.newInLongMsg(true);
        String attrs = "m=0&dt=1584806400000&__addcol1_=1&__addcol2_=2";
        String body = ",interfaceId,field1,field2,field3";
        inLongMsg1.addMsg(attrs, body.getBytes());

        Map<String, String> expectedAttributes = new HashMap<>();
        expectedAttributes.put("m", "0");
        expectedAttributes.put("dt", "1584806400000");
        expectedAttributes.put("__addcol1_", "1");
        expectedAttributes.put("__addcol2_", "2");

        Row expectedRow = Row.of(
                new Timestamp(1584806400000L),
                expectedAttributes,
                1,
                2,
                "field1",
                "field2",
                "field3");

        testRowDeserialization(
                inLongMsg1.buildArray(),
                Collections.singletonList(expectedRow));
    }

    @Test
    public void testUnmatchedFields1() throws Exception {
        InLongMsg inLongMsg1 = InLongMsg.newInLongMsg(true);
        String attrs = "m=0&dt=1584806400000&__addcol1_=1&__addcol2_=2";
        String body1 = "interfaceId1,field1,field2";
        String body2 = "interfaceId2,field1,field2,field3,field4";
        inLongMsg1.addMsg(attrs, body1.getBytes());
        inLongMsg1.addMsg(attrs, body2.getBytes());

        Map<String, String> expectedAttributes = new HashMap<>();
        expectedAttributes.put("m", "0");
        expectedAttributes.put("dt", "1584806400000");
        expectedAttributes.put("__addcol1_", "1");
        expectedAttributes.put("__addcol2_", "2");

        Row expectedRow1 = Row.of(
                new Timestamp(1584806400000L),
                expectedAttributes,
                1,
                2,
                "field1",
                "field2",
                null);

        Row expectedRow2 = Row.of(
                new Timestamp(1584806400000L),
                expectedAttributes,
                1,
                2,
                "field1",
                "field2",
                "field3");
        List<Row> expectedRows = new ArrayList<>();
        expectedRows.add(expectedRow1);
        expectedRows.add(expectedRow2);
        testRowDeserialization(
                inLongMsg1.buildArray(),
                expectedRows);
    }

    @Test
    public void testUnmatchedFields2() throws Exception {
        InLongMsg inLongMsg1 = InLongMsg.newInLongMsg(true);
        String attrs = "m=0&dt=1584806400000&__addcol1_=1&__addcol2_=2&" +
                "__addcol3_=3&__addcol4_=4&__addcol5_=5&__addcol6_=6";
        String body = "interfaceId1,field1,field2,field3";
        inLongMsg1.addMsg(attrs, body.getBytes());

        Map<String, String> expectedAttributes = new HashMap<>();
        expectedAttributes.put("m", "0");
        expectedAttributes.put("dt", "1584806400000");
        expectedAttributes.put("__addcol1_", "1");
        expectedAttributes.put("__addcol2_", "2");
        expectedAttributes.put("__addcol3_", "3");
        expectedAttributes.put("__addcol4_", "4");
        expectedAttributes.put("__addcol5_", "5");
        expectedAttributes.put("__addcol6_", "6");

        Row expectedRow = Row.of(
                new Timestamp(1584806400000L),
                expectedAttributes,
                1,
                2,
                "3",
                "4",
                "5");

        testRowDeserialization(
                inLongMsg1.buildArray(),
                Collections.singletonList(expectedRow));
    }

    private void testRowDeserialization(
            byte[] bytes,
            List<Row> expectedRows) throws Exception {
        InLongMsgTlogCsvFormatDeserializer deserializer =
                new InLongMsgTlogCsvFormatDeserializer.Builder(TEST_ROW_INFO)
                        .setTimeFieldName("inlongmsg_time")
                        .setAttributesFieldName("inlongmsg_attributes")
                        .setIsIncludeFirstSegment(false)
                        .build();

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
    }
}
