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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.inlong.sort.formats.base.TableFormatConstants.DEFAULT_ENTRY_DELIMITER;
import static org.apache.inlong.sort.formats.base.TableFormatConstants.DEFAULT_KV_DELIMITER;
import static org.apache.inlong.sort.formats.inlongmsgkv.InLongMsgKvUtils.DEFAULT_INLONGMSGKV_CHARSET;
import static org.junit.Assert.assertEquals;

/**
 * Unit tests for {@link InLongMsgKvFormatDeserializer}.
 */
public class InLongMsgKvFormatDeserializerTest {

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
                        DEFAULT_INLONGMSGKV_CHARSET,
                        DEFAULT_ENTRY_DELIMITER,
                        DEFAULT_KV_DELIMITER,
                        null,
                        null,
                        null,
                        null,
                        errorHandler);

        InLongMsg inLongMsg = InLongMsg.newInLongMsg();
        String attrs = "m=0&streamId=testInterfaceId&t=20200322&__addcol1__=1&__addcol2__=2";
        String body1 = "f1=123&f2=field11&f3=field12&f4=field13";
        String body2 = "f1=errormsg&f2=field21&f3=field22&f4=field23";
        inLongMsg.addMsg(attrs, body1.getBytes());
        inLongMsg.addMsg(attrs, body2.getBytes());

        List<Row> actualRows = new ArrayList<>();
        Collector<Row> collector = new ListCollector<>(actualRows);
        deserializer.flatMap(inLongMsg.buildArray(), collector);
        assertEquals(0, errorHandler.getRowCount());

        InLongMsg InLongMsgHead = InLongMsg.newInLongMsg();
        String abNormalAttrs = "m=0&streamId=testInterfaceId&__addcol1__=1&__addcol2__=2";
        InLongMsgHead.addMsg(abNormalAttrs, body1.getBytes());
        InLongMsgHead.addMsg(abNormalAttrs, body2.getBytes());
        deserializer.flatMap(InLongMsgHead.buildArray(), collector);
        assertEquals(1, errorHandler.getHeadCount());
    }

    @Test
    public void testNormal() throws Exception {

        InLongMsg inLongMsg = InLongMsg.newInLongMsg();
        String attrs = "m=0&streamId=testInterfaceId&t=20200322&__addcol1__=1&__addcol2__=2";
        String body1 = "f1=123&f2=field11&f3=field12&f4=field13";
        String body2 = "f1=123&f2=field21&f3=field22&f4=field23";
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

        InLongMsgKvFormatDeserializer deserializer =
                new InLongMsgKvFormatDeserializer.Builder(TEST_ROW_INFO)
                        .setTimeFieldName("inlongmsg_time")
                        .setAttributesFieldName("inlongmsg_attributes")
                        .build();
        testRowDeserialization(
                deserializer, inLongMsg.buildArray(),
                Arrays.asList(expectedRow1, expectedRow2));
    }

    @Test
    public void testNullField() throws Exception {

        InLongMsg inLongMsg = InLongMsg.newInLongMsg();
        String attrs = "m=0&streamId=testInterfaceId&t=20200322&__addcol1__=1&__addcol2__=2";
        String body1 = "f1=123&f2=field11&f3=field12";
        String body2 = "f1=123&f2=field21&f4=field23&f5=field24&=";
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
                null);

        Row expectedRow2 = Row.of(
                Timestamp.valueOf("2020-03-22 00:00:00"),
                expectedAttributes,
                1,
                2,
                123,
                "field21",
                null,
                "field23");

        InLongMsgKvFormatDeserializer deserializer =
                new InLongMsgKvFormatDeserializer.Builder(TEST_ROW_INFO)
                        .setTimeFieldName("inlongmsg_time")
                        .setAttributesFieldName("inlongmsg_attributes")
                        .build();
        testRowDeserialization(
                deserializer, inLongMsg.buildArray(),
                Arrays.asList(expectedRow1, expectedRow2));
    }

    @Test
    public void testNullField1() throws Exception {

        InLongMsg inLongMsg = InLongMsg.newInLongMsg();
        String attrs = "m=0&streamId=testInterfaceId&t=20200322&__addcol1__=1&__addcol2__=2";
        String body1 = "f1=123&f2=field11&f3=field12";
        String body2 = "f1=123&f2=field21&f4=field23&f5=field24&=";
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
                null,
                null,
                123,
                "field11",
                "field12",
                null);

        Row expectedRow2 = Row.of(
                Timestamp.valueOf("2020-03-22 00:00:00"),
                expectedAttributes,
                null,
                null,
                123,
                "field21",
                null,
                "field23");

        InLongMsgKvFormatDeserializer deserializer =
                new InLongMsgKvFormatDeserializer.Builder(TEST_ROW_INFO)
                        .setTimeFieldName("inlongmsg_time")
                        .setAttributesFieldName("inlongmsg_attributes")
                        .setRetainPredefinedField(false)
                        .build();
        testRowDeserialization(
                deserializer, inLongMsg.buildArray(),
                Arrays.asList(expectedRow1, expectedRow2));
    }

    @Test
    public void testLineDelimiter() throws Exception {
        InLongMsg inLongMsg = InLongMsg.newInLongMsg();
        String attrs = "m=0&streamId=testInterfaceId&t=20200322&__addcol1__=1&__addcol2__=2";
        String body = "f1=123&f2=field11\nf1=1&f2=2";
        inLongMsg.addMsg(attrs, body.getBytes());

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
                null,
                null);

        Row expectedRow2 = Row.of(
                Timestamp.valueOf("2020-03-22 00:00:00"),
                expectedAttributes,
                1,
                2,
                1,
                "2",
                null,
                null);

        InLongMsgKvFormatDeserializer deserializer =
                new InLongMsgKvFormatDeserializer.Builder(TEST_ROW_INFO)
                        .setTimeFieldName("inloingmsg_time")
                        .setAttributesFieldName("inlongmsg_attributes")
                        .setLineDelimiter('\n')
                        .build();

        testRowDeserialization(
                deserializer, inLongMsg.buildArray(),
                Arrays.asList(expectedRow1, expectedRow2));
    }

    private void testRowDeserialization(
            InLongMsgKvFormatDeserializer deserializer,
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
                InLongMsgBody body,
                Exception exception) throws Exception {
            rowCount++;
        }

        @Override
        public void onConvertingFieldFailure(String fieldName, String fieldText, FormatInfo formatInfo,
                Exception exception) throws Exception {
            rowCount++;
        }
    }
}
