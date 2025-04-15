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

import static org.apache.inlong.sort.formats.base.TableFormatConstants.DEFAULT_DELIMITER;
import static org.apache.inlong.sort.formats.base.TableFormatConstants.DEFAULT_ENTRY_DELIMITER;
import static org.apache.inlong.sort.formats.base.TableFormatConstants.DEFAULT_KV_DELIMITER;
import static org.apache.inlong.sort.formats.inlongmsg.row.InLongMsgUtils.DEFAULT_ATTRIBUTES_FIELD_NAME;
import static org.apache.inlong.sort.formats.inlongmsg.row.InLongMsgUtils.DEFAULT_TIME_FIELD_NAME;
import static org.apache.inlong.sort.formats.inlongmsgtlogkv.InLongMsgTlogKvUtils.DEFAULT_INLONGMSG_TLOGKV_CHARSET;
import static org.junit.Assert.assertEquals;

/**
 * Tests for {@link InLongMsgTlogKvFormatDeserializer}.
 */
public class InLongMsgTlogKvFormatDeserializerTest {

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
                        null,
                        errorHandler);

        InLongMsg inLongMsg1 = InLongMsg.newInLongMsg(true);
        String attrs = "m=0&t=20200322&__addcol1_=1&__addcol2_=tes";
        String body1 = "testInterfaceId1,f1=field1&f2=field2&f3=field3";
        String body2 = "f1=field1&f2=field2&f3=field3";
        inLongMsg1.addMsg(attrs, body1.getBytes());
        inLongMsg1.addMsg(attrs, body2.getBytes());

        List<Row> actualRows = new ArrayList<>();
        Collector<Row> collector = new ListCollector<>(actualRows);
        deserializer.flatMap(inLongMsg1.buildArray(), collector);
        assertEquals(0, errorHandler.getRowCount());

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
        String body1 = "testInterfaceId1,f1=field1&f2=field2&f3=field3";
        String body2 = "f1=field1&f2=field2&f3=field3";
        String body3 = "f1=field1&f2=field2,f1=field1&f2=field2&f3=field3";
        String body4 = ",testInterfaceId1,f1=field1&f2=field2&f3=field3";

        inLongMsg1.addMsg(attrs, body1.getBytes());
        inLongMsg1.addMsg(attrs, body2.getBytes());
        inLongMsg1.addMsg(attrs, body3.getBytes());
        inLongMsg1.addMsg(attrs, body4.getBytes());

        Map<String, String> expectedAttributes = new HashMap<>();
        expectedAttributes.put("m", "0");
        expectedAttributes.put("t", "20200322");
        expectedAttributes.put("__addcol1_", "1");
        expectedAttributes.put("__addcol2_", "2");

        Row expectedRow1 = Row.of(
                Timestamp.valueOf("2020-03-22 00:00:00"),
                expectedAttributes,
                1,
                2,
                "field1",
                "field2",
                "field3");

        Row expectedRow2 = Row.of(
                Timestamp.valueOf("2020-03-22 00:00:00"),
                expectedAttributes,
                1,
                2,
                null,
                null,
                null);

        Row expectedRow3 = Row.of(
                Timestamp.valueOf("2020-03-22 00:00:00"),
                expectedAttributes,
                1,
                2,
                "field1",
                "field2",
                "field3");

        Row expectedRow4 = Row.of(
                Timestamp.valueOf("2020-03-22 00:00:00"),
                expectedAttributes,
                1,
                2,
                "field1",
                "field2",
                "field3");

        testRowDeserialization(inLongMsg1.buildArray(),
                Arrays.asList(expectedRow1, expectedRow2, expectedRow3, expectedRow4));
    }

    private void testRowDeserialization(
            byte[] bytes,
            List<Row> expectedRows) throws Exception {
        InLongMsgTlogKvFormatDeserializer deserializer =
                new InLongMsgTlogKvFormatDeserializer.Builder(TEST_ROW_INFO)
                        .setTimeFieldName("inlongmsg_time")
                        .setAttributesFieldName("inlongmsg_attributes")
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

        @Override
        public void onConvertingFieldFailure(String fieldName, String fieldText, FormatInfo formatInfo,
                Exception exception) throws Exception {
            rowCount++;
        }
    }
}
