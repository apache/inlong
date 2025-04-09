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
import org.apache.inlong.sort.formats.base.FieldToRowDataConverters;
import org.apache.inlong.sort.formats.inlongmsg.FailureHandler;
import org.apache.inlong.sort.formats.inlongmsg.InLongMsgBody;
import org.apache.inlong.sort.formats.inlongmsg.InLongMsgHead;

import org.apache.flink.api.common.functions.util.ListCollector;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.MapType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.util.Collector;
import org.junit.Test;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.flink.table.api.DataTypes.MAP;
import static org.apache.flink.table.api.DataTypes.STRING;
import static org.apache.inlong.sort.formats.base.TableFormatConstants.DEFAULT_CHARSET;
import static org.apache.inlong.sort.formats.base.TableFormatConstants.DEFAULT_DELIMITER;
import static org.apache.inlong.sort.formats.inlongmsg.InLongMsgMetadata.ReadableMetadata.STREAMID;
import static org.apache.inlong.sort.formats.inlongmsg.InLongMsgUtils.DEFAULT_ATTRIBUTES_FIELD_NAME;
import static org.apache.inlong.sort.formats.inlongmsg.InLongMsgUtils.DEFAULT_TIME_FIELD_NAME;
import static org.junit.Assert.assertEquals;

/**
 * Tests for {@link InLongMsgTlogCsvFormatDeserializer}.
 */
public class InLongMsgTlogCsvFormatDeserializerTest {

    private final FieldToRowDataConverters.FieldToRowDataConverter mapConvert =
            FieldToRowDataConverters.createConverter(MAP(STRING(), STRING()).getLogicalType());

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
                        '\n',
                        null,
                        Collections.emptyList(),
                        false,
                        false,
                        false,
                        errorHandler);
        InLongMsg inLongMsg1 = InLongMsg.newInLongMsg(true);
        String attrs = "m=0&dt=1584806400000&__addcol1_=1&__addcol2_=test";
        String body1 = "interfaceId1,field1,field2,field3\ninterfaceId1,field1,field2,field3";
        String body2 = "interfaceId2,field1,field2,field3";
        inLongMsg1.addMsg(attrs, body1.getBytes());
        inLongMsg1.addMsg(attrs, body2.getBytes());

        List<RowData> actualRows = new ArrayList<>();
        Collector<RowData> collector = new ListCollector<>(actualRows);
        deserializer.flatMap(inLongMsg1.buildArray(), collector);
        assertEquals(3, errorHandler.getRowCount());
        assertEquals(3, actualRows.size());

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

        GenericRowData expectRowData1 = new GenericRowData(7);
        expectRowData1.setField(0, TimestampData.fromTimestamp(new Timestamp(1584806400000L)));
        expectRowData1.setField(1, mapConvert.convert(expectedAttributes));
        expectRowData1.setField(2, 1);
        expectRowData1.setField(3, 2);
        expectRowData1.setField(4, StringData.fromString("field1"));
        expectRowData1.setField(5, StringData.fromString("field2"));
        expectRowData1.setField(6, StringData.fromString("field3"));

        GenericRowData expectRowData2 = new GenericRowData(7);
        expectRowData2.setField(0, TimestampData.fromTimestamp(new Timestamp(1584806400000L)));
        expectRowData2.setField(1, mapConvert.convert(expectedAttributes));
        expectRowData2.setField(2, 1);
        expectRowData2.setField(3, 2);
        expectRowData2.setField(4, StringData.fromString("field1"));
        expectRowData2.setField(5, StringData.fromString("field2"));
        expectRowData2.setField(6, StringData.fromString("field3"));

        List expList = new ArrayList<>();
        expList.add(expectRowData1);
        expList.add(expectRowData2);

        testRowDeserialization(inLongMsg1.buildArray(), expList);
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

        GenericRowData expectRowData = new GenericRowData(7);
        expectRowData.setField(0, TimestampData.fromTimestamp(new Timestamp(1584806400000L)));
        expectRowData.setField(1, mapConvert.convert(expectedAttributes));
        expectRowData.setField(2, 1);
        expectRowData.setField(3, 2);
        expectRowData.setField(4, StringData.fromString("field1"));
        expectRowData.setField(5, StringData.fromString("field2"));
        expectRowData.setField(6, StringData.fromString("field3"));

        List expList = new ArrayList<>();
        expList.add(expectRowData);
        testRowDeserialization(
                inLongMsg1.buildArray(), expList);
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

        GenericRowData expectRowData1 = new GenericRowData(7);
        expectRowData1.setField(0, TimestampData.fromTimestamp(new Timestamp(1584806400000L)));
        expectRowData1.setField(1, mapConvert.convert(expectedAttributes));
        expectRowData1.setField(2, 1);
        expectRowData1.setField(3, 2);
        expectRowData1.setField(4, StringData.fromString("field1"));
        expectRowData1.setField(5, StringData.fromString("field2"));
        expectRowData1.setField(6, null);

        GenericRowData expectRowData2 = new GenericRowData(7);
        expectRowData2.setField(0, TimestampData.fromTimestamp(new Timestamp(1584806400000L)));
        expectRowData2.setField(1, mapConvert.convert(expectedAttributes));
        expectRowData2.setField(2, 1);
        expectRowData2.setField(3, 2);
        expectRowData2.setField(4, StringData.fromString("field1"));
        expectRowData2.setField(5, StringData.fromString("field2"));
        expectRowData2.setField(6, StringData.fromString("field3"));
        List expList = new ArrayList<>();
        expList.add(expectRowData1);
        expList.add(expectRowData2);
        testRowDeserialization(
                inLongMsg1.buildArray(), expList);
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

        GenericRowData expectRowData = new GenericRowData(7);
        expectRowData.setField(0, TimestampData.fromTimestamp(new Timestamp(1584806400000L)));
        expectRowData.setField(1, mapConvert.convert(expectedAttributes));
        expectRowData.setField(2, 1);
        expectRowData.setField(3, 2);
        expectRowData.setField(4, StringData.fromString("3"));
        expectRowData.setField(5, StringData.fromString("4"));
        expectRowData.setField(6, StringData.fromString("5"));

        testRowDeserialization(
                inLongMsg1.buildArray(),
                Collections.singletonList(expectRowData));
    }

    @Test
    public void testRowType() {
        InLongMsgTlogCsvFormatDeserializer deserializer =
                new InLongMsgTlogCsvFormatDeserializer.Builder(TEST_ROW_INFO)
                        .setTimeFieldName("inlongmsg_time")
                        .setAttributesFieldName("inlongmsg_attributes")
                        .setIncludeFirstSegment(false)
                        .setMetadataKeys(Collections.singletonList(STREAMID.getKey()))
                        .build();

        String[] expectedFieldNames = new String[]{
                "inlongmsg_time",
                "inlongmsg_attributes",
                "f1",
                "f2",
                "f3",
                "f4",
                "f5",
                "metadata-streamId"
        };

        LogicalType[] expectedFieldTypes = new LogicalType[]{
                new TimestampType(),
                new MapType(new VarCharType(), new VarCharType()),
                new IntType(),
                new IntType(),
                new VarCharType(),
                new VarCharType(),
                new VarCharType(),
                new VarCharType()
        };
        RowType expectedRowType = RowType.of(expectedFieldTypes, expectedFieldNames);
        assertEquals(InternalTypeInfo.of(expectedRowType), deserializer.getProducedType());
    }

    private void testRowDeserialization(
            byte[] bytes,
            List<RowData> expectedRows) throws Exception {
        InLongMsgTlogCsvFormatDeserializer deserializer =
                new InLongMsgTlogCsvFormatDeserializer.Builder(TEST_ROW_INFO)
                        .setTimeFieldName("inlongmsg_time")
                        .setAttributesFieldName("inlongmsg_attributes")
                        .build();

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
        public void onConvertingRowFailure(InLongMsgHead head, InLongMsgBody body, Exception exception)
                throws Exception {
            rowCount++;
        }

        @Override
        public void onConvertingFieldFailure(String fieldName, String fieldText, FormatInfo formatInfo,
                Exception exception) throws Exception {
            rowCount++;
        }
    }
}
