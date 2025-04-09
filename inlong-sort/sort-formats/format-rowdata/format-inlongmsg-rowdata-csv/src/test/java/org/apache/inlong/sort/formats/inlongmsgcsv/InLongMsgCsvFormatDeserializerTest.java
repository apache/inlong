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
import org.apache.inlong.common.pojo.sort.dataflow.field.format.MapFormatInfo;
import org.apache.inlong.common.pojo.sort.dataflow.field.format.RowFormatInfo;
import org.apache.inlong.common.pojo.sort.dataflow.field.format.StringFormatInfo;
import org.apache.inlong.common.pojo.sort.dataflow.field.format.TimestampFormatInfo;
import org.apache.inlong.sort.formats.base.FieldToRowDataConverters;
import org.apache.inlong.sort.formats.base.FieldToRowDataConverters.FieldToRowDataConverter;
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

import java.nio.charset.Charset;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
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
 * Unit tests for {@link InLongMsgCsvFormatDeserializer}.
 */
public class InLongMsgCsvFormatDeserializerTest {

    private final FieldToRowDataConverter mapConvert =
            FieldToRowDataConverters.createConverter(MAP(STRING(), STRING()).getLogicalType());

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

    private static final RowFormatInfo TEST_NO_PREDEFINE_ROW_INFO =
            new RowFormatInfo(
                    new String[]{"inlongmsg_time", "inlongmsg_attributes", "f1", "f2", "f3", "f4", "f5", "f6"},
                    new FormatInfo[]{
                            new TimestampFormatInfo(),
                            new MapFormatInfo(StringFormatInfo.INSTANCE, StringFormatInfo.INSTANCE),
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
                new InLongMsgCsvFormatDeserializer.Builder(TEST_ROW_INFO)
                        .setTimeFieldName("inlongmsg_time")
                        .setAttributesFieldName("inlongmsg_attributes")
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
                "f6",
                "metadata-streamId"
        };

        LogicalType[] expectedFieldTypes = new LogicalType[]{
                new TimestampType(),
                new MapType(new VarCharType(), new VarCharType()),
                new IntType(),
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

    @Test
    public void testRowTypeWithoutHeadFields() {
        InLongMsgCsvFormatDeserializer deserializer =
                new InLongMsgCsvFormatDeserializer.Builder(TEST_ROW_INFO)
                        .build();

        String[] fieldNames = new String[]{
                "inlongmsg_time",
                "inlongmsg_attributes",
                "f1",
                "f2",
                "f3",
                "f4",
                "f5",
                "f6"
        };

        LogicalType[] fieldTypes = new LogicalType[]{
                new TimestampType(),
                new MapType(new VarCharType(), new VarCharType()),
                new IntType(),
                new IntType(),
                new IntType(),
                new VarCharType(),
                new VarCharType(),
                new VarCharType()
        };
        RowType rowType = RowType.of(fieldTypes, fieldNames);
        assertEquals(InternalTypeInfo.of(rowType), deserializer.getProducedType());
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

        testRowDeserialization(
                deserializer,
                inLongMsg.buildArray(),
                Arrays.asList(expectRowData1, expectRowData2));
    }

    @Test
    public void testInlongMsg() throws Exception {

        InLongMsgCsvFormatDeserializer deserializer =
                new InLongMsgCsvFormatDeserializer.Builder(TEST_ROW_INFO)
                        .setTimeFieldName("inlongmsg_time")
                        .setAttributesFieldName("inlongmsg_attributes")
                        .build();

        InLongMsg inLongMsg = InLongMsg.newInLongMsg();
        String attrs = "m=0&streamId=testStreamId&t=20200322&__addcol1__=1&__addcol2__=2";
        String body1 = "123,field11,field12,field13";
        String body2 = "123,field21,field22,field23";
        inLongMsg.addMsg(attrs, body1.getBytes());
        inLongMsg.addMsg(attrs, body2.getBytes());

        Map<String, String> expectedAttributes = new HashMap<>();
        expectedAttributes.put("m", "0");
        expectedAttributes.put("streamId", "testStreamId");
        expectedAttributes.put("t", "20200322");
        expectedAttributes.put("__addcol1__", "1");
        expectedAttributes.put("__addcol2__", "2");

        GenericRowData expectedRow1 = new GenericRowData(8);
        expectedRow1.setField(0, TimestampData.fromTimestamp(Timestamp.valueOf("2020-03-22 00:00:00")));
        expectedRow1.setField(1, mapConvert.convert(expectedAttributes));
        expectedRow1.setField(2, 1);
        expectedRow1.setField(3, 2);
        expectedRow1.setField(4, 123);
        expectedRow1.setField(5, StringData.fromString("field11"));
        expectedRow1.setField(6, StringData.fromString("field12"));
        expectedRow1.setField(7, StringData.fromString("field13"));

        GenericRowData expectedRow2 = new GenericRowData(8);
        expectedRow2.setField(0, TimestampData.fromTimestamp(Timestamp.valueOf("2020-03-22 00:00:00")));
        expectedRow2.setField(1, mapConvert.convert(expectedAttributes));
        expectedRow2.setField(2, 1);
        expectedRow2.setField(3, 2);
        expectedRow2.setField(4, 123);
        expectedRow2.setField(5, StringData.fromString("field21"));
        expectedRow2.setField(6, StringData.fromString("field22"));
        expectedRow2.setField(7, StringData.fromString("field23"));

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
                        InLongMsgCsvUtils.DEFAULT_DELETE_HEAD_DELIMITER,
                        Collections.emptyList(),
                        true,
                        true,
                        errorHandler);

        InLongMsg inLongMsg = InLongMsg.newInLongMsg();
        String attrs = "m=0&streamId=testInterfaceId&t=20200322&__addcol1__=1&__addcol2__=2";
        String body1 = "test,field11,field12,field13";
        String body2 = "123,field21,field22,field23";
        inLongMsg.addMsg(attrs, body1.getBytes());
        inLongMsg.addMsg(attrs, body2.getBytes());

        List<RowData> actualRows = new ArrayList<>();
        Collector<RowData> collector = new ListCollector<>(actualRows);
        deserializer.flatMap(inLongMsg.buildArray(), collector);
        assertEquals(1, errorHandler.getRowCount());

        InLongMsg inLongMsg1Head = InLongMsg.newInLongMsg();
        String abNormalAttrs = "m=0&streamId=testInterfaceId&__addcol1__=1&__addcol2__=2";
        inLongMsg1Head.addMsg(abNormalAttrs, body1.getBytes());
        inLongMsg1Head.addMsg(abNormalAttrs, body2.getBytes());
        deserializer.flatMap(inLongMsg1Head.buildArray(), collector);
        assertEquals(1, errorHandler.getHeadCount());
    }

    @Test
    public void testEmptyField() throws Exception {

        InLongMsgCsvFormatDeserializer deserializer =
                new InLongMsgCsvFormatDeserializer.Builder(TEST_ROW_INFO)
                        .setTimeFieldName("inlongmsg_time")
                        .setAttributesFieldName("inlongmsg_attributes")
                        .build();

        InLongMsg inLongMsg = InLongMsg.newInLongMsg();
        String attrs = "m=0&streamId=testInterfaceId&t=20200322&__addcol1__=1&__addcol2__=2";
        String body1 = "123,field11,field12,";
        String body2 = "123,field21,,field23";
        inLongMsg.addMsg(attrs, body1.getBytes());
        inLongMsg.addMsg(attrs, body2.getBytes());

        Map<String, String> expectedAttributes = new HashMap<>();
        expectedAttributes.put("m", "0");
        expectedAttributes.put("streamId", "testInterfaceId");
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
        expectRowData1.setField(7, StringData.fromString(""));

        GenericRowData expectRowData2 = new GenericRowData(8);
        expectRowData2.setField(0, TimestampData.fromTimestamp(Timestamp.valueOf("2020-03-22 00:00:00")));
        expectRowData2.setField(1, mapConvert.convert(expectedAttributes));
        expectRowData2.setField(2, 1);
        expectRowData2.setField(3, 2);
        expectRowData2.setField(4, 123);
        expectRowData2.setField(5, StringData.fromString("field21"));
        expectRowData2.setField(6, StringData.fromString(""));
        expectRowData2.setField(7, StringData.fromString("field23"));

        testRowDeserialization(
                deserializer,
                inLongMsg.buildArray(),
                Arrays.asList(expectRowData1, expectRowData2));
    }

    @Test
    public void testNoPredefinedFields() throws Exception {

        InLongMsgCsvFormatDeserializer deserializer =
                new InLongMsgCsvFormatDeserializer.Builder(TEST_ROW_INFO)
                        .setTimeFieldName("inlongmsg_time")
                        .setAttributesFieldName("inlongmsg_attributes")
                        .build();

        InLongMsg inLongMsg = InLongMsg.newInLongMsg();
        String attrs = "m=0&streamId=testInterfaceId&t=20200322";
        String body1 = "1,2,123,field11,field12,";
        String body2 = "1,2,123,field21,,field23";
        inLongMsg.addMsg(attrs, body1.getBytes());
        inLongMsg.addMsg(attrs, body2.getBytes());

        Map<String, String> expectedAttributes = new HashMap<>();
        expectedAttributes.put("m", "0");
        expectedAttributes.put("streamId", "testInterfaceId");
        expectedAttributes.put("t", "20200322");

        GenericRowData expectRowData1 = new GenericRowData(8);
        expectRowData1.setField(0, TimestampData.fromTimestamp(Timestamp.valueOf("2020-03-22 00:00:00")));
        expectRowData1.setField(1, mapConvert.convert(expectedAttributes));
        expectRowData1.setField(2, 1);
        expectRowData1.setField(3, 2);
        expectRowData1.setField(4, 123);
        expectRowData1.setField(5, StringData.fromString("field11"));
        expectRowData1.setField(6, StringData.fromString("field12"));
        expectRowData1.setField(7, StringData.fromString(""));

        GenericRowData expectRowData2 = new GenericRowData(8);
        expectRowData2.setField(0, TimestampData.fromTimestamp(Timestamp.valueOf("2020-03-22 00:00:00")));
        expectRowData2.setField(1, mapConvert.convert(expectedAttributes));
        expectRowData2.setField(2, 1);
        expectRowData2.setField(3, 2);
        expectRowData2.setField(4, 123);
        expectRowData2.setField(5, StringData.fromString("field21"));
        expectRowData2.setField(6, StringData.fromString(""));
        expectRowData2.setField(7, StringData.fromString("field23"));

        testRowDeserialization(
                deserializer,
                inLongMsg.buildArray(),
                Arrays.asList(expectRowData1, expectRowData2));
    }

    @Test
    public void testPredefinedFieldWithFlagOn() throws Exception {

        InLongMsgCsvFormatDeserializer deserializer =
                new InLongMsgCsvFormatDeserializer.Builder(TEST_ROW_INFO)
                        .setTimeFieldName("inlongmsg_time")
                        .setAttributesFieldName("inlongmsg_attributes")
                        .setRetainPredefinedField(true)
                        .build();

        InLongMsg inLongMsg = InLongMsg.newInLongMsg();
        String attrs = "m=0&streamId=testInterfaceId&t=20200322" +
                "&__addcol1__repdate=20220224&__addcol2__hour=1517";
        String body1 = "123,field11,field12,";
        String body2 = "123,field21,,field23";
        inLongMsg.addMsg(attrs, body1.getBytes());
        inLongMsg.addMsg(attrs, body2.getBytes());

        Map<String, String> expectedAttributes = new HashMap<>();
        expectedAttributes.put("m", "0");
        expectedAttributes.put("streamId", "testInterfaceId");
        expectedAttributes.put("t", "20200322");
        expectedAttributes.put("__addcol1__repdate", "20220224");
        expectedAttributes.put("__addcol2__hour", "1517");

        GenericRowData expectRowData1 = new GenericRowData(8);
        expectRowData1.setField(0, TimestampData.fromTimestamp(Timestamp.valueOf("2020-03-22 00:00:00")));
        expectRowData1.setField(1, mapConvert.convert(expectedAttributes));
        expectRowData1.setField(2, 20220224);
        expectRowData1.setField(3, 1517);
        expectRowData1.setField(4, 123);
        expectRowData1.setField(5, StringData.fromString("field11"));
        expectRowData1.setField(6, StringData.fromString("field12"));
        expectRowData1.setField(7, StringData.fromString(""));

        GenericRowData expectRowData2 = new GenericRowData(8);
        expectRowData2.setField(0, TimestampData.fromTimestamp(Timestamp.valueOf("2020-03-22 00:00:00")));
        expectRowData2.setField(1, mapConvert.convert(expectedAttributes));
        expectRowData2.setField(2, 20220224);
        expectRowData2.setField(3, 1517);
        expectRowData2.setField(4, 123);
        expectRowData2.setField(5, StringData.fromString("field21"));
        expectRowData2.setField(6, StringData.fromString(""));
        expectRowData2.setField(7, StringData.fromString("field23"));

        testRowDeserialization(
                deserializer,
                inLongMsg.buildArray(),
                Arrays.asList(expectRowData1, expectRowData2));
    }

    @Test
    public void testPredefinedFieldWithFlagOff() throws Exception {

        InLongMsgCsvFormatDeserializer deserializer =
                new InLongMsgCsvFormatDeserializer.Builder(TEST_ROW_INFO)
                        .setTimeFieldName("inlongmsg_time")
                        .setAttributesFieldName("inlongmsg_attributes")
                        .setRetainPredefinedField(false)
                        .build();

        InLongMsg inLongMsg = InLongMsg.newInLongMsg();
        String attrs = "m=0&streamId=testInterfaceId&t=20200322" +
                "&__addcol1__repdate=20220224&__addcol2__hour=1517";
        String body1 = "1,2,123,field11,field12,";
        String body2 = "1,2,123,field21,,field23";
        inLongMsg.addMsg(attrs, body1.getBytes());
        inLongMsg.addMsg(attrs, body2.getBytes());

        Map<String, String> expectedAttributes = new HashMap<>();
        expectedAttributes.put("m", "0");
        expectedAttributes.put("streamId", "testInterfaceId");
        expectedAttributes.put("t", "20200322");
        expectedAttributes.put("__addcol1__repdate", "20220224");
        expectedAttributes.put("__addcol2__hour", "1517");

        GenericRowData expectRowData1 = new GenericRowData(8);
        expectRowData1.setField(0, TimestampData.fromTimestamp(Timestamp.valueOf("2020-03-22 00:00:00")));
        expectRowData1.setField(1, mapConvert.convert(expectedAttributes));
        expectRowData1.setField(2, 1);
        expectRowData1.setField(3, 2);
        expectRowData1.setField(4, 123);
        expectRowData1.setField(5, StringData.fromString("field11"));
        expectRowData1.setField(6, StringData.fromString("field12"));
        expectRowData1.setField(7, StringData.fromString(""));

        GenericRowData expectRowData2 = new GenericRowData(8);
        expectRowData2.setField(0, TimestampData.fromTimestamp(Timestamp.valueOf("2020-03-22 00:00:00")));
        expectRowData2.setField(1, mapConvert.convert(expectedAttributes));
        expectRowData2.setField(2, 1);
        expectRowData2.setField(3, 2);
        expectRowData2.setField(4, 123);
        expectRowData2.setField(5, StringData.fromString("field21"));
        expectRowData2.setField(6, StringData.fromString(""));
        expectRowData2.setField(7, StringData.fromString("field23"));

        testRowDeserialization(
                deserializer,
                inLongMsg.buildArray(),
                Arrays.asList(expectRowData1, expectRowData2));
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
                        Collections.emptyList(),
                        true);

        InLongMsg inLongMsg = InLongMsg.newInLongMsg();
        String attrs = "m=0&&&&";
        String body1 = "123,field11,field12,field13";
        inLongMsg.addMsg(attrs, body1.getBytes());

        testRowDeserialization(
                deserializer,
                inLongMsg.buildArray(),
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

        InLongMsg inLongMsg = InLongMsg.newInLongMsg();
        String attrs = "m=0&streamId=testInterfaceId&t=20200322&__addcol1__=1&__addcol2__=2";
        String body1 = "aaa,field11,field12,field13";
        String body2 = "123,field21,field22,field23";
        inLongMsg.addMsg(attrs, body1.getBytes());
        inLongMsg.addMsg(attrs, body2.getBytes());

        Map<String, String> expectedAttributes = new HashMap<>();
        expectedAttributes.put("m", "0");
        expectedAttributes.put("streamId", "testInterfaceId");
        expectedAttributes.put("t", "20200322");
        expectedAttributes.put("__addcol1__", "1");
        expectedAttributes.put("__addcol2__", "2");

        GenericRowData expectRowData1 = new GenericRowData(8);
        expectRowData1.setField(0, TimestampData.fromTimestamp(Timestamp.valueOf("2020-03-22 00:00:00")));
        expectRowData1.setField(1, mapConvert.convert(expectedAttributes));
        expectRowData1.setField(2, 1);
        expectRowData1.setField(3, 2);
        expectRowData1.setField(4, null);
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

        List expectList = new ArrayList();
        expectList.add(expectRowData1);
        expectList.add(expectRowData2);
        testRowDeserialization(
                deserializer,
                inLongMsg.buildArray(),
                expectList);
    }

    @Test
    public void testDeleteHeadDelimiter() throws Exception {
        InLongMsgCsvFormatDeserializer deserializer =
                new InLongMsgCsvFormatDeserializer.Builder(TEST_ROW_INFO)
                        .setTimeFieldName("inlongmsg_time")
                        .setAttributesFieldName("inlongmsg_attributes")
                        .setDeleteHeadDelimiter(true)
                        .build();

        InLongMsg inLongMsg = InLongMsg.newInLongMsg();
        String attrs = "m=0&streamId=testInterfaceId&t=20200322";
        String body = ",1,2,3,field1,field2,field3";

        inLongMsg.addMsg(attrs, body.getBytes());

        Map<String, String> expectedAttributes = new HashMap<>();
        expectedAttributes.put("m", "0");
        expectedAttributes.put("streamId", "testInterfaceId");
        expectedAttributes.put("t", "20200322");

        GenericRowData expectRowData = new GenericRowData(8);
        expectRowData.setField(0, TimestampData.fromTimestamp(Timestamp.valueOf("2020-03-22 00:00:00")));
        expectRowData.setField(1, mapConvert.convert(expectedAttributes));
        expectRowData.setField(2, 1);
        expectRowData.setField(3, 2);
        expectRowData.setField(4, 3);
        expectRowData.setField(5, StringData.fromString("field1"));
        expectRowData.setField(6, StringData.fromString("field2"));
        expectRowData.setField(7, StringData.fromString("field3"));

        testRowDeserialization(
                deserializer,
                inLongMsg.buildArray(),
                Collections.singletonList(expectRowData));
    }

    @Test
    public void testRetainHeadDelimiter() throws Exception {
        InLongMsgCsvFormatDeserializer deserializer =
                new InLongMsgCsvFormatDeserializer.Builder(TEST_ROW_INFO)
                        .setTimeFieldName("inlongmsg_time")
                        .setAttributesFieldName("inlongmsg_attributes")
                        .setDeleteHeadDelimiter(false)
                        .build();

        InLongMsg inLongMsg = InLongMsg.newInLongMsg();
        String attrs = "m=0&streamId=testInterfaceId&t=20200322";
        String body = ",1,2,field1,field2,field3";

        inLongMsg.addMsg(attrs, body.getBytes());

        Map<String, String> expectedAttributes = new HashMap<>();
        expectedAttributes.put("m", "0");
        expectedAttributes.put("streamId", "testInterfaceId");
        expectedAttributes.put("t", "20200322");

        GenericRowData expectRowData = new GenericRowData(8);
        expectRowData.setField(0, TimestampData.fromTimestamp(Timestamp.valueOf("2020-03-22 00:00:00")));
        expectRowData.setField(1, mapConvert.convert(expectedAttributes));
        expectRowData.setField(2, null);
        expectRowData.setField(3, 1);
        expectRowData.setField(4, 2);
        expectRowData.setField(5, StringData.fromString("field1"));
        expectRowData.setField(6, StringData.fromString("field2"));
        expectRowData.setField(7, StringData.fromString("field3"));

        testRowDeserialization(
                deserializer,
                inLongMsg.buildArray(),
                Collections.singletonList(expectRowData));
    }

    @Test
    public void testUnmatchedFields1() throws Exception {
        InLongMsgCsvFormatDeserializer deserializer =
                new InLongMsgCsvFormatDeserializer.Builder(TEST_ROW_INFO)
                        .setTimeFieldName("inlongmsg_time")
                        .setAttributesFieldName("inlongmsg_attributes")
                        .build();

        InLongMsg inLongMsg = InLongMsg.newInLongMsg();
        String attrs = "m=0&streamId=testInterfaceId&t=20200322&__addcol1__=1&__addcol2__=2";
        String body1 = "123,field11,field12";
        String body2 = "123,field21,field22,field23,field24";
        inLongMsg.addMsg(attrs, body1.getBytes());
        inLongMsg.addMsg(attrs, body2.getBytes());

        Map<String, String> expectedAttributes = new HashMap<>();
        expectedAttributes.put("m", "0");
        expectedAttributes.put("streamId", "testInterfaceId");
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
        expectRowData2.setField(6, StringData.fromString("field22"));
        expectRowData2.setField(7, StringData.fromString("field23"));

        testRowDeserialization(
                deserializer,
                inLongMsg.buildArray(),
                Arrays.asList(expectRowData1, expectRowData2));
    }

    @Test
    public void testUnmatchedFields2() throws Exception {
        InLongMsgCsvFormatDeserializer deserializer =
                new InLongMsgCsvFormatDeserializer.Builder(TEST_ROW_INFO)
                        .setTimeFieldName("inlongmsg_time")
                        .setAttributesFieldName("inlongmsg_attributes")
                        .build();

        InLongMsg inLongMsg = InLongMsg.newInLongMsg();
        String attrs = "m=0&streamId=testInterfaceId&t=20200322&__addcol1__=1&" +
                "__addcol2__=2&__addcol3__=3&__addcol4__=4&__addcol5__=5&" +
                "__addcol6__=6&__addcol7__=7";
        String body = "field11,field12";
        inLongMsg.addMsg(attrs, body.getBytes());

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

        GenericRowData expectRowData = new GenericRowData(8);
        expectRowData.setField(0, TimestampData.fromTimestamp(Timestamp.valueOf("2020-03-22 00:00:00")));
        expectRowData.setField(1, mapConvert.convert(expectedAttributes));
        expectRowData.setField(2, 1);
        expectRowData.setField(3, 2);
        expectRowData.setField(4, 3);
        expectRowData.setField(5, StringData.fromString("4"));
        expectRowData.setField(6, StringData.fromString("5"));
        expectRowData.setField(7, StringData.fromString("6"));

        testRowDeserialization(
                deserializer,
                inLongMsg.buildArray(),
                Collections.singletonList(expectRowData));
    }

    @Test
    public void testLineDelimiter() throws Exception {

        InLongMsgCsvFormatDeserializer deserializer =
                new InLongMsgCsvFormatDeserializer.Builder(TEST_ROW_INFO)
                        .setTimeFieldName("inlongmsg_time")
                        .setAttributesFieldName("inlongmsg_attributes")
                        .setLineDelimiter('\n')
                        .build();

        InLongMsg inLongMsg = InLongMsg.newInLongMsg();
        String attrs = "m=0&streamId=testInterfaceId&t=20200322&__addcol1__=1&__addcol2__=2";
        String body = "123,field11,field12,field13\n123,field21,field22,field23";
        inLongMsg.addMsg(attrs, body.getBytes());

        Map<String, String> expectedAttributes = new HashMap<>();
        expectedAttributes.put("m", "0");
        expectedAttributes.put("streamId", "testInterfaceId");
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

        testRowDeserialization(
                deserializer,
                inLongMsg.buildArray(),
                Arrays.asList(expectRowData1, expectRowData2));
    }

    @Test
    public void testMetadata() throws Exception {

        InLongMsgCsvFormatDeserializer deserializer =
                new InLongMsgCsvFormatDeserializer.Builder(TEST_ROW_INFO)
                        .setTimeFieldName("inlongmsg_time")
                        .setAttributesFieldName("inlongmsg_attributes")
                        .setMetadataKeys(Collections.singletonList(STREAMID.getKey()))
                        .build();

        InLongMsg inLongMsg = InLongMsg.newInLongMsg();
        String attrs = "m=0&streamId=testInterfaceId&t=20200322&__addcol1__=1&__addcol2__=2";
        String body = "123,field11,field12,field13";
        inLongMsg.addMsg(attrs, body.getBytes());

        Map<String, String> expectedAttributes = new HashMap<>();
        expectedAttributes.put("m", "0");
        expectedAttributes.put("streamId", "testInterfaceId");
        expectedAttributes.put("t", "20200322");
        expectedAttributes.put("__addcol1__", "1");
        expectedAttributes.put("__addcol2__", "2");

        GenericRowData expectRowData = new GenericRowData(9);
        expectRowData.setField(0, TimestampData.fromTimestamp(Timestamp.valueOf("2020-03-22 00:00:00")));
        expectRowData.setField(1, mapConvert.convert(expectedAttributes));
        expectRowData.setField(2, 1);
        expectRowData.setField(3, 2);
        expectRowData.setField(4, 123);
        expectRowData.setField(5, StringData.fromString("field11"));
        expectRowData.setField(6, StringData.fromString("field12"));
        expectRowData.setField(7, StringData.fromString("field13"));
        expectRowData.setField(8, StringData.fromString("testInterfaceId"));

        testRowDeserialization(
                deserializer,
                inLongMsg.buildArray(),
                Collections.singletonList(expectRowData));
    }

    private void testRowDeserialization(
            InLongMsgCsvFormatDeserializer deserializer,
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
        public void onConvertingRowFailure(InLongMsgHead head, InLongMsgBody body,
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
