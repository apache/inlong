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

package org.apache.inlong.sort.formats.inlongmsg;

import org.apache.inlong.common.pojo.sort.dataflow.field.format.RowFormatInfo;
import org.apache.inlong.sort.formats.base.FieldToRowDataConverters;
import org.apache.inlong.sort.formats.base.TableFormatUtils;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.descriptors.DescriptorProperties;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.BinaryType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.MapType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.logical.VarCharType;

import javax.annotation.Nullable;

import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.inlong.sort.formats.base.TableFormatConstants.FORMAT_ATTRIBUTE_FIELD_NAME;
import static org.apache.inlong.sort.formats.base.TableFormatConstants.FORMAT_TIME_FIELD_NAME;
import static org.apache.inlong.sort.formats.base.TableFormatUtils.validateSchema;
import static org.apache.inlong.sort.formats.inlongmsg.InLongMsgMetadata.ReadableMetadata.STREAMID;
import static org.apache.inlong.sort.formats.util.StringUtils.splitKv;

/**
 * A utility class for parsing InLongMsg {@link}.
 */
public class InLongMsgUtils {

    public static final char INLONGMSG_ATTR_ENTRY_DELIMITER = '&';
    public static final char INLONGMSG_ATTR_KV_DELIMITER = '=';

    // keys in attributes
    @Deprecated
    public static final String INLONGMSG_ATTR_INTERFACE_NAME = "iname";
    @Deprecated
    public static final String INLONGMSG_ATTR_INTERFACE_ID = "id";
    @Deprecated
    public static final String INLONGMSG_ATTR_INTERFACE_TID = "tid";
    public static final String INLONGMSG_ATTR_STREAM_ID = "streamId";
    public static final String INLONGMSG_ATTR_TIME_T = "t";
    public static final String INLONGMSG_ATTR_TIME_DT = "dt";
    public static final String INLONGMSG_ATTR_ADD_COLUMN_PREFIX = "__addcol";

    public static final String DEFAULT_TIME_FIELD_NAME = "inlongmsg_time";
    public static final String DEFAULT_ATTRIBUTES_FIELD_NAME = "inlongmsg_attributes";

    public static final boolean DEFAULT_IS_RETAIN_PREDEFINED_FIELD = false;
    public static final boolean DEFAULT_IS_DELETE_ESCAPE_CHAR = true;
    public static final boolean DEFAULT_IS_PATCH_ESCAPE_CHAR = false;
    public static final boolean DEFAULT_IS_DELETE_HEAD_DELIMITER = false;

    private static final FieldToRowDataConverters.FieldToRowDataConverter TIME_FIELD_CONVERTER =
            FieldToRowDataConverters.createConverter(new TimestampType());

    private static final FieldToRowDataConverters.FieldToRowDataConverter ATTRIBUTES_FIELD_CONVERTER =
            FieldToRowDataConverters.createConverter(new MapType(new VarCharType(), new VarCharType()));

    public static TypeInformation<RowData> buildMixedRowDataType() {

        String[] fieldNames = new String[]{
                "attributes",
                "data",
                "streamId",
                "time",
                "predefinedFields",
                "fields",
                "entries"
        };
        LogicalType[] fieldTypes = new LogicalType[]{
                new MapType(new VarCharType(), new VarCharType()),
                new BinaryType(),
                new VarCharType(),
                new TimestampType(),
                new VarCharType(),
                new VarCharType(),
                new MapType(new VarCharType(), new VarCharType())
        };
        RowType rowType = RowType.of(fieldTypes, fieldNames);
        return InternalTypeInfo.of(rowType);
    }

    public static Map<String, String> parseAttr(String attr) {
        return splitKv(
                attr,
                INLONGMSG_ATTR_ENTRY_DELIMITER,
                INLONGMSG_ATTR_KV_DELIMITER,
                null,
                null);
    }

    public static Timestamp parseEpochTime(String value) {
        long millis = Long.parseLong(value);
        return new Timestamp(millis);
    }

    /**
     * Parse the date from the given string.
     *
     * @param value The date to be parsed. The format of dates may be one of
     *              {yyyyMMdd, yyyyMMddHH, yyyyMMddHHmm}.
     */
    public static Timestamp parseDateTime(String value) {
        try {
            if (value.length() < 8) {
                return null;
            } else if (value.length() <= 9) {
                SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyyMMdd");
                Date date = simpleDateFormat.parse(value.substring(0, 8));
                return new Timestamp(date.getTime());
            } else if (value.length() <= 11) {
                SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyyMMddHH");
                Date date = simpleDateFormat.parse(value.substring(0, 10));
                return new Timestamp(date.getTime());
            } else {
                SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyyMMddHHmm");
                Date date = simpleDateFormat.parse(value.substring(0, 12));
                return new Timestamp(date.getTime());
            }
        } catch (ParseException e) {
            throw new IllegalArgumentException("Unexpected time format : " + value + ".");
        }
    }

    public static List<String> getPredefinedFields(Map<String, String> head) {
        Map<Integer, String> predefinedFields = new HashMap<>();
        for (String key : head.keySet()) {
            if (!key.startsWith(INLONGMSG_ATTR_ADD_COLUMN_PREFIX)) {
                continue;
            }

            int index =
                    Integer.parseInt(
                            key.substring(
                                    INLONGMSG_ATTR_ADD_COLUMN_PREFIX.length(),
                                    key.indexOf('_', INLONGMSG_ATTR_ADD_COLUMN_PREFIX.length())));

            predefinedFields.put(index, head.get(key));
        }

        List<String> result = new ArrayList<>(predefinedFields.size());
        for (int i = 0; i < predefinedFields.size(); ++i) {
            String predefinedField = predefinedFields.get(i + 1);
            result.add(predefinedField);
        }

        return result;
    }

    /**
     * Creates the type information with given field names and data schema.
     *
     * @param dataRowFormatInfo   The schema of the data fields.
     * @param metadataKeys        The metadata keys.
     * @return The type information
     */
    public static TypeInformation<RowData> decorateRowTypeWithMetadata(
            RowFormatInfo dataRowFormatInfo,
            List<String> metadataKeys) {
        RowType rowType = (RowType) TableFormatUtils.deriveLogicalType(dataRowFormatInfo);
        return InLongMsgUtils.decorateRowTypeWithMetadata(
                rowType,
                metadataKeys);
    }

    /**
     * Creates the type information with given field names and data schema.
     *
     * @param dataRowType         The type of the data fields.
     * @param metadataKeys        The metadata keys.
     * @return The type information
     */
    public static TypeInformation<RowData> decorateRowTypeWithMetadata(
            RowType dataRowType,
            List<String> metadataKeys) {
        List<String> fieldNames = new ArrayList<>();
        List<LogicalType> fieldTypes = new ArrayList<>();

        // Physical fields
        List<RowType.RowField> fields = dataRowType.getFields();
        for (RowType.RowField field : fields) {
            fieldNames.add(field.getName());
            fieldTypes.add(field.getType());
        }

        // Metadata
        final List<InLongMsgMetadata.ReadableMetadata> readableMetadata = metadataKeys.stream()
                .map(k -> Stream.of(InLongMsgMetadata.ReadableMetadata.values())
                        .filter(rm -> rm.key.equals(k))
                        .findFirst()
                        .orElseThrow(IllegalStateException::new))
                .collect(Collectors.toList());

        for (InLongMsgMetadata.ReadableMetadata rm : readableMetadata) {
            // TODO : avoid name conflict?
            fieldNames.add(rm.key);
            fieldTypes.add(rm.dataType.getLogicalType());
        }

        RowType rowType = RowType.of(fieldTypes.toArray(new LogicalType[0]), fieldNames.toArray(new String[0]));

        return InternalTypeInfo.of(rowType);
    }

    public static TypeInformation<RowData> decorateRowDataTypeWithNeededHeadFields(@Nullable String timeFieldName,
            @Nullable String attributesFieldName,
            RowType dataRowType) {
        List<String> fieldNames = new ArrayList<>();
        List<LogicalType> fieldTypes = new ArrayList<>();

        // Timestamp and attribute field
        if (timeFieldName != null) {
            fieldNames.add(timeFieldName);
            fieldTypes.add(new TimestampType());
        }

        if (attributesFieldName != null) {
            fieldNames.add(attributesFieldName);
            fieldTypes.add(new MapType(new VarCharType(), new VarCharType()));
        }

        // Physical fields
        List<RowType.RowField> fields = dataRowType.getFields();
        for (RowType.RowField field : fields) {
            fieldNames.add(field.getName());
            fieldTypes.add(field.getType());
        }

        RowType rowType = RowType.of(fieldTypes.toArray(new LogicalType[0]), fieldNames.toArray(new String[0]));

        return InternalTypeInfo.of(rowType);
    }

    /**
     * Creates the type information with given field names and data schema.
     *
     * @param timeFieldName       The name of the time field. Null if the time field is not required.
     * @param attributesFieldName The name of the attributes field. Null if the attributes field is
     *                            not required.
     * @param dataRowFormatInfo   The schema of the data fields.
     * @param metadataKeys        The metadata keys.
     * @return The type information
     */
    public static TypeInformation<RowData> decorateRowTypeWithNeededHeadFieldsAndMetadata(
            @Nullable String timeFieldName,
            @Nullable String attributesFieldName,
            RowFormatInfo dataRowFormatInfo,
            List<String> metadataKeys) {
        RowType rowType = (RowType) TableFormatUtils.deriveLogicalType(dataRowFormatInfo);
        return InLongMsgUtils.decorateRowTypeWithNeededHeadFieldsAndMetadata(
                timeFieldName,
                attributesFieldName,
                rowType,
                metadataKeys);
    }

    /**
     * Creates the type information with given field names and data schema.
     *
     * @param timeFieldName       The name of the time field. Null if the time field is not required.
     * @param attributesFieldName The name of the attributes field. Null if the attributes field is
     *                            not required.
     * @param dataRowType         The type of the data fields.
     * @param metadataKeys        The metadata keys.
     * @return The type information
     */
    public static TypeInformation<RowData> decorateRowTypeWithNeededHeadFieldsAndMetadata(
            @Nullable String timeFieldName,
            @Nullable String attributesFieldName,
            RowType dataRowType,
            List<String> metadataKeys) {
        List<String> fieldNames = new ArrayList<>();
        List<LogicalType> fieldTypes = new ArrayList<>();

        // Timestamp and attribute field
        if (timeFieldName != null) {
            fieldNames.add(timeFieldName);
            fieldTypes.add(new TimestampType());
        }

        if (attributesFieldName != null) {
            fieldNames.add(attributesFieldName);
            fieldTypes.add(new MapType(new VarCharType(), new VarCharType()));
        }

        // Physical fields
        List<RowType.RowField> fields = dataRowType.getFields();
        for (RowType.RowField field : fields) {
            fieldNames.add(field.getName());
            fieldTypes.add(field.getType());
        }

        // Metadata
        final List<InLongMsgMetadata.ReadableMetadata> readableMetadata = metadataKeys.stream()
                .map(k -> Stream.of(InLongMsgMetadata.ReadableMetadata.values())
                        .filter(rm -> rm.key.equals(k))
                        .findFirst()
                        .orElseThrow(IllegalStateException::new))
                .collect(Collectors.toList());

        for (InLongMsgMetadata.ReadableMetadata rm : readableMetadata) {
            // TODO : avoid name conflict?
            fieldNames.add(rm.key);
            fieldTypes.add(rm.dataType.getLogicalType());
        }

        RowType rowType = RowType.of(fieldTypes.toArray(new LogicalType[0]), fieldNames.toArray(new String[0]));

        return InternalTypeInfo.of(rowType);
    }

    /**
     * Creates the row with the given head fields and data row.
     *
     * @param timeFieldName       The name of the time field. Null if the time field is not required.
     * @param attributesFieldName The name of the attributes field. Null if the attributes field is
     *                            not required.
     * @param time                The time of the inLongMsg.
     * @param attributes          The attributes of the inLongMsg.
     * @param dataRow             The data row.
     * @return The row decorated with head fields.
     */
    public static RowData decorateRowWithNeededHeadFields(
            @Nullable String timeFieldName,
            @Nullable String attributesFieldName,
            Timestamp time,
            Map<String, String> attributes,
            GenericRowData dataRow) {
        List<Object> headFields = new ArrayList<>();
        if (timeFieldName != null) {
            headFields.add(TIME_FIELD_CONVERTER.convert(time));
        }

        if (attributesFieldName != null) {
            headFields.add(ATTRIBUTES_FIELD_CONVERTER.convert(attributes));
        }

        GenericRowData rowData = new GenericRowData(headFields.size() + dataRow.getArity());
        for (int i = 0; i < headFields.size(); ++i) {
            rowData.setField(i, headFields.get(i));
        }

        for (int i = 0; i < dataRow.getArity(); ++i) {
            rowData.setField(i + headFields.size(), dataRow.getField(i));
        }

        return rowData;
    }

    public static GenericRowData decorateRowWithMetaData(
            GenericRowData physicalRowData,
            InLongMsgHead head,
            List<String> metadataKeys) {
        final int physicalArity = physicalRowData.getArity();
        final GenericRowData producedRow = new GenericRowData(
                physicalRowData.getRowKind(),
                physicalArity + metadataKeys.size());

        for (int i = 0; i < physicalArity; i++) {
            producedRow.setField(i, physicalRowData.getField(i));
        }

        for (int j = 0; j < metadataKeys.size(); j++) {
            if (metadataKeys.get(j).equals(STREAMID.getKey())) {
                producedRow.setField(physicalArity + j, StringData.fromString(head.getStreamId()));
            }
        }

        return producedRow;
    }

    public static GenericRowData decorateRowDataWithNeededHeadFields(
            @Nullable String timeFieldName,
            @Nullable String attributesFieldName,
            Timestamp time,
            Map<String, String> attributes,
            GenericRowData rowData) {
        List<Object> headFields = new ArrayList<>();
        if (timeFieldName != null) {
            headFields.add(TIME_FIELD_CONVERTER.convert(time));
        }

        if (attributesFieldName != null) {
            headFields.add(ATTRIBUTES_FIELD_CONVERTER.convert(attributes));
        }

        GenericRowData result = new GenericRowData(headFields.size() + rowData.getArity());
        for (int i = 0; i < headFields.size(); ++i) {
            result.setField(i, headFields.get(i));
        }

        for (int i = 0; i < rowData.getArity(); ++i) {
            result.setField(i + headFields.size(), rowData.getField(i));
        }

        return result;
    }

    @SuppressWarnings("unchecked")
    public static Map<String, String> getAttributesFromMixedRowData(RowData rowData) {
        return (Map<String, String>) ((GenericRowData) rowData).getField(0);
    }

    public static byte[] getDataFromMixedRowData(RowData rowData) {
        return (byte[]) ((GenericRowData) rowData).getField(1);
    }

    public static String getTidFromMixedRowData(RowData rowData) {
        return (String) ((GenericRowData) rowData).getField(2);
    }

    public static Timestamp getTimeFromMixedRowData(RowData rowData) {
        return (Timestamp) ((GenericRowData) rowData).getField(3);
    }

    @SuppressWarnings("unchecked")
    public static List<String> getPredefinedFieldsFromMixedRowData(RowData rowData) {
        return (List<String>) ((GenericRowData) rowData).getField(4);
    }

    @SuppressWarnings("unchecked")
    public static List<String> getFieldsFromMixedRowData(RowData rowData) {
        return (List<String>) ((GenericRowData) rowData).getField(5);
    }

    @SuppressWarnings("unchecked")
    public static Map<String, String> getEntriesFromMixedRowData(RowData rowData) {
        return (Map<String, String>) ((GenericRowData) rowData).getField(6);
    }

    public static FailureHandler getDefaultExceptionHandler(boolean ignoreErrors) {
        if (ignoreErrors) {
            return new IgnoreFailureHandler();
        } else {
            return new NoOpFailureHandler();
        }
    }

    public static void validateInLongMsgSchema(DescriptorProperties descriptorProperties) {
        validateSchema(descriptorProperties);

        descriptorProperties.validateString(FORMAT_TIME_FIELD_NAME, true, 1);
        descriptorProperties.validateString(FORMAT_ATTRIBUTE_FIELD_NAME, true, 1);
    }
}
