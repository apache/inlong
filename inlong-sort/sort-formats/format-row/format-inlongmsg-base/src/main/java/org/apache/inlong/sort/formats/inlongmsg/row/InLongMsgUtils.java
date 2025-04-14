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

package org.apache.inlong.sort.formats.inlongmsg.row;

import org.apache.inlong.common.msg.InLongMsg;
import org.apache.inlong.common.pojo.sort.dataflow.field.format.FormatInfo;
import org.apache.inlong.common.pojo.sort.dataflow.field.format.RowFormatInfo;
import org.apache.inlong.sort.formats.base.TableFormatConstants;
import org.apache.inlong.sort.formats.base.TableFormatForRowUtils;
import org.apache.inlong.sort.formats.inlongmsg.FailureHandler;
import org.apache.inlong.sort.formats.inlongmsg.IgnoreFailureHandler;
import org.apache.inlong.sort.formats.inlongmsg.InLongMsgBody;
import org.apache.inlong.sort.formats.inlongmsg.InLongMsgHead;
import org.apache.inlong.sort.formats.inlongmsg.NoOpFailureHandler;
import org.apache.inlong.sort.formats.util.StringUtils;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.descriptors.DescriptorProperties;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.Row;

import javax.annotation.Nullable;

import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import static org.apache.flink.table.factories.TableFormatFactoryBase.deriveSchema;
import static org.apache.inlong.sort.formats.base.TableFormatUtils.deserializeRowFormatInfo;
import static org.apache.inlong.sort.formats.base.TableFormatUtils.validateSchema;

/**
 * A utility class for parsing InLongMsg {@link InLongMsg}.
 */
public class InLongMsgUtils {

    public static final char INLONGMSG_ATTR_ENTRY_DELIMITER = '&';
    public static final char INLONGMSG_ATTR_KV_DELIMITER = '=';

    // keys in attributes
    public static final String INLONGMSG_ATTR_STREAM_ID = "streamId";

    @Deprecated
    public static final String INLONGMSG_ATTR_INTERFACE_NAME = "iname";
    @Deprecated
    public static final String INLONGMSG_ATTR_TID = "tid";
    public static final String INLONGMSG_ATTR_TIME_T = "t";
    public static final String INLONGMSG_ATTR_TIME_DT = "dt";
    public static final String INLONGMSG_ATTR_ADD_COLUMN_PREFIX = "__addcol";

    public static final String FORMAT_TIME_FIELD_NAME = "format.time-field-name";
    public static final String FORMAT_ATTRIBUTES_FIELD_NAME = "format.attributes-field-name";
    public static final String FORMAT_RETAIN_PREDEFINED_FIELD = "format.retain-predefined-field";
    public static final String FORMAT_APPEND_ESCAPE_FIELD = "format.append-escape";

    public static final String DEFAULT_TIME_FIELD_NAME = "inlongmsg_time";
    public static final String DEFAULT_ATTRIBUTES_FIELD_NAME = "inlongmsg_attributes";
    public static final boolean DEFAULT_PREDEFINED_FIELD = true;
    public static final boolean DEFAULT_APPEND_ESCAPE = false;

    public static final TypeInformation<Row> MIXED_ROW_TYPE =
            Types.ROW_NAMED(
                    new String[]{
                            "attributes",
                            "data",
                            "streamId",
                            "time",
                            "predefinedFields",
                            "fields",
                            "entries"
                    },
                    Types.MAP(Types.STRING, Types.STRING),
                    Types.PRIMITIVE_ARRAY(Types.BYTE),
                    Types.STRING,
                    Types.SQL_TIMESTAMP,
                    Types.LIST(Types.STRING),
                    Types.LIST(Types.STRING),
                    Types.MAP(Types.STRING, Types.STRING));

    public static RowFormatInfo getDataFormatInfo(
            DescriptorProperties descriptorProperties) {
        if (descriptorProperties.containsKey(TableFormatConstants.FORMAT_SCHEMA)) {
            return deserializeRowFormatInfo(descriptorProperties);
        } else {
            TableSchema tableSchema =
                    deriveSchema(descriptorProperties.asMap());

            String[] fieldNames = tableSchema.getFieldNames();
            DataType[] fieldTypes = tableSchema.getFieldDataTypes();

            String[] dataFieldNames = new String[fieldNames.length - 2];
            FormatInfo[] dataFieldFormatInfos = new FormatInfo[fieldNames.length - 2];

            for (int i = 0; i < dataFieldNames.length; ++i) {
                dataFieldNames[i] = fieldNames[i + 2];
                dataFieldFormatInfos[i] =
                        TableFormatForRowUtils.deriveFormatInfo(fieldTypes[i + 2].getLogicalType());
            }

            return new RowFormatInfo(dataFieldNames, dataFieldFormatInfos);
        }
    }

    public static RowFormatInfo getDataRowFormatInfo(
            DescriptorProperties descriptorProperties) {
        RowFormatInfo rowFormatInfo = deserializeRowFormatInfo(descriptorProperties);

        Set<String> fieldNames = new HashSet<>();
        Collections.addAll(fieldNames, rowFormatInfo.getFieldNames());

        String timeFieldName =
                descriptorProperties
                        .getOptionalString(FORMAT_TIME_FIELD_NAME)
                        .orElse(DEFAULT_TIME_FIELD_NAME);
        if (timeFieldName != null && fieldNames.contains(timeFieldName)) {
            throw new ValidationException("The name of the time field " + timeFieldName +
                    " conflicts with one of the data fields.");
        }

        String attributesFieldName =
                descriptorProperties
                        .getOptionalString(FORMAT_ATTRIBUTES_FIELD_NAME)
                        .orElse(DEFAULT_ATTRIBUTES_FIELD_NAME);
        if (attributesFieldName != null && fieldNames.contains(attributesFieldName)) {
            throw new ValidationException("The name of the attributes field " +
                    attributesFieldName + " conflicts with one of the data fields.");
        }

        return rowFormatInfo;
    }

    /**
     * Parse head of message.
     */
    public static InLongMsgHead parseHead(String attr) {
        Map<String, String> attributes = parseAttr(attr);

        // Extracts interface from the attributes.
        String streamId;

        if (attributes.containsKey(INLONGMSG_ATTR_STREAM_ID)) {
            streamId = attributes.get(INLONGMSG_ATTR_STREAM_ID);
        } else if (attributes.containsKey(INLONGMSG_ATTR_TID)) {
            streamId = attributes.get(INLONGMSG_ATTR_TID);
        } else {
            throw new IllegalArgumentException("Could not find " + INLONGMSG_ATTR_STREAM_ID + " in attributes!");
        }

        // Extracts time from the attributes
        Timestamp time;

        if (attributes.containsKey(INLONGMSG_ATTR_TIME_T)) {
            String date = attributes.get(INLONGMSG_ATTR_TIME_T).trim();
            time = parseDateTime(date);
        } else if (attributes.containsKey(INLONGMSG_ATTR_TIME_DT)) {
            String epoch = attributes.get(INLONGMSG_ATTR_TIME_DT).trim();
            time = parseEpochTime(epoch);
        } else {
            throw new IllegalArgumentException(
                    "Could not find " + INLONGMSG_ATTR_TIME_T
                            + " or " + INLONGMSG_ATTR_TIME_DT + " in attributes!");
        }

        // Extracts predefined fields from the attributes
        List<String> predefinedFields = getPredefinedFields(attributes);

        return new InLongMsgHead(attributes, streamId, time, predefinedFields);
    }

    public static Map<String, String> parseAttr(String attr) {
        return StringUtils.splitKv(
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
     *         {yyyyMMdd, yyyyMMddHH, yyyyMMddHHmm}.
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

    public static Row buildMixedRow(
            InLongMsgHead head,
            InLongMsgBody body,
            String streamId) {
        Row row = new Row(7);
        row.setField(0, head.getAttributes());
        row.setField(1, body.getData());
        row.setField(2, streamId);
        row.setField(3, head.getTime());
        row.setField(4, head.getPredefinedFields());
        row.setField(5, body.getFields());
        row.setField(6, body.getEntries());

        return row;
    }

    @SuppressWarnings("unchecked")
    public static Map<String, String> getAttributesFromMixedRow(Row row) {
        return (Map<String, String>) row.getField(0);
    }

    public static byte[] getDataFromMixedRow(Row row) {
        return (byte[]) row.getField(1);
    }

    public static String getStreamIdFromMixedRow(Row row) {
        return (String) row.getField(2);
    }

    public static Timestamp getTimeFromMixedRow(Row row) {
        return (Timestamp) row.getField(3);
    }

    @SuppressWarnings("unchecked")
    public static List<String> getPredefinedFieldsFromMixedRow(Row row) {
        return (List<String>) row.getField(4);
    }

    @SuppressWarnings("unchecked")
    public static List<String> getFieldsFromMixedRow(Row row) {
        return (List<String>) row.getField(5);
    }

    @SuppressWarnings("unchecked")
    public static Map<String, String> getEntriesFromMixedRow(Row row) {
        return (Map<String, String>) row.getField(6);
    }

    /**
     * Validates that the names of the time and attributes field do no conflict
     * with the names of data fields.
     */
    public static void validateFieldNames(
            String timeFieldName,
            String attributesFieldName,
            RowFormatInfo rowFormatInfo) {
        if (Objects.equals(timeFieldName, attributesFieldName)) {
            throw new ValidationException("The names of the time and attributes fields are same.");
        }

        String[] dataFieldNames = rowFormatInfo.getFieldNames();
        for (String dataFieldName : dataFieldNames) {
            if (dataFieldName.equals(timeFieldName)) {
                throw new ValidationException("The time field " + timeFieldName + " is already defined in the schema.");
            }

            if (dataFieldName.equals(attributesFieldName)) {
                throw new ValidationException("The attributes field "
                        + attributesFieldName + " is already defined in the schema.");
            }
        }
    }

    public static void validateInLongMsgSchema(DescriptorProperties descriptorProperties) {
        validateSchema(descriptorProperties);

        descriptorProperties.validateString(FORMAT_TIME_FIELD_NAME, true, 1);
        descriptorProperties.validateString(FORMAT_ATTRIBUTES_FIELD_NAME, true, 1);
    }

    /**
     * Creates the type information with given field names and data schema.
     *
     * @param timeFieldName The name of the time field.
     * @param attributesFieldName The name of the attributes field.
     * @param rowFormatInfo The schema of data fields.
     * @return The type information
     */
    public static TypeInformation<Row> buildRowType(
            String timeFieldName,
            String attributesFieldName,
            RowFormatInfo rowFormatInfo) {
        String[] dataFieldNames = rowFormatInfo.getFieldNames();
        String[] fieldNames = new String[dataFieldNames.length + 2];
        fieldNames[0] = timeFieldName;
        fieldNames[1] = attributesFieldName;
        System.arraycopy(dataFieldNames, 0, fieldNames, 2, dataFieldNames.length);

        FormatInfo[] dataFieldFormatInfos = rowFormatInfo.getFieldFormatInfos();
        TypeInformation<?>[] fieldTypes =
                new TypeInformation<?>[dataFieldFormatInfos.length + 2];
        fieldTypes[0] = Types.SQL_TIMESTAMP;
        fieldTypes[1] = Types.MAP(Types.STRING, Types.STRING);

        for (int i = 0; i < dataFieldFormatInfos.length; ++i) {
            fieldTypes[i + 2] = TableFormatForRowUtils.getType(dataFieldFormatInfos[i].getTypeInfo());
        }

        return Types.ROW_NAMED(fieldNames, fieldTypes);
    }

    public static FailureHandler getDefaultExceptionHandler(boolean ignoreErrors) {
        if (ignoreErrors) {
            return new IgnoreFailureHandler();
        } else {
            return new NoOpFailureHandler();
        }
    }

    /**
     * Creates the type information with given field names and data schema.
     *
     * @param timeFieldName The name of the time field. Null if the time field is not required.
     * @param attributesFieldName The name of the attributes field. Null if the attributes field is
     *                               not required.
     * @param dataRowFormatInfo The schema of the data fields.
     * @return The type information
     */
    public static TypeInformation<Row> decorateRowTypeWithNeededHeadFields(
            @Nullable String timeFieldName,
            @Nullable String attributesFieldName,
            RowFormatInfo dataRowFormatInfo) {
        RowTypeInfo rowTypeInfo = (RowTypeInfo) TableFormatForRowUtils.getType(dataRowFormatInfo.getTypeInfo());

        return InLongMsgUtils.decorateRowTypeWithNeededHeadFields(
                timeFieldName,
                attributesFieldName,
                rowTypeInfo);
    }

    /**
     * Creates the type information with given field names and data schema.
     *
     * @param timeFieldName The name of the time field. Null if the time field is not required.
     * @param attributesFieldName The name of the attributes field. Null if the attributes field is
     *                            not required.
     * @param dataRowType The type of the data fields.
     * @return The type information
     */
    public static TypeInformation<Row> decorateRowTypeWithNeededHeadFields(
            @Nullable String timeFieldName,
            @Nullable String attributesFieldName,
            RowTypeInfo dataRowType) {
        List<String> fieldNames = new ArrayList<>();
        List<TypeInformation<?>> fieldTypes = new ArrayList<>();

        if (timeFieldName != null) {
            fieldNames.add(timeFieldName);
            fieldTypes.add(Types.SQL_TIMESTAMP);
        }

        if (attributesFieldName != null) {
            fieldNames.add(attributesFieldName);
            fieldTypes.add(Types.MAP(Types.STRING, Types.STRING));
        }

        String[] dataFieldNames = dataRowType.getFieldNames();
        TypeInformation<?>[] dataFieldTypes = dataRowType.getFieldTypes();

        for (int i = 0; i < dataFieldNames.length; ++i) {
            fieldNames.add(dataFieldNames[i]);
            fieldTypes.add(dataFieldTypes[i]);
        }

        return Types.ROW_NAMED(
                fieldNames.toArray(new String[0]),
                fieldTypes.toArray(new TypeInformation[0]));
    }

    /**
     * Creates the row with the given head fields and data row.
     *
     * @param timeFieldName The name of the time field. Null if the time field is not required.
     * @param attributesFieldName The name of the attributes field. Null if the attributes field is
     *                            not required.
     * @param time The time of the InLongMsg.
     * @param attributes The attributes of the InLongMsg.
     * @param dataRow The data row.
     * @return The row decorated with head fields.
     */
    public static Row decorateRowWithNeededHeadFields(
            @Nullable String timeFieldName,
            @Nullable String attributesFieldName,
            Timestamp time,
            Map<String, String> attributes,
            Row dataRow) {
        List<Object> headFields = new ArrayList<>(2);
        if (timeFieldName != null) {
            headFields.add(time);
        }

        if (attributesFieldName != null) {
            headFields.add(attributes);
        }

        Row row = new Row(headFields.size() + dataRow.getArity());
        for (int i = 0; i < headFields.size(); ++i) {
            row.setField(i, headFields.get(i));
        }

        for (int i = 0; i < dataRow.getArity(); ++i) {
            row.setField(i + headFields.size(), dataRow.getField(i));
        }

        return row;
    }
}
