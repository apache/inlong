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

package org.apache.inlong.sort.formats.inlongmsgbinlog;

import org.apache.inlong.common.pojo.sort.dataflow.field.format.FormatInfo;
import org.apache.inlong.common.pojo.sort.dataflow.field.format.RowFormatInfo;
import org.apache.inlong.sort.formats.binlog.InLongBinlog;
import org.apache.inlong.sort.formats.inlongmsg.InLongMsgBody;
import org.apache.inlong.sort.formats.inlongmsg.InLongMsgHead;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.descriptors.DescriptorProperties;
import org.apache.flink.types.Row;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.inlong.sort.formats.base.TableFormatUtils.deserializeBasicField;
import static org.apache.inlong.sort.formats.base.TableFormatUtils.deserializeRowFormatInfo;
import static org.apache.inlong.sort.formats.base.TableFormatUtils.getType;
import static org.apache.inlong.sort.formats.inlongmsg.row.InLongMsgUtils.DEFAULT_ATTRIBUTES_FIELD_NAME;
import static org.apache.inlong.sort.formats.inlongmsg.row.InLongMsgUtils.DEFAULT_TIME_FIELD_NAME;
import static org.apache.inlong.sort.formats.inlongmsg.row.InLongMsgUtils.FORMAT_ATTRIBUTES_FIELD_NAME;
import static org.apache.inlong.sort.formats.inlongmsg.row.InLongMsgUtils.FORMAT_TIME_FIELD_NAME;
import static org.apache.inlong.sort.formats.inlongmsg.row.InLongMsgUtils.INLONGMSG_ATTR_INTERFACE_NAME;
import static org.apache.inlong.sort.formats.inlongmsg.row.InLongMsgUtils.INLONGMSG_ATTR_STREAM_ID;
import static org.apache.inlong.sort.formats.inlongmsg.row.InLongMsgUtils.INLONGMSG_ATTR_TID;
import static org.apache.inlong.sort.formats.inlongmsg.row.InLongMsgUtils.getPredefinedFields;
import static org.apache.inlong.sort.formats.inlongmsg.row.InLongMsgUtils.parseAttr;
import static org.apache.inlong.sort.formats.inlongmsg.row.InLongMsgUtils.parseEpochTime;

public class InLongMsgBinlogUtils {

    public static final String FORMAT_METADATA_FIELD_NAME = "format.metadata-field-name";
    public static final String FORMAT_INCLUDE_UPDATE_BEFORE = "format.include-update-before";
    public static final String DEFAULT_METADATA_FIELD_NAME = "dbsync_metadata";
    public static final boolean DEFAULT_INCLUDE_UPDATE_BEFORE = false;

    public static final String METADATA_OPERATION_TYPE = "dbsync_operation_type";
    public static final String METADATA_INSTANCE_NAME = "dbsync_instance_name";
    public static final String METADATA_SCHEMA_NAME = "dbsync_schema_name";
    public static final String METADATA_TABLE_NAME = "dbsync_table_name";
    public static final String METADATA_EXECUTE_TIME = "dbsync_execute_time";
    public static final String METADATA_EXECUTE_ORDER = "dbsync_execute_order";
    public static final String METADATA_TRANSFER_IP = "dbsync_transfer_ip";

    public static final String DBSYNC_OPERATION_INERT = "I";
    public static final String DBSYNC_OPERATION_UPDATE = "U";
    public static final String DBSYNC_OPERATION_UPDATE_BEFORE = "UB";
    public static final String DBSYNC_OPERATION_DELETE = "D";

    public static RowFormatInfo getDataRowFormatInfo(DescriptorProperties descriptorProperties) {
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

        String metadataFieldName =
                descriptorProperties
                        .getOptionalString(FORMAT_METADATA_FIELD_NAME)
                        .orElse(DEFAULT_METADATA_FIELD_NAME);
        if (metadataFieldName != null && fieldNames.contains(metadataFieldName)) {
            throw new ValidationException("The name of the metadata field " +
                    metadataFieldName + " conflicts with one of the data fields.");
        }

        return rowFormatInfo;
    }

    public static InLongMsgHead parseHead(String attr) {
        Map<String, String> attributes = parseAttr(attr);

        // Extracts interface from the attributes.
        String streamId;
        if (attributes.containsKey(INLONGMSG_ATTR_STREAM_ID)) {
            streamId = attributes.get(INLONGMSG_ATTR_STREAM_ID);
        } else if (attributes.containsKey(INLONGMSG_ATTR_TID)) {
            streamId = attributes.get(INLONGMSG_ATTR_TID);
        } else if (attributes.containsKey(INLONGMSG_ATTR_INTERFACE_NAME)) {
            streamId = attributes.get(INLONGMSG_ATTR_INTERFACE_NAME);
        } else {
            throw new IllegalArgumentException(
                    "Could not find " + INLONGMSG_ATTR_STREAM_ID
                            + " or " + INLONGMSG_ATTR_TID
                            + " or " + INLONGMSG_ATTR_INTERFACE_NAME
                            + " in attributes!");
        }

        // Extracts time from the attributes
        Timestamp time = parseEpochTime(Long.toString(System.currentTimeMillis()));
        List<String> predefinedFields = getPredefinedFields(attributes);

        return new InLongMsgHead(attributes, streamId, time, predefinedFields);
    }

    public static InLongMsgBody parseBody(byte[] bytes) {
        return new InLongMsgBody(
                bytes,
                null,
                null,
                Collections.emptyList(),
                Collections.emptyMap());
    }

    public static TypeInformation<Row> getRowType(
            RowFormatInfo rowFormatInfo,
            String timeFieldName,
            String attributesFieldName,
            String metadataFieldName) {
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

        if (metadataFieldName != null) {
            fieldNames.add(metadataFieldName);
            fieldTypes.add(Types.MAP(Types.STRING, Types.STRING));
        }

        String[] dataFieldNames = rowFormatInfo.getFieldNames();
        FormatInfo[] dataFieldFormatInfos = rowFormatInfo.getFieldFormatInfos();

        for (int i = 0; i < dataFieldNames.length; ++i) {
            fieldNames.add(dataFieldNames[i]);
            fieldTypes.add(getType(dataFieldFormatInfos[i].getTypeInfo()));
        }

        return Types.ROW_NAMED(
                fieldNames.toArray(new String[0]),
                fieldTypes.toArray(new TypeInformation<?>[0]));
    }

    public static List<Row> getRows(
            RowFormatInfo rowFormatInfo,
            String timeFieldName,
            String attributesFieldName,
            String metadataFieldName,
            Map<String, String> attributes,
            byte[] bytes,
            boolean includeUpdateBefore) throws Exception {

        InLongBinlog.RowData rowData = InLongBinlog.RowData.parseFrom(bytes);

        List<Row> rows = new ArrayList<>();

        switch (rowData.getEventType()) {
            case INSERT:
                rows.add(
                        constructRow(
                                rowFormatInfo,
                                timeFieldName,
                                attributesFieldName,
                                metadataFieldName,
                                attributes,
                                rowData,
                                DBSYNC_OPERATION_INERT,
                                rowData.getAfterColumnsList()));
                break;
            case UPDATE:
                if (includeUpdateBefore) {
                    rows.add(
                            constructRow(
                                    rowFormatInfo,
                                    timeFieldName,
                                    attributesFieldName,
                                    metadataFieldName,
                                    attributes,
                                    rowData,
                                    DBSYNC_OPERATION_UPDATE_BEFORE,
                                    rowData.getBeforeColumnsList()));
                }
                rows.add(
                        constructRow(
                                rowFormatInfo,
                                timeFieldName,
                                attributesFieldName,
                                metadataFieldName,
                                attributes,
                                rowData,
                                DBSYNC_OPERATION_UPDATE,
                                rowData.getAfterColumnsList()));
                break;
            case DELETE:
                rows.add(
                        constructRow(
                                rowFormatInfo,
                                timeFieldName,
                                attributesFieldName,
                                metadataFieldName,
                                attributes,
                                rowData,
                                DBSYNC_OPERATION_DELETE,
                                rowData.getBeforeColumnsList()));
                break;
            default:
                return null;
        }

        return rows;
    }

    private static Row constructRow(
            RowFormatInfo rowFormatInfo,
            String timeFieldName,
            String attributesFieldName,
            String metadataFieldName,
            Map<String, String> attributes,
            InLongBinlog.RowData rowData,
            String operation,
            List<InLongBinlog.Column> columns) throws Exception {
        List<Object> headFields = new ArrayList<>();

        if (timeFieldName != null) {
            headFields.add(new Timestamp(rowData.getExecuteTime()));
        }

        if (attributesFieldName != null) {
            headFields.add(attributes);
        }

        if (metadataFieldName != null) {
            Map<String, String> metadata = new HashMap<>();

            metadata.put(METADATA_INSTANCE_NAME, rowData.getInstanceName());
            metadata.put(METADATA_SCHEMA_NAME, rowData.getSchemaName());
            metadata.put(METADATA_TABLE_NAME, rowData.getTableName());
            metadata.put(METADATA_OPERATION_TYPE, operation);
            metadata.put(METADATA_EXECUTE_TIME, Long.toString(rowData.getExecuteTime()));
            metadata.put(METADATA_EXECUTE_ORDER, Long.toString(rowData.getExecuteOrder()));
            metadata.put(METADATA_TRANSFER_IP, rowData.getTransferIp());

            headFields.add(metadata);
        }

        String[] dataFieldNames = rowFormatInfo.getFieldNames();
        FormatInfo[] dataFieldFormatInfos = rowFormatInfo.getFieldFormatInfos();

        Row row = new Row(dataFieldNames.length + headFields.size());

        for (int i = 0; i < headFields.size(); ++i) {
            row.setField(i, headFields.get(i));
        }

        Map<String, String> columnMap = new HashMap<>(columns.size());
        for (InLongBinlog.Column column : columns) {
            if (column.getIsNull()) {
                columnMap.put(column.getName(), null);
            } else {
                columnMap.put(column.getName(), column.getValue());
            }
        }

        for (int i = 0; i < dataFieldNames.length; ++i) {
            String fieldName = dataFieldNames[i];
            String fieldText = columnMap.get(fieldName);

            if (fieldText == null) {
                row.setField(i + headFields.size(), null);
            } else {
                Object field =
                        deserializeBasicField(
                                fieldName,
                                dataFieldFormatInfos[i],
                                fieldText,
                                null, null);
                row.setField(i + headFields.size(), field);
            }
        }

        return row;
    }
}
