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
import org.apache.inlong.sort.formats.base.FormatMsg;
import org.apache.inlong.sort.formats.base.TableFormatUtils;
import org.apache.inlong.sort.formats.binlog.InLongBinlog;
import org.apache.inlong.sort.formats.inlongmsg.FailureHandler;
import org.apache.inlong.sort.formats.inlongmsg.InLongMsgBody;
import org.apache.inlong.sort.formats.inlongmsg.InLongMsgHead;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.MapType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.logical.VarCharType;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.inlong.sort.formats.base.TableFormatUtils.deserializeBasicField;
import static org.apache.inlong.sort.formats.base.TableFormatUtils.getFormatValueLength;
import static org.apache.inlong.sort.formats.inlongmsg.InLongMsgUtils.INLONGMSG_ATTR_INTERFACE_ID;
import static org.apache.inlong.sort.formats.inlongmsg.InLongMsgUtils.INLONGMSG_ATTR_INTERFACE_NAME;
import static org.apache.inlong.sort.formats.inlongmsg.InLongMsgUtils.INLONGMSG_ATTR_INTERFACE_TID;
import static org.apache.inlong.sort.formats.inlongmsg.InLongMsgUtils.INLONGMSG_ATTR_STREAM_ID;
import static org.apache.inlong.sort.formats.inlongmsg.InLongMsgUtils.getPredefinedFields;
import static org.apache.inlong.sort.formats.inlongmsg.InLongMsgUtils.parseAttr;
import static org.apache.inlong.sort.formats.inlongmsg.InLongMsgUtils.parseEpochTime;

/**
 * Utilities for InLongMsgBinlog.
 */
public class InLongMsgBinlogUtils {

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

    public static InLongMsgHead parseHead(String attr) {
        Map<String, String> attributes = parseAttr(attr);

        // Extracts interface from the attributes.
        String streamId;
        if (attributes.containsKey(INLONGMSG_ATTR_STREAM_ID)) {
            streamId = attributes.get(INLONGMSG_ATTR_STREAM_ID);
        } else if (attributes.containsKey(INLONGMSG_ATTR_INTERFACE_TID)) {
            streamId = attributes.get(INLONGMSG_ATTR_INTERFACE_TID);
        } else if (attributes.containsKey(INLONGMSG_ATTR_INTERFACE_NAME)) {
            streamId = attributes.get(INLONGMSG_ATTR_INTERFACE_NAME);
        } else if (attributes.containsKey(INLONGMSG_ATTR_INTERFACE_ID)) {
            streamId = attributes.get(INLONGMSG_ATTR_INTERFACE_ID);
        } else {
            throw new IllegalArgumentException(
                    "Could not find " + INLONGMSG_ATTR_STREAM_ID
                            + " or " + INLONGMSG_ATTR_INTERFACE_TID
                            + " or " + INLONGMSG_ATTR_INTERFACE_NAME
                            + " or " + INLONGMSG_ATTR_INTERFACE_ID + " in attributes!");
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
                Collections.emptyList(),
                Collections.emptyMap());
    }

    public static TypeInformation<RowData> getRowType(
            RowFormatInfo rowFormatInfo,
            String timeFieldName,
            String attributesFieldName,
            String metadataFieldName) {
        List<String> fieldNames = new ArrayList<>();
        List<LogicalType> fieldTypes = new ArrayList<>();

        if (timeFieldName != null) {
            fieldNames.add(timeFieldName);
            fieldTypes.add(new TimestampType());
        }

        if (attributesFieldName != null) {
            fieldNames.add(attributesFieldName);
            fieldTypes.add(new MapType(new VarCharType(), new VarCharType()));
        }

        if (metadataFieldName != null) {
            fieldNames.add(metadataFieldName);
            fieldTypes.add(new MapType(new VarCharType(), new VarCharType()));
        }
        RowType dataRowType = (RowType) TableFormatUtils.deriveLogicalType(rowFormatInfo);
        List<RowType.RowField> fields = dataRowType.getFields();
        for (RowType.RowField field : fields) {
            fieldNames.add(field.getName());
            fieldTypes.add(field.getType());
        }

        RowType rowType = RowType.of(fieldTypes.toArray(new LogicalType[0]), fieldNames.toArray(new String[0]));
        return InternalTypeInfo.of(rowType);
    }

    public static List<RowData> getRowData(
            RowFormatInfo rowFormatInfo,
            String timeFieldName,
            String attributesFieldName,
            String metadataFieldName,
            Map<String, String> attributes,
            byte[] bytes,
            boolean includeUpdateBefore, FailureHandler failureHandler) throws Exception {

        InLongBinlog.RowData rowData = InLongBinlog.RowData.parseFrom(bytes);

        List<RowData> rows = new ArrayList<>();

        switch (rowData.getEventType()) {
            case INSERT:
                rows.add(
                        constructRowData(
                                rowFormatInfo,
                                timeFieldName,
                                attributesFieldName,
                                metadataFieldName,
                                attributes,
                                rowData,
                                DBSYNC_OPERATION_INERT,
                                rowData.getAfterColumnsList(),
                                failureHandler));
                break;
            case UPDATE:
                if (includeUpdateBefore) {
                    rows.add(
                            constructRowData(
                                    rowFormatInfo,
                                    timeFieldName,
                                    attributesFieldName,
                                    metadataFieldName,
                                    attributes,
                                    rowData,
                                    DBSYNC_OPERATION_UPDATE_BEFORE,
                                    rowData.getBeforeColumnsList(),
                                    failureHandler));
                }
                rows.add(
                        constructRowData(
                                rowFormatInfo,
                                timeFieldName,
                                attributesFieldName,
                                metadataFieldName,
                                attributes,
                                rowData,
                                DBSYNC_OPERATION_UPDATE,
                                rowData.getAfterColumnsList(),
                                failureHandler));
                break;
            case DELETE:
                rows.add(
                        constructRowData(
                                rowFormatInfo,
                                timeFieldName,
                                attributesFieldName,
                                metadataFieldName,
                                attributes,
                                rowData,
                                DBSYNC_OPERATION_DELETE,
                                rowData.getBeforeColumnsList(),
                                failureHandler));
                break;
            default:
                return null;
        }

        return rows;
    }

    private static RowData constructRowData(
            RowFormatInfo rowFormatInfo,
            String timeFieldName,
            String attributesFieldName,
            String metadataFieldName,
            Map<String, String> attributes,
            InLongBinlog.RowData rowData,
            String operation,
            List<InLongBinlog.Column> columns, FailureHandler failureHandler) throws Exception {
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

        GenericRowData row = new GenericRowData(dataFieldNames.length + headFields.size());

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
                                null, failureHandler);
                row.setField(i + headFields.size(), field);
            }
        }

        return row;
    }

    public static List<FormatMsg> getFormatMsgData(
            RowFormatInfo rowFormatInfo,
            String timeFieldName,
            String attributesFieldName,
            String metadataFieldName,
            Map<String, String> attributes,
            byte[] bytes,
            boolean includeUpdateBefore, FailureHandler failureHandler) throws Exception {

        InLongBinlog.RowData rowData = InLongBinlog.RowData.parseFrom(bytes);

        List<FormatMsg> rows = new ArrayList<>();

        switch (rowData.getEventType()) {
            case INSERT:
                rows.add(
                        constructFormatMsgData(
                                rowFormatInfo,
                                timeFieldName,
                                attributesFieldName,
                                metadataFieldName,
                                attributes,
                                rowData,
                                DBSYNC_OPERATION_INERT,
                                rowData.getAfterColumnsList(),
                                failureHandler));
                break;
            case UPDATE:
                if (includeUpdateBefore) {
                    rows.add(
                            constructFormatMsgData(
                                    rowFormatInfo,
                                    timeFieldName,
                                    attributesFieldName,
                                    metadataFieldName,
                                    attributes,
                                    rowData,
                                    DBSYNC_OPERATION_UPDATE_BEFORE,
                                    rowData.getBeforeColumnsList(),
                                    failureHandler));
                }
                rows.add(
                        constructFormatMsgData(
                                rowFormatInfo,
                                timeFieldName,
                                attributesFieldName,
                                metadataFieldName,
                                attributes,
                                rowData,
                                DBSYNC_OPERATION_UPDATE,
                                rowData.getAfterColumnsList(),
                                failureHandler));
                break;
            case DELETE:
                rows.add(
                        constructFormatMsgData(
                                rowFormatInfo,
                                timeFieldName,
                                attributesFieldName,
                                metadataFieldName,
                                attributes,
                                rowData,
                                DBSYNC_OPERATION_DELETE,
                                rowData.getBeforeColumnsList(),
                                failureHandler));
                break;
            default:
                return null;
        }

        return rows;
    }

    private static FormatMsg constructFormatMsgData(
            RowFormatInfo rowFormatInfo,
            String timeFieldName,
            String attributesFieldName,
            String metadataFieldName,
            Map<String, String> attributes,
            InLongBinlog.RowData rowData,
            String operation,
            List<InLongBinlog.Column> columns, FailureHandler failureHandler) throws Exception {
        List<Object> headFields = new ArrayList<>();
        long rowDataLength = 0L;
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

        GenericRowData row = new GenericRowData(dataFieldNames.length + headFields.size());

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
                                null, failureHandler);
                row.setField(i + headFields.size(), field);
                rowDataLength += getFormatValueLength(dataFieldFormatInfos[i], fieldText);
            }
        }

        return new FormatMsg(row, rowDataLength);
    }
}
