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

package org.apache.inlong.manager.service.sort.util;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.inlong.manager.common.enums.FieldType;
import org.apache.inlong.manager.common.enums.MetaFieldType;
import org.apache.inlong.manager.common.pojo.sink.SinkField;
import org.apache.inlong.manager.common.pojo.stream.StreamField;
import org.apache.inlong.sort.formats.common.ArrayFormatInfo;
import org.apache.inlong.sort.formats.common.BooleanFormatInfo;
import org.apache.inlong.sort.formats.common.ByteFormatInfo;
import org.apache.inlong.sort.formats.common.ByteTypeInfo;
import org.apache.inlong.sort.formats.common.DateFormatInfo;
import org.apache.inlong.sort.formats.common.DecimalFormatInfo;
import org.apache.inlong.sort.formats.common.DoubleFormatInfo;
import org.apache.inlong.sort.formats.common.FloatFormatInfo;
import org.apache.inlong.sort.formats.common.FormatInfo;
import org.apache.inlong.sort.formats.common.IntFormatInfo;
import org.apache.inlong.sort.formats.common.LongFormatInfo;
import org.apache.inlong.sort.formats.common.ShortFormatInfo;
import org.apache.inlong.sort.formats.common.StringFormatInfo;
import org.apache.inlong.sort.formats.common.TimeFormatInfo;
import org.apache.inlong.sort.formats.common.TimestampFormatInfo;
import org.apache.inlong.sort.protocol.FieldInfo;
import org.apache.inlong.sort.protocol.MetaFieldInfo;
import org.apache.inlong.sort.protocol.MetaFieldInfo.MetaField;
import org.apache.inlong.sort.protocol.transformation.FieldMappingRule.FieldMappingUnit;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Util for sort field info.
 */
@Slf4j
public class FieldInfoUtils {

    /**
     * Meta field map, key is meta field name, value is MetaField defined in  sort protocol
     */
    public static final Map<String, MetaField> META_FIELD_MAP = new LinkedHashMap<>();

    static {
        META_FIELD_MAP.put(MetaFieldType.DATABASE.getName(), MetaField.DATABASE_NAME);
        META_FIELD_MAP.put(MetaFieldType.TABLE.getName(), MetaField.TABLE_NAME);
        META_FIELD_MAP.put(MetaFieldType.EVENT_TIME.getName(), MetaField.OP_TS);
        META_FIELD_MAP.put(MetaFieldType.IS_DDL.getName(), MetaField.IS_DDL);
        META_FIELD_MAP.put(MetaFieldType.EVENT_TYPE.getName(), MetaField.OP_TYPE);
        META_FIELD_MAP.put(MetaFieldType.PROCESSING_TIME.getName(), MetaField.PROCESS_TIME);
        META_FIELD_MAP.put(MetaFieldType.UPDATE_BEFORE.getName(), MetaField.UPDATE_BEFORE);
        META_FIELD_MAP.put(MetaFieldType.BATCH_ID.getName(), MetaField.BATCH_ID);
        META_FIELD_MAP.put(MetaFieldType.SQL_TYPE.getName(), MetaField.SQL_TYPE);
        META_FIELD_MAP.put(MetaFieldType.TS.getName(), MetaField.TS);
        META_FIELD_MAP.put(MetaFieldType.MYSQL_TYPE.getName(), MetaField.MYSQL_TYPE);
        META_FIELD_MAP.put(MetaFieldType.PK_NAMES.getName(), MetaField.PK_NAMES);
        META_FIELD_MAP.put(MetaFieldType.DATA.getName(), MetaField.DATA);
    }

    public static FieldInfo parseSinkFieldInfo(SinkField sinkField, String nodeId) {
        boolean isMetaField = sinkField.getIsMetaField() == 1;
        FieldInfo fieldInfo = getFieldInfo(sinkField.getFieldName(),
                sinkField.getFieldType(),
                isMetaField, sinkField.getFieldFormat());
        fieldInfo.setNodeId(nodeId);
        return fieldInfo;
    }

    public static FieldInfo parseStreamFieldInfo(StreamField streamField, String nodeId) {
        boolean isMetaField = streamField.getIsMetaField() == 1;
        FieldInfo fieldInfo = getFieldInfo(streamField.getFieldName(), streamField.getFieldType(), isMetaField,
                streamField.getFieldFormat());
        fieldInfo.setNodeId(nodeId);
        return fieldInfo;
    }

    public static FieldInfo parseStreamField(StreamField streamField) {
        boolean isMetaField = streamField.getIsMetaField() == 1;
        FieldInfo fieldInfo = getFieldInfo(streamField.getFieldName(), streamField.getFieldType(), isMetaField,
                streamField.getFieldFormat());
        fieldInfo.setNodeId(streamField.getOriginNodeName());
        return fieldInfo;
    }

    /**
     * Get field info list.
     * TODO 1. Support partition field(not need to add index at 0), 2. Add is_metadata field in StreamSinkFieldEntity
     */
    public static List<FieldMappingUnit> createFieldInfo(
            List<StreamField> streamFieldList, List<SinkField> fieldList,
            List<FieldInfo> sourceFields, List<FieldInfo> sinkFields) {

        // Set source field info list.
        for (StreamField field : streamFieldList) {
            FieldInfo sourceField = getFieldInfo(field.getFieldName(), field.getFieldType(),
                    field.getIsMetaField() == 1, field.getFieldFormat());
            sourceFields.add(sourceField);
        }

        List<FieldMappingUnit> mappingUnitList = new ArrayList<>();
        // Get sink field info list, if the field name equals to build-in field, new a build-in field info
        for (SinkField field : fieldList) {
            FieldInfo sinkField = getFieldInfo(field.getFieldName(), field.getFieldType(),
                    field.getIsMetaField() == 1, field.getFieldFormat());
            sinkFields.add(sinkField);
            if (StringUtils.isNotBlank(field.getSourceFieldName())) {
                FieldInfo sourceField = getFieldInfo(field.getSourceFieldName(),
                        field.getSourceFieldType(), field.getIsMetaField() == 1, field.getFieldFormat());
                mappingUnitList.add(new FieldMappingUnit(sourceField, sinkField));
            }
        }

        return mappingUnitList;
    }

    /**
     * Get field info by the given field name ant type.
     *
     * @apiNote If the field name equals to build-in field, new a build-in field info
     */
    private static FieldInfo getFieldInfo(String fieldName, String fieldType, boolean isMetaField, String format) {
        MetaField metaField = META_FIELD_MAP.get(fieldName);
        FormatInfo formatInfo = convertFieldFormat(fieldType.toLowerCase(), format);
        if (isMetaField && metaField != null) {
            return new MetaFieldInfo(fieldName, metaField);
        } else {
            if (isMetaField) {
                // Check if fieldName contains metaFieldName, such as left_database
                // TODO The meta field needs to be selectable and cannot be filled in by the user
                for (String metaFieldName : META_FIELD_MAP.keySet()) {
                    if (fieldName.contains(metaFieldName)) {
                        metaField = META_FIELD_MAP.get(metaFieldName);
                        break;
                    }
                }
                if (metaField != null) {
                    return new MetaFieldInfo(fieldName, metaField);
                }
                log.warn("Unsupported metadata fieldName={} as the MetaField is null", fieldName);
            }
            return new FieldInfo(fieldName, formatInfo);
        }
    }

    /**
     * Get all migration field mapping unit list for binlog source.
     */
    public static List<FieldMappingUnit> setAllMigrationFieldMapping(List<FieldInfo> sourceFields,
                                                                     List<FieldInfo> sinkFields) {
        List<FieldMappingUnit> mappingUnitList = new ArrayList<>();
        MetaFieldInfo dataField = new MetaFieldInfo("data",
                MetaField.DATA);
        sourceFields.add(dataField);
        sinkFields.add(dataField);
        mappingUnitList.add(new FieldMappingUnit(dataField, dataField));

        for (Map.Entry<String, MetaField> entry : META_FIELD_MAP.entrySet()) {
            MetaFieldInfo fieldInfo = new MetaFieldInfo(entry.getKey(), entry.getValue());
            sourceFields.add(fieldInfo);
            sinkFields.add(fieldInfo);
            mappingUnitList.add(new FieldMappingUnit(fieldInfo, fieldInfo));
        }

        return mappingUnitList;
    }

    /**
     * Get the FieldFormat of Sort according to type string and format of field
     *
     * @param type type string
     * @return Sort field format instance
     */
    public static FormatInfo convertFieldFormat(String type) {
        return convertFieldFormat(type, null);
    }

    /**
     * Get the FieldFormat of Sort according to type string
     *
     * @param type type string
     * @return Sort field format instance
     */
    public static FormatInfo convertFieldFormat(String type, String format) {
        FormatInfo formatInfo;
        FieldType fieldType = FieldType.forName(type);
        switch (fieldType) {
            case BOOLEAN:
                formatInfo = new BooleanFormatInfo();
                break;
            case TINYINT:
            case BYTE:
                formatInfo = new ByteFormatInfo();
                break;
            case SMALLINT:
            case SHORT:
                formatInfo = new ShortFormatInfo();
                break;
            case INT:
                formatInfo = new IntFormatInfo();
                break;
            case BIGINT:
            case LONG:
                formatInfo = new LongFormatInfo();
                break;
            case FLOAT:
                formatInfo = new FloatFormatInfo();
                break;
            case DOUBLE:
                formatInfo = new DoubleFormatInfo();
                break;
            case DECIMAL:
                formatInfo = new DecimalFormatInfo();
                break;
            case DATE:
                if (StringUtils.isNotBlank(format)) {
                    formatInfo = new DateFormatInfo(convertToSortFormat(format));
                } else {
                    formatInfo = new DateFormatInfo();
                }
                break;
            case TIME:
                if (StringUtils.isNotBlank(format)) {
                    formatInfo = new TimeFormatInfo(convertToSortFormat(format));
                } else {
                    formatInfo = new TimeFormatInfo();
                }
                break;
            case TIMESTAMP:
                if (StringUtils.isNotBlank(format)) {
                    formatInfo = new TimestampFormatInfo(convertToSortFormat(format));
                } else {
                    formatInfo = new TimestampFormatInfo();
                }
                break;
            case BINARY:
            case FIXED:
                formatInfo = new ArrayFormatInfo(ByteTypeInfo::new);
                break;
            default: // default is string
                formatInfo = new StringFormatInfo();
        }

        return formatInfo;
    }

    /**
     * Convert to sort field format
     *
     * @param format The format
     * @return The sort format
     */
    private static String convertToSortFormat(String format) {
        String sortFormat = format;
        switch (format) {
            case "MICROSECONDS":
                sortFormat = "MICROS";
                break;
            case "MILLISECONDS":
                sortFormat = "MILLIS";
                break;
            case "SECONDS":
                sortFormat = "SECONDS";
                break;
            default:
        }
        return sortFormat;
    }

}
