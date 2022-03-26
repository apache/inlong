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

package org.apache.inlong.manager.service.thirdparty.sort.util;

import org.apache.commons.lang3.StringUtils;
import org.apache.inlong.manager.common.enums.FieldType;
import org.apache.inlong.manager.common.enums.MetaFieldType;
import org.apache.inlong.manager.common.pojo.sink.SinkFieldResponse;
import org.apache.inlong.manager.common.pojo.stream.InlongStreamFieldInfo;
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
import org.apache.inlong.sort.protocol.BuiltInFieldInfo;
import org.apache.inlong.sort.protocol.BuiltInFieldInfo.BuiltInField;
import org.apache.inlong.sort.protocol.FieldInfo;
import org.apache.inlong.sort.protocol.transformation.FieldMappingRule.FieldMappingUnit;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Util for sort field info.
 */
public class FieldInfoUtils {

    /**
     * Built in field map, key is field name, value is built in field name
     */
    public static final Map<String, BuiltInField> BUILT_IN_FIELD_MAP = new HashMap<>();

    static {
        BUILT_IN_FIELD_MAP.put(MetaFieldType.DATA_TIME.getName(), BuiltInField.DATA_TIME);
        BUILT_IN_FIELD_MAP.put(MetaFieldType.DATABASE.getName(), BuiltInField.MYSQL_METADATA_DATABASE);
        BUILT_IN_FIELD_MAP.put(MetaFieldType.TABLE.getName(), BuiltInField.MYSQL_METADATA_TABLE);
        BUILT_IN_FIELD_MAP.put(MetaFieldType.EVENT_TIME.getName(), BuiltInField.MYSQL_METADATA_EVENT_TIME);
        BUILT_IN_FIELD_MAP.put(MetaFieldType.IS_DDL.getName(), BuiltInField.MYSQL_METADATA_IS_DDL);
        BUILT_IN_FIELD_MAP.put(MetaFieldType.EVENT_TYPE.getName(), BuiltInField.MYSQL_METADATA_EVENT_TYPE);
    }

    /**
     * Get field info list.
     * TODO 1. Support partition field(not need to add index at 0), 2. Add is_metadata field in StreamSinkFieldEntity
     */
    public static List<FieldMappingUnit> createFieldInfo(
            List<InlongStreamFieldInfo> streamFieldList, List<SinkFieldResponse> fieldList,
            List<FieldInfo> sourceFields, List<FieldInfo> sinkFields) {

        // Set source field info list.
        for (InlongStreamFieldInfo field : streamFieldList) {
            FieldInfo sourceField = getFieldInfo(field.getFieldName(), field.getFieldType(),
                    field.getIsMetaField() == 1, field.getFieldFormat());
            sourceFields.add(sourceField);
        }

        List<FieldMappingUnit> mappingUnitList = new ArrayList<>();
        // Get sink field info list, if the field name equals to build-in field, new a build-in field info
        for (SinkFieldResponse field : fieldList) {
            FieldInfo sinkField = getFieldInfo(field.getFieldName(), field.getFieldType(),
                    field.getIsMetaField() == 1, field.getFieldFormat());
            sinkFields.add(sinkField);

            FieldInfo sourceField = getFieldInfo(field.getSourceFieldName(),
                    field.getSourceFieldType(), field.getIsMetaField() == 1, field.getFieldFormat());
            mappingUnitList.add(new FieldMappingUnit(sourceField, sinkField));
        }

        return mappingUnitList;
    }

    /**
     * Get field info by the given field name ant type.
     *
     * @apiNote If the field name equals to build-in field, new a build-in field info
     */
    private static FieldInfo getFieldInfo(String fieldName, String fieldType, boolean isBuiltin, String format) {
        FieldInfo fieldInfo;
        BuiltInField builtInField = BUILT_IN_FIELD_MAP.get(fieldName);
        FormatInfo formatInfo = convertFieldFormat(fieldType.toLowerCase(), format);
        if (isBuiltin && builtInField != null) {
            fieldInfo = new BuiltInFieldInfo(fieldName, formatInfo, builtInField);
        } else {
            fieldInfo = new FieldInfo(fieldName, formatInfo);
        }
        return fieldInfo;
    }

    /**
     * Get all migration field mapping unit list for binlog source.
     */
    public static List<FieldMappingUnit> setAllMigrationFieldMapping(List<FieldInfo> sourceFields,
            List<FieldInfo> sinkFields) {
        List<FieldMappingUnit> mappingUnitList = new ArrayList<>();
        BuiltInFieldInfo dataField = new BuiltInFieldInfo("data", StringFormatInfo.INSTANCE,
                BuiltInField.MYSQL_METADATA_DATA);
        sourceFields.add(dataField);
        sinkFields.add(dataField);
        mappingUnitList.add(new FieldMappingUnit(dataField, dataField));

        for (Map.Entry<String, BuiltInField> entry : BUILT_IN_FIELD_MAP.entrySet()) {
            if (entry.getKey().equals("data_time")) {
                continue;
            }
            BuiltInFieldInfo fieldInfo = new BuiltInFieldInfo(entry.getKey(),
                    StringFormatInfo.INSTANCE, entry.getValue());
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
