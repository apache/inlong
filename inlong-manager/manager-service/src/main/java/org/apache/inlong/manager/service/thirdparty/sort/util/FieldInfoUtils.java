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
import org.apache.inlong.manager.common.enums.MetaFieldType;
import org.apache.inlong.manager.common.pojo.sink.SinkFieldResponse;
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
import org.apache.inlong.sort.protocol.transformation.FieldMappingRule;
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
     * TODO 1. Support partition field, 2. Add is_metadata field in StreamSinkFieldEntity
     */
    public static FieldMappingRule createFieldInfo(boolean isAllMigration, List<SinkFieldResponse> fieldList,
            List<FieldInfo> sourceFields, List<FieldInfo> sinkFields, String partitionField) {

        List<FieldMappingUnit> fieldMappingUnitList = new ArrayList<>();
        if (isAllMigration) {
            setAllMigrationBuiltInField(sourceFields, sinkFields, fieldMappingUnitList);
        } else {
            boolean duplicate = false;
            for (SinkFieldResponse field : fieldList) {
                // If the field name equals to build-in field, new a build-in field info
                FieldInfo sourceFieldInfo = getFieldInfo(field.getSourceFieldName(),
                        field.getSourceFieldType(), field.getIsSourceMetaField() == 1);
                sourceFields.add(sourceFieldInfo);

                // Get sink field info
                String sinkFieldName = field.getFieldName();
                if (sinkFieldName.equals(partitionField)) {
                    duplicate = true;
                }
                FieldInfo sinkFieldInfo = getSinkFieldInfo(field.getFieldName(), field.getFieldType(),
                        field.getSourceFieldName(), field.getIsSourceMetaField() == 1);
                sinkFields.add(sinkFieldInfo);

                fieldMappingUnitList.add(new FieldMappingUnit(sourceFieldInfo, sinkFieldInfo));
            }

            // If no partition field in the ordinary fields, add the partition field to the first position
            if (!duplicate && StringUtils.isNotEmpty(partitionField)) {
                FieldInfo fieldInfo = new FieldInfo(partitionField, new TimestampFormatInfo("MILLIS"));
                sourceFields.add(0, fieldInfo);
            }
        }

        return new FieldMappingRule(fieldMappingUnitList.toArray(new FieldMappingUnit[0]));
    }

    /**
     * Get field info by the given field name ant type.
     *
     * @apiNote If the field name equals to build-in field, new a build-in field info
     */
    private static FieldInfo getFieldInfo(String fieldName, String fieldType) {
        return getFieldInfo(fieldName, fieldType, false);
    }

    /**
     * Get field info by the given field name ant type.
     *
     * @apiNote If the field name equals to build-in field, new a build-in field info
     */
    private static FieldInfo getFieldInfo(String fieldName, String fieldType, boolean isBuiltin) {
        FieldInfo fieldInfo;
        BuiltInField builtInField = BUILT_IN_FIELD_MAP.get(fieldName);
        FormatInfo formatInfo = convertFieldFormat(fieldType.toLowerCase());
        if (isBuiltin && builtInField != null) {
            fieldInfo = new BuiltInFieldInfo(fieldName, formatInfo, builtInField);
        } else {
            fieldInfo = new FieldInfo(fieldName, formatInfo);
        }
        return fieldInfo;
    }

    /**
     * Get field info by the given field name ant type.
     *
     * @apiNote If the field name equals to build-in field, new a build-in field info
     */
    private static FieldInfo getSinkFieldInfo(String fieldName, String fieldType,
            String sourceFieldName, boolean isBuiltin) {
        FieldInfo fieldInfo;
        BuiltInField builtInField = BUILT_IN_FIELD_MAP.get(sourceFieldName);
        FormatInfo formatInfo = convertFieldFormat(fieldType.toLowerCase());
        if (isBuiltin && builtInField != null) {
            fieldInfo = new BuiltInFieldInfo(fieldName, formatInfo, builtInField);
        } else {
            fieldInfo = new FieldInfo(fieldName, formatInfo);
        }
        return fieldInfo;
    }

    /**
     * Get all migration built-in field for binlog source.
     */
    public static void setAllMigrationBuiltInField(List<FieldInfo> sourceFields, List<FieldInfo> sinkFields,
            List<FieldMappingUnit> fieldMappingUnitList) {
        BuiltInFieldInfo dataField = new BuiltInFieldInfo("data",
                StringFormatInfo.INSTANCE, BuiltInField.MYSQL_METADATA_DATA);
        sourceFields.add(dataField);
        sinkFields.add(dataField);
        fieldMappingUnitList.add(new FieldMappingUnit(dataField, dataField));

        for (Map.Entry<String, BuiltInField> entry : BUILT_IN_FIELD_MAP.entrySet()) {
            if (entry.getKey().equals("data_time")) {
                continue;
            }
            BuiltInFieldInfo fieldInfo = new BuiltInFieldInfo(entry.getKey(),
                    StringFormatInfo.INSTANCE, entry.getValue());
            sourceFields.add(fieldInfo);
            sinkFields.add(fieldInfo);
            fieldMappingUnitList.add(new FieldMappingUnit(fieldInfo, fieldInfo));
        }
    }

    /**
     * Get the FieldFormat of Sort according to type string
     *
     * @param type type string
     * @return Sort field format instance
     */
    public static FormatInfo convertFieldFormat(String type) {
        FormatInfo formatInfo;
        switch (type) {
            case "boolean":
                formatInfo = new BooleanFormatInfo();
                break;
            case "tinyint":
            case "byte":
                formatInfo = new ByteFormatInfo();
                break;
            case "smallint":
            case "short":
                formatInfo = new ShortFormatInfo();
                break;
            case "int":
                formatInfo = new IntFormatInfo();
                break;
            case "bigint":
            case "long":
                formatInfo = new LongFormatInfo();
                break;
            case "float":
                formatInfo = new FloatFormatInfo();
                break;
            case "double":
                formatInfo = new DoubleFormatInfo();
                break;
            case "decimal":
                formatInfo = new DecimalFormatInfo();
                break;
            case "date":
                formatInfo = new DateFormatInfo();
                break;
            case "time":
                formatInfo = new TimeFormatInfo();
                break;
            case "timestamp":
                formatInfo = new TimestampFormatInfo();
                break;
            case "binary":
            case "fixed":
                formatInfo = new ArrayFormatInfo(ByteTypeInfo::new);
                break;
            default: // default is string
                formatInfo = new StringFormatInfo();
        }

        return formatInfo;
    }

}
