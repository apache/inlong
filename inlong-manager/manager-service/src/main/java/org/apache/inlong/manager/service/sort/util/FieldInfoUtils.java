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

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import javafx.util.Pair;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.inlong.manager.common.enums.FieldType;
import org.apache.inlong.manager.common.enums.MetaFieldType;
import org.apache.inlong.manager.common.pojo.sink.SinkFieldResponse;
import org.apache.inlong.manager.common.pojo.stream.InlongStreamFieldInfo;
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
import org.apache.inlong.sort.formats.common.MapFormatInfo;
import org.apache.inlong.sort.formats.common.RowFormatInfo;
import org.apache.inlong.sort.formats.common.ShortFormatInfo;
import org.apache.inlong.sort.formats.common.StringFormatInfo;
import org.apache.inlong.sort.formats.common.TimeFormatInfo;
import org.apache.inlong.sort.formats.common.TimestampFormatInfo;
import org.apache.inlong.sort.protocol.BuiltInFieldInfo;
import org.apache.inlong.sort.protocol.BuiltInFieldInfo.BuiltInField;
import org.apache.inlong.sort.protocol.FieldInfo;
import org.apache.inlong.sort.protocol.transformation.FieldMappingRule.FieldMappingUnit;

import java.util.ArrayList;
import java.util.Deque;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Util for sort field info.
 */
@Slf4j
public class FieldInfoUtils {

    /**
     * Built in field map, key is field name, value is built in field name
     */
    public static final Map<String, BuiltInField> BUILT_IN_FIELD_MAP = new LinkedHashMap<>();

    static {
        BUILT_IN_FIELD_MAP.put(MetaFieldType.DATA_TIME.getName(), BuiltInField.DATA_TIME);
        BUILT_IN_FIELD_MAP.put(MetaFieldType.DATABASE.getName(), BuiltInField.MYSQL_METADATA_DATABASE);
        BUILT_IN_FIELD_MAP.put(MetaFieldType.TABLE.getName(), BuiltInField.MYSQL_METADATA_TABLE);
        BUILT_IN_FIELD_MAP.put(MetaFieldType.EVENT_TIME.getName(), BuiltInField.MYSQL_METADATA_EVENT_TIME);
        BUILT_IN_FIELD_MAP.put(MetaFieldType.IS_DDL.getName(), BuiltInField.MYSQL_METADATA_IS_DDL);
        BUILT_IN_FIELD_MAP.put(MetaFieldType.EVENT_TYPE.getName(), BuiltInField.MYSQL_METADATA_EVENT_TYPE);
        BUILT_IN_FIELD_MAP.put(MetaFieldType.PROCESSING_TIME.getName(), BuiltInField.PROCESS_TIME);
        BUILT_IN_FIELD_MAP.put(MetaFieldType.UPDATE_BEFORE.getName(), BuiltInField.METADATA_UPDATE_BEFORE);
        BUILT_IN_FIELD_MAP.put(MetaFieldType.BATCH_ID.getName(), BuiltInField.METADATA_BATCH_ID);
        BUILT_IN_FIELD_MAP.put(MetaFieldType.SQL_TYPE.getName(), BuiltInField.METADATA_SQL_TYPE);
        BUILT_IN_FIELD_MAP.put(MetaFieldType.TS.getName(), BuiltInField.METADATA_TS);
        BUILT_IN_FIELD_MAP.put(MetaFieldType.MYSQL_TYPE.getName(), BuiltInField.METADATA_MYSQL_TYPE);
        BUILT_IN_FIELD_MAP.put(MetaFieldType.PK_NAMES.getName(), BuiltInField.METADATA_PK_NAMES);
        BUILT_IN_FIELD_MAP.put(MetaFieldType.MYSQL_DATA.getName(), BuiltInField.MYSQL_METADATA_DATA);
    }

    public static FieldInfo parseSinkFieldInfo(SinkFieldResponse sinkFieldResponse, String nodeId) {
        boolean isBuiltIn = sinkFieldResponse.getIsMetaField() == 1;
        FieldInfo fieldInfo = getFieldInfo(sinkFieldResponse.getFieldName(), sinkFieldResponse.getFieldType(),
                isBuiltIn, sinkFieldResponse.getFieldFormat());
        fieldInfo.setNodeId(nodeId);
        return fieldInfo;
    }

    public static FieldInfo parseStreamFieldInfo(InlongStreamFieldInfo streamField, String nodeId) {
        boolean isBuiltIn = streamField.getIsMetaField() == 1;
        FieldInfo fieldInfo = getFieldInfo(streamField.getFieldName(), streamField.getFieldType(), isBuiltIn,
                streamField.getFieldFormat());
        fieldInfo.setNodeId(nodeId);
        return fieldInfo;
    }

    public static FieldInfo parseStreamField(StreamField streamField) {
        boolean isBuiltIn = streamField.getIsMetaField() == 1;
        FieldInfo fieldInfo;
        if (StringUtils.isEmpty(streamField.getComplexSubType())) {
            fieldInfo = getFieldInfo(streamField.getFieldName(), streamField.getFieldType().name(), isBuiltIn,
                    streamField.getFieldFormat());
        } else {
            String fieldType = streamField.getFieldType().name() + streamField.getComplexSubType();
            fieldInfo = getFieldInfo(streamField.getFieldName(), fieldType, isBuiltIn,
                    streamField.getFieldFormat());
        }
        fieldInfo.setNodeId(streamField.getOriginNodeName());
        return fieldInfo;
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
    private static FieldInfo getFieldInfo(String fieldName, String fieldType, boolean isBuiltin, String format) {
        FieldInfo fieldInfo;
        BuiltInField builtInField = BUILT_IN_FIELD_MAP.get(fieldName);
        FormatInfo formatInfo = convertFieldFormat(fieldType.toLowerCase(), format);
        if (isBuiltin && builtInField != null) {
            return new BuiltInFieldInfo(fieldName, formatInfo, builtInField);
        } else {
            if (isBuiltin) {
                // Check if fieldName contains buildInFieldName, such as left_database
                // TODO The buildin field needs to be selectable and cannot be filled in by the user
                for (String buildInFieldName : BUILT_IN_FIELD_MAP.keySet()) {
                    if (fieldName.contains(buildInFieldName)) {
                        builtInField = BUILT_IN_FIELD_MAP.get(buildInFieldName);
                        break;
                    }
                }
                if (builtInField != null) {
                    return new BuiltInFieldInfo(fieldName, formatInfo, builtInField);
                }
                log.warn("Unsupported metadata fieldName={} as the builtInField is null", fieldName);
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
        String baseType = type.contains("<") ? type.substring(0,type.indexOf("<")) : type;
        if (isComplecType(baseType)) {
            Map<String, String> complexType = Maps.newHashMap();
            complexType.put(baseType, type.substring(type.indexOf("<") + 1, type.length() - 1));
            return transferComplexType(complexType);
        } else {
            return transferSimpleType(baseType, format);
        }
    }

    /**
     * Get the FieldFormat of Sort according to simple type string
     *
     * @param type
     * @param format
     * @return
     */
    public static FormatInfo transferSimpleType(String type, String format) {
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
     * Get the FieldFormat of Sort according to complex type string
     *
     * @param map
     * @return
     */
    private static FormatInfo transferComplexType(Map<String, String> map) {
        FormatInfo formatInfo = null;
        Iterator iter = map.entrySet().iterator();
        while (iter.hasNext()) {
            Map.Entry entry = (Map.Entry) iter.next();
            String subtype = (String) entry.getValue();
            FieldType fieldType = FieldType.forName((String) entry.getKey());
            switch (fieldType) {
                case ARRAY:
                    formatInfo = new ArrayFormatInfo(convertFieldFormat(subtype));
                    break;
                case MAP:
                    Pair<String, String> mapPair = parseMapType(subtype);
                    formatInfo = new MapFormatInfo(convertFieldFormat(mapPair.getKey()),
                            convertFieldFormat(mapPair.getValue()));
                    break;
                case ROW:
                    Pair<List<String>, List<String>> rowPair = parseRowType(subtype);
                    formatInfo = new RowFormatInfo(rowPair.getKey().toArray(new String[0]),
                            rowPair.getValue().stream()
                                    .map(m -> convertFieldFormat(m))
                                    .collect(Collectors.toList())
                                    .toArray(new FormatInfo[0]));
                    break;
                default:
            }
        }
        return formatInfo;
    }

    /**
     * parse string of complex type : row
     *
     * @param str
     * @return
     */
    private static Pair<List<String>, List<String>> parseRowType(String str) {
        Deque<Pair<Character, Integer>> stack = new LinkedList<>();
        for (int i = 0; i < str.length(); i++) {
            char ch = str.charAt(i);
            if (ch == ' ' || ch == ',' ||  ch == '<') {
                stack.addFirst(new Pair<>(ch, i));
            }
            if (ch == '>' && !stack.isEmpty()) {
                while (stack.peekFirst().getKey() != '<') {
                    stack.pollFirst();
                }
                stack.pollFirst();
            }
        }
        List<Integer> separationIndexs = stack.stream()
                .map(pair -> pair.getValue())
                .sorted()
                .collect(Collectors.toList());

        List<String> fieldNames = Lists.newArrayList();
        List<String> fieldFormatInfos = Lists.newArrayList();

        fieldNames.add(str.substring(0, separationIndexs.get(0)));
        for (int j = 1; j < separationIndexs.size(); j++) {
            if (j % 2 == 0) {
                fieldNames.add(str.substring(separationIndexs.get(j - 1) + 1, separationIndexs.get(j)));
            } else {
                fieldFormatInfos.add(str.substring(separationIndexs.get(j - 1) + 1, separationIndexs.get(j)));
            }
        }
        fieldFormatInfos.add(str.substring(separationIndexs.get(separationIndexs.size() - 1) + 1));
        return  new Pair<>(fieldNames, fieldFormatInfos);
    }

    /**
     *Judge whether complextype
     *
     * @param type
     * @return
     */
    private static boolean isComplecType(String type) {
        return type.toLowerCase(Locale.ROOT).startsWith("array")
                || type.toLowerCase(Locale.ROOT).startsWith("map")
                || type.toLowerCase(Locale.ROOT).startsWith("row");
    }

    /**
     * parse string of complex type : map
     *
     * @param str
     * @return
     */
    private static Pair<String, String> parseMapType(String str) {
        Deque<Character> stack = new LinkedList<Character>();
        if (str.indexOf("<") == -1 || str.indexOf(">") == -1) {
            return new Pair<>(str.substring(0, str.indexOf(",")),str.substring(str.indexOf(",") + 1));
        } else {
            for (int i = 0; i < str.length(); i++) {
                char ch = str.charAt(i);
                if (ch == '<' || ch == ',') {
                    stack.addFirst(ch);
                }
                if (ch == '>' && !stack.isEmpty()) {
                    while (stack.peekFirst() != '<') {
                        stack.pollFirst();
                    }
                    stack.pollFirst();
                    if (stack.isEmpty()) {
                        return new Pair<>(str.substring(0, i + 1), str.substring(i + 2));
                    }
                }
            }
        }
        throw new IllegalArgumentException(String.format("Unsupported mode %s for subtype", str));
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
