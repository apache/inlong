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

package org.apache.inlong.sort.singletenant.flink.serialization;

import static org.apache.inlong.sort.singletenant.flink.serialization.SerializationSchemaFactory.MAP_NULL_KEY_LITERAL_DEFAULT;
import static org.apache.inlong.sort.singletenant.flink.serialization.SerializationSchemaFactory.getMapNullKeyMode;
import static org.apache.inlong.sort.singletenant.flink.utils.CommonUtils.convertDateToStringFormatInfo;
import static org.apache.inlong.sort.singletenant.flink.utils.CommonUtils.convertFieldInfosToRowType;
import static org.apache.inlong.sort.singletenant.flink.utils.CommonUtils.createRowConverter;
import static org.apache.inlong.sort.singletenant.flink.utils.CommonUtils.extractFormatInfos;
import static org.apache.inlong.sort.singletenant.flink.utils.CommonUtils.getTimestampFormatStandard;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.Row;
import org.apache.inlong.sort.formats.json.canal.CanalJsonDecodingFormat.ReadableMetadata;
import org.apache.inlong.sort.formats.json.canal.CanalJsonSerializationSchema;
import org.apache.inlong.sort.protocol.BuiltInFieldInfo;
import org.apache.inlong.sort.protocol.BuiltInFieldInfo.BuiltInField;
import org.apache.inlong.sort.protocol.FieldInfo;
import org.apache.inlong.sort.protocol.serialization.CanalSerializationInfo;
import org.apache.inlong.sort.singletenant.flink.utils.CommonUtils;

public class CanalSerializationSchemaBuilder {

    public static SerializationSchema<Row> build(
            FieldInfo[] fieldInfos,
            CanalSerializationInfo canalSerializationInfo
    ) throws IOException, ClassNotFoundException {
        String mapNullKeyLiteral = canalSerializationInfo.getMapNullKeyLiteral();
        if (StringUtils.isEmpty(mapNullKeyLiteral)) {
            mapNullKeyLiteral = MAP_NULL_KEY_LITERAL_DEFAULT;
        }

        FieldInfo[] originPhysicalFieldInfos = CommonUtils.extractNonBuiltInFieldInfos(fieldInfos);
        FieldInfo[] convertedPhysicalFieldInfos = convertDateToStringFormatInfo(originPhysicalFieldInfos);
        RowType convertedPhysicalRowType = convertFieldInfosToRowType(convertedPhysicalFieldInfos);
        FieldInfo[] convertedFieldInfos = convertDateToStringFormatInfo(fieldInfos);

        CanalJsonSerializationSchema canalSchema = new CanalJsonSerializationSchema(
                convertedPhysicalRowType,
                getFieldIndexToMetadata(fieldInfos),
                createRowConverter(convertedFieldInfos),
                createRowConverter(convertedPhysicalFieldInfos),
                getTimestampFormatStandard(canalSerializationInfo.getTimestampFormatStandard()),
                getMapNullKeyMode(canalSerializationInfo.getMapNullKeyMod()),
                mapNullKeyLiteral,
                canalSerializationInfo.isEncodeDecimalAsPlainNumber()
        );

        RowToRowDataSerializationSchemaWrapper rowToRowDataSchema
                = new RowToRowDataSerializationSchemaWrapper(canalSchema, convertedFieldInfos);

        return new CustomDateFormatSerializationSchemaWrapper(rowToRowDataSchema, extractFormatInfos(fieldInfos));
    }

    private static Map<Integer, ReadableMetadata> getFieldIndexToMetadata(FieldInfo[] fieldInfos) {
        Map<Integer, ReadableMetadata> fieldIndexToMetadata = new HashMap<>();

        for (int i = 0; i < fieldInfos.length; i++) {
            FieldInfo fieldInfo = fieldInfos[i];
            if (fieldInfo instanceof BuiltInFieldInfo) {
                BuiltInFieldInfo builtInFieldInfo = (BuiltInFieldInfo) fieldInfo;
                BuiltInField builtInField = builtInFieldInfo.getBuiltInField();
                switch (builtInField) {
                    case MYSQL_METADATA_DATABASE:
                        fieldIndexToMetadata.put(i, ReadableMetadata.DATABASE);
                        break;
                    case MYSQL_METADATA_TABLE:
                        fieldIndexToMetadata.put(i, ReadableMetadata.TABLE);
                        break;
                    case MYSQL_METADATA_EVENT_TIME:
                        fieldIndexToMetadata.put(i, ReadableMetadata.EVENT_TIMESTAMP);
                        break;
                    case MYSQL_METADATA_IS_DDL:
                        fieldIndexToMetadata.put(i, ReadableMetadata.IS_DDL);
                        break;
                    case MYSQL_METADATA_EVENT_TYPE:
                        // We will always append `type` to the result
                        break;
                    default:
                        throw new IllegalArgumentException(
                                "Unsupported builtin field '" + builtInField + "' in debezium deserialization");
                }
            }
        }

        return fieldIndexToMetadata;
    }

}
