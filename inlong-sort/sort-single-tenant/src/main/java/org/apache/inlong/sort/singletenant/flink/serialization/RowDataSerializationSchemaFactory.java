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

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.formats.common.TimestampFormat;
import org.apache.flink.formats.json.JsonOptions;
import org.apache.flink.formats.json.canal.CanalJsonSerializationSchema;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.inlong.sort.protocol.FieldInfo;
import org.apache.inlong.sort.protocol.serialization.CanalSerializationInfo;
import org.apache.inlong.sort.protocol.serialization.SerializationInfo;

import static org.apache.inlong.sort.singletenant.flink.utils.CommonUtils.convertFieldInfosToRowType;

public class RowDataSerializationSchemaFactory {

    private static final String CANAL_TIMESTAMP_STANDARD_SQL = "SQL";
    private static final String CANAL_TIMESTAMP_STANDARD_ISO = "ISO_8601";
    private static final String CANAL_MAP_NULL_KEY_MODE_FAIL = "FAIL";
    private static final String CANAL_MAP_NULL_KEY_MODE_DROP = "DROP";
    private static final String CANAL_MAP_NULL_KEY_MODE_LITERAL = "LITERAL";

    private static final String CANAL_MAP_NULL_KEY_LITERAL_DEFAULT = "null";

    public static SerializationSchema<RowData> build(FieldInfo[] fieldInfos, SerializationInfo serializationInfo) {
        if (serializationInfo instanceof CanalSerializationInfo) {
            return buildCanalRowDataSerializationSchema(fieldInfos, (CanalSerializationInfo) serializationInfo);
        }

        throw new IllegalArgumentException("Unsupported RowData serialization info: " + serializationInfo);
    }

    private static SerializationSchema<RowData> buildCanalRowDataSerializationSchema(
            FieldInfo[] fieldInfos,
            CanalSerializationInfo canalSerializationInfo
    ) {
        String mapNullKeyLiteral = canalSerializationInfo.getMapNullKeyLiteral();
        if (StringUtils.isEmpty(mapNullKeyLiteral)) {
            mapNullKeyLiteral = CANAL_MAP_NULL_KEY_LITERAL_DEFAULT;
        }

        RowType rowType = convertFieldInfosToRowType(fieldInfos);
        return new CanalJsonSerializationSchema(
                rowType,
                getTimestampFormatStandard(canalSerializationInfo.getTimestampFormatStandard()),
                getMapNullKeyMode(canalSerializationInfo.getMapNullKeyMod()),
                mapNullKeyLiteral,
                canalSerializationInfo.isEncodeDecimalAsPlainNumber()
        );
    }

    private static TimestampFormat getTimestampFormatStandard(String input) {
        if (CANAL_TIMESTAMP_STANDARD_SQL.equals(input)) {
            return TimestampFormat.SQL;
        } else if (CANAL_TIMESTAMP_STANDARD_ISO.equals(input)) {
            return TimestampFormat.ISO_8601;
        }

        throw new IllegalArgumentException("Unsupported timestamp format standard: " + input);
    }

    private static JsonOptions.MapNullKeyMode getMapNullKeyMode(String input) {
        if (CANAL_MAP_NULL_KEY_MODE_FAIL.equals(input)) {
            return JsonOptions.MapNullKeyMode.FAIL;
        } else if (CANAL_MAP_NULL_KEY_MODE_DROP.equals(input)) {
            return JsonOptions.MapNullKeyMode.DROP;
        } else if (CANAL_MAP_NULL_KEY_MODE_LITERAL.equals(input)) {
            return JsonOptions.MapNullKeyMode.LITERAL;
        }

        throw new IllegalArgumentException("Unsupported map null key mode: " + input);
    }

}
