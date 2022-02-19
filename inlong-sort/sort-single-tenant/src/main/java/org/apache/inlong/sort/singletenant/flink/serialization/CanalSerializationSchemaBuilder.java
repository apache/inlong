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
import org.apache.flink.formats.json.canal.CanalJsonSerializationSchema;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.Row;
import org.apache.inlong.sort.protocol.FieldInfo;
import org.apache.inlong.sort.protocol.serialization.CanalSerializationInfo;

import java.io.IOException;

import static org.apache.inlong.sort.singletenant.flink.serialization.SerializationSchemaFactory.MAP_NULL_KEY_LITERAL_DEFAULT;
import static org.apache.inlong.sort.singletenant.flink.serialization.SerializationSchemaFactory.getMapNullKeyMode;
import static org.apache.inlong.sort.singletenant.flink.utils.CommonUtils.convertDateToStringFormatInfo;
import static org.apache.inlong.sort.singletenant.flink.utils.CommonUtils.convertFieldInfosToRowType;
import static org.apache.inlong.sort.singletenant.flink.utils.CommonUtils.extractFormatInfos;
import static org.apache.inlong.sort.singletenant.flink.utils.CommonUtils.getTimestampFormatStandard;

public class CanalSerializationSchemaBuilder {

    public static SerializationSchema<Row> build(
            FieldInfo[] fieldInfos,
            CanalSerializationInfo canalSerializationInfo
    ) throws IOException, ClassNotFoundException {
        String mapNullKeyLiteral = canalSerializationInfo.getMapNullKeyLiteral();
        if (StringUtils.isEmpty(mapNullKeyLiteral)) {
            mapNullKeyLiteral = MAP_NULL_KEY_LITERAL_DEFAULT;
        }

        FieldInfo[] convertedFieldInfos = convertDateToStringFormatInfo(fieldInfos);
        RowType convertedRowType = convertFieldInfosToRowType(convertedFieldInfos);
        CanalJsonSerializationSchema canalSchema = new CanalJsonSerializationSchema(
                convertedRowType,
                getTimestampFormatStandard(canalSerializationInfo.getTimestampFormatStandard()),
                getMapNullKeyMode(canalSerializationInfo.getMapNullKeyMod()),
                mapNullKeyLiteral,
                canalSerializationInfo.isEncodeDecimalAsPlainNumber()
        );

        RowToRowDataSerializationSchemaWrapper rowToRowDataSchema
                = new RowToRowDataSerializationSchemaWrapper(canalSchema, convertedFieldInfos);

        return new CustomDateFormatSerializationSchemaWrapper(rowToRowDataSchema, extractFormatInfos(fieldInfos));
    }

}
