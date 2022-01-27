/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.inlong.sort.singletenant.flink.serialization;

import org.apache.avro.Schema;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.inlong.sort.formats.common.FormatInfo;
import org.apache.inlong.sort.formats.common.RowFormatInfo;
import org.apache.inlong.sort.protocol.FieldInfo;

import static org.apache.flink.formats.avro.typeutils.AvroSchemaConverter.convertToSchema;
import static org.apache.inlong.sort.formats.base.TableFormatUtils.deriveLogicalType;

public class AvroUtils {

    public static String buildAvroRecordSchemaInJson(FieldInfo[] fieldInfos) {
        int fieldLength = fieldInfos.length;
        String[] fieldNames = new String[fieldLength];
        FormatInfo[] fieldFormatInfos = new FormatInfo[fieldLength];
        for (int i = 0; i < fieldLength; i++) {
            fieldNames[i] = fieldInfos[i].getName();
            fieldFormatInfos[i] = fieldInfos[i].getFormatInfo();
        }

        RowFormatInfo rowFormatInfo = new RowFormatInfo(fieldNames, fieldFormatInfos);
        LogicalType logicalType = deriveLogicalType(rowFormatInfo);
        Schema schema = convertToSchema(logicalType);

        if (schema.isUnion()) {
            return schema.getTypes().get(1).toString();
        }
        return schema.toString();
    }

}
