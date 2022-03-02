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

package org.apache.inlong.sort.singletenant.flink.deserialization;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.formats.avro.AvroRowDeserializationSchema;
import org.apache.flink.formats.json.JsonRowDeserializationSchema;
import org.apache.flink.types.Row;
import org.apache.inlong.sort.protocol.FieldInfo;
import org.apache.inlong.sort.protocol.deserialization.AvroDeserializationInfo;
import org.apache.inlong.sort.protocol.deserialization.CanalDeserializationInfo;
import org.apache.inlong.sort.protocol.deserialization.DebeziumDeserializationInfo;
import org.apache.inlong.sort.protocol.deserialization.DeserializationInfo;
import org.apache.inlong.sort.protocol.deserialization.JsonDeserializationInfo;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

import static org.apache.inlong.sort.singletenant.flink.utils.CommonUtils.buildAvroRecordSchemaInJson;
import static org.apache.inlong.sort.singletenant.flink.utils.CommonUtils.convertDateToStringFormatInfo;
import static org.apache.inlong.sort.singletenant.flink.utils.CommonUtils.convertFieldInfosToRowTypeInfo;
import static org.apache.inlong.sort.singletenant.flink.utils.CommonUtils.extractFormatInfos;
import static org.apache.inlong.sort.singletenant.flink.utils.CommonUtils.extractNonBuiltInFieldInfos;

public class DeserializationSchemaFactory {

    public static DeserializationSchema<Row> build(
            FieldInfo[] fieldInfos,
            DeserializationInfo deserializationInfo) throws IOException, ClassNotFoundException {
        if (deserializationInfo instanceof JsonDeserializationInfo) {
            return buildJsonDeserializationSchema(extractNonBuiltInFieldInfos(fieldInfos, false));
        } else if (deserializationInfo instanceof AvroDeserializationInfo) {
            return buildAvroDeserializationSchema(extractNonBuiltInFieldInfos(fieldInfos, false));
        } else if (deserializationInfo instanceof CanalDeserializationInfo) {
            return CanalDeserializationSchemaBuilder.build(
                    extractNonBuiltInFieldInfos(fieldInfos, false),
                    (CanalDeserializationInfo) deserializationInfo);
        } else if (deserializationInfo instanceof DebeziumDeserializationInfo) {
            return DebeziumDeserializationSchemaBuilder.build(
                    fieldInfos,
                    (DebeziumDeserializationInfo) deserializationInfo);
        } else {
            return buildStringDeserializationSchema(extractNonBuiltInFieldInfos(fieldInfos, false));
        }
    }

    private static DeserializationSchema<Row> buildJsonDeserializationSchema(
            FieldInfo[] fieldInfos) throws IOException, ClassNotFoundException {
        FieldInfo[] convertedFieldInfos = convertDateToStringFormatInfo(fieldInfos);
        RowTypeInfo rowTypeInfo = convertFieldInfosToRowTypeInfo(convertedFieldInfos);
        JsonRowDeserializationSchema jsonSchema = new JsonRowDeserializationSchema.Builder(rowTypeInfo).build();
        return new CustomDateFormatDeserializationSchemaWrapper(jsonSchema, extractFormatInfos(fieldInfos));
    }

    private static DeserializationSchema<Row> buildAvroDeserializationSchema(FieldInfo[] fieldInfos) {
        String avroSchemaInJson = buildAvroRecordSchemaInJson(fieldInfos);
        return new AvroRowDeserializationSchema(avroSchemaInJson);
    }

    private static DeserializationSchema<Row> buildStringDeserializationSchema(FieldInfo[] fieldInfos) {
        return new DeserializationSchema<Row>() {

            private static final long serialVersionUID = 9206114128358065420L;

            private final RowTypeInfo rowTypeInfo = convertFieldInfosToRowTypeInfo(fieldInfos);

            @Override
            public Row deserialize(byte[] message) {
                Row row = new Row(1);
                row.setField(0, new String(message, StandardCharsets.UTF_8));
                return row;
            }

            @Override
            public boolean isEndOfStream(Row nextElement) {
                return false;
            }

            @Override
            public TypeInformation<Row> getProducedType() {
                return rowTypeInfo;
            }
        };
    }

}
