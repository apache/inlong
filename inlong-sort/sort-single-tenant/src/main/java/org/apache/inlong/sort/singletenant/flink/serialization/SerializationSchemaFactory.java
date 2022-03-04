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

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.formats.avro.AvroRowSerializationSchema;
import org.apache.flink.formats.json.JsonOptions;
import org.apache.flink.formats.json.JsonRowSerializationSchema;
import org.apache.flink.types.Row;
import org.apache.inlong.sort.formats.common.FormatInfo;
import org.apache.inlong.sort.protocol.FieldInfo;
import org.apache.inlong.sort.protocol.serialization.AvroSerializationInfo;
import org.apache.inlong.sort.protocol.serialization.CanalSerializationInfo;
import org.apache.inlong.sort.protocol.serialization.DebeziumSerializationInfo;
import org.apache.inlong.sort.protocol.serialization.JsonSerializationInfo;
import org.apache.inlong.sort.protocol.serialization.SerializationInfo;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

import static org.apache.inlong.sort.singletenant.flink.utils.CommonUtils.buildAvroRecordSchemaInJson;
import static org.apache.inlong.sort.singletenant.flink.utils.CommonUtils.convertDateToStringFormatInfo;
import static org.apache.inlong.sort.singletenant.flink.utils.CommonUtils.convertFieldInfosToRowTypeInfo;
import static org.apache.inlong.sort.singletenant.flink.utils.CommonUtils.extractFormatInfos;

public class SerializationSchemaFactory {

    public static final String MAP_NULL_KEY_MODE_FAIL = "FAIL";
    public static final String MAP_NULL_KEY_MODE_DROP = "DROP";
    public static final String MAP_NULL_KEY_MODE_LITERAL = "LITERAL";

    public static final String MAP_NULL_KEY_LITERAL_DEFAULT = "null";

    public static SerializationSchema<Row> build(
            FieldInfo[] fieldInfos,
            SerializationInfo serializationInfo
    ) throws IOException, ClassNotFoundException {
        if (serializationInfo instanceof JsonSerializationInfo) {
            return buildJsonSerializationSchema(fieldInfos);
        } else if (serializationInfo instanceof AvroSerializationInfo) {
            return buildAvroSerializationSchema(fieldInfos);
        } else if (serializationInfo instanceof CanalSerializationInfo) {
            return CanalSerializationSchemaBuilder.build(fieldInfos);
        } else if (serializationInfo instanceof DebeziumSerializationInfo) {
            return DebeziumSerializationSchemaBuilder.build(fieldInfos, (DebeziumSerializationInfo) serializationInfo);
        } else {
            return buildStringSerializationSchema(extractFormatInfos(fieldInfos));
        }
    }

    private static SerializationSchema<Row> buildJsonSerializationSchema(
            FieldInfo[] fieldInfos
    ) throws IOException, ClassNotFoundException {
        FieldInfo[] convertedFieldInfos = convertDateToStringFormatInfo(fieldInfos);
        RowTypeInfo convertedRowTypeInfo = convertFieldInfosToRowTypeInfo(convertedFieldInfos);
        JsonRowSerializationSchema.Builder builder = JsonRowSerializationSchema.builder();
        JsonRowSerializationSchema innerSchema = builder.withTypeInfo(convertedRowTypeInfo).build();
        return new CustomDateFormatSerializationSchemaWrapper(innerSchema, extractFormatInfos(fieldInfos));
    }

    private static SerializationSchema<Row> buildAvroSerializationSchema(FieldInfo[] fieldInfos) {
        String avroSchemaInJson = buildAvroRecordSchemaInJson(fieldInfos);
        return new AvroRowSerializationSchema(avroSchemaInJson);
    }

    private static SerializationSchema<Row> buildStringSerializationSchema(FormatInfo[] formatInfos) {
        SerializationSchema<Row> stringSchema = new SerializationSchema<Row>() {

            private static final long serialVersionUID = -6818985955456373916L;

            @Override
            public byte[] serialize(Row element) {
                return element.toString().getBytes(StandardCharsets.UTF_8);
            }
        };

        return new CustomDateFormatSerializationSchemaWrapper(stringSchema, formatInfos);
    }

    static JsonOptions.MapNullKeyMode getMapNullKeyMode(String input) {
        if (MAP_NULL_KEY_MODE_FAIL.equalsIgnoreCase(input)) {
            return JsonOptions.MapNullKeyMode.FAIL;
        } else if (MAP_NULL_KEY_MODE_DROP.equalsIgnoreCase(input)) {
            return JsonOptions.MapNullKeyMode.DROP;
        } else if (MAP_NULL_KEY_MODE_LITERAL.equalsIgnoreCase(input)) {
            return JsonOptions.MapNullKeyMode.LITERAL;
        }

        throw new IllegalArgumentException("Unsupported map null key mode: " + input);
    }

}
