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

import com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.formats.avro.AvroRowSerializationSchema;
import org.apache.flink.formats.json.JsonRowSerializationSchema;
import org.apache.flink.types.Row;
import org.apache.inlong.sort.protocol.FieldInfo;
import org.apache.inlong.sort.protocol.serialization.AvroSerializationInfo;
import org.apache.inlong.sort.protocol.serialization.JsonSerializationInfo;
import org.apache.inlong.sort.protocol.serialization.SerializationInfo;

import java.nio.charset.StandardCharsets;

import static org.apache.inlong.sort.singletenant.flink.serialization.AvroUtils.buildAvroRecordSchemaInJson;
import static org.apache.inlong.sort.singletenant.flink.utils.CommonUtils.convertFieldInfosToRowTypeInfo;

public class RowSerializationSchemaFactory {

    public static SerializationSchema<Row> build(FieldInfo[] fieldInfos, SerializationInfo serializationInfo)
            throws JsonProcessingException {
        if (serializationInfo instanceof JsonSerializationInfo) {
            return buildJsonRowSerializationSchema(fieldInfos);
        } else if (serializationInfo instanceof AvroSerializationInfo) {
            return buildAvroRowSerializationSchema(fieldInfos);
        } else {
            return buildStringRowSerializationSchema();
        }
    }

    private static SerializationSchema<Row> buildJsonRowSerializationSchema(FieldInfo[] fieldInfos) {
        RowTypeInfo rowTypeInfo = convertFieldInfosToRowTypeInfo(fieldInfos);
        JsonRowSerializationSchema.Builder builder = JsonRowSerializationSchema.builder();
        return builder.withTypeInfo(rowTypeInfo).build();
    }

    private static SerializationSchema<Row> buildAvroRowSerializationSchema(FieldInfo[] fieldInfos) {
        String avroSchemaInJson = buildAvroRecordSchemaInJson(fieldInfos);
        return new AvroRowSerializationSchema(avroSchemaInJson);
    }

    private static SerializationSchema<Row> buildStringRowSerializationSchema() {
        return new SerializationSchema<Row>() {

            private static final long serialVersionUID = -6818985955456373916L;

            @Override
            public byte[] serialize(Row element) {
                return element.toString().getBytes(StandardCharsets.UTF_8);
            }
        };
    }
}
