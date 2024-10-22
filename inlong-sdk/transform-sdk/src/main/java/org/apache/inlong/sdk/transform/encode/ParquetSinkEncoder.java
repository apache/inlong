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

package org.apache.inlong.sdk.transform.encode;

import org.apache.inlong.sdk.transform.pojo.FieldInfo;
import org.apache.inlong.sdk.transform.pojo.ParquetSinkInfo;
import org.apache.inlong.sdk.transform.process.Context;

import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Type;
import org.apache.parquet.schema.Types;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY;

/**
 * ParquetSinkEncoder
 */
public class ParquetSinkEncoder extends SinkEncoder<ByteArrayOutputStream> {

    protected ParquetSinkInfo sinkInfo;
    private ParquetByteArrayWriter<Object[]> writer;

    public ParquetSinkEncoder(ParquetSinkInfo sinkInfo) {
        super(sinkInfo.getFields());
        this.sinkInfo = sinkInfo;
        ArrayList<Type> typesList = new ArrayList<>();
        for (FieldInfo fieldInfo : this.fields) {
            typesList.add(Types.required(BINARY)
                    .as(LogicalTypeAnnotation.stringType())
                    .named(fieldInfo.getName()));
        }
        MessageType schema = new MessageType("Output", typesList);
        ParquetWriteRunner<Object[]> writeRunner = (record, valueWriter) -> {
            for (int i = 0; i < record.length; i++) {
                valueWriter.write(this.fields.get(i).getName(), record[i]);
            }
        };
        try {
            writer = ParquetByteArrayWriter.buildWriter(schema, writeRunner);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public ByteArrayOutputStream encode(SinkData sinkData, Context context) {
        int size = this.fields.size();
        Object[] rowsInfo = new Object[size];
        Arrays.fill(rowsInfo, "");
        for (int i = 0; i < size; i++) {
            String fieldData = sinkData.getField(this.fields.get(i).getName());
            if (fieldData == null) {
                continue;
            }
            rowsInfo[i] = fieldData;
        }
        try {
            writer.write(rowsInfo);
        } catch (Exception ignored) {

        }
        return writer.getByteArrayOutputStream();
    }

    public byte[] mergeByteArray(List<ByteArrayOutputStream> list) {
        if (list.isEmpty()) {
            return null;
        }
        try {
            this.writer.close(); // need firstly close
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return list.get(0).toByteArray();
    }
}
