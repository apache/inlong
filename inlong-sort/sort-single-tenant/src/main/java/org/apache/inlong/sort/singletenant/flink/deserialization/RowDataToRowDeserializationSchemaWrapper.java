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

package org.apache.inlong.sort.singletenant.flink.deserialization;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.util.DataFormatConverters;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.apache.inlong.sort.protocol.FieldInfo;

import java.io.IOException;

import static org.apache.flink.shaded.guava18.com.google.common.base.Preconditions.checkNotNull;
import static org.apache.inlong.sort.singletenant.flink.utils.CommonUtils.convertFieldInfosToRowTypeInfo;
import static org.apache.inlong.sort.singletenant.flink.utils.CommonUtils.createRowConverter;

public class RowDataToRowDeserializationSchemaWrapper implements DeserializationSchema<Row> {

    private static final long serialVersionUID = 6918677263991851770L;

    private final DeserializationSchema<RowData> innerSchema;

    private final DataFormatConverters.RowConverter rowConverter;

    private final TypeInformation<Row> producedTypeInfo;

    public RowDataToRowDeserializationSchemaWrapper(
            DeserializationSchema<RowData> innerSchema, FieldInfo[] fieldInfos) {
        this.innerSchema = checkNotNull(innerSchema);
        rowConverter = createRowConverter(checkNotNull(fieldInfos));
        producedTypeInfo = convertFieldInfosToRowTypeInfo(fieldInfos);
    }

    @Override
    public Row deserialize(byte[] message) {
        throw new RuntimeException(
                "Please invoke DeserializationSchema#deserialize(byte[], Collector<RowData>) instead.");
    }

    @Override
    public void deserialize(byte[] message, Collector<Row> out) throws IOException {
        ListCollector<RowData> collector = new ListCollector<>();
        innerSchema.deserialize(message, collector);
        collector.getInnerList().forEach(record -> out.collect(rowConverter.toExternal(record)));
    }

    @Override
    public boolean isEndOfStream(Row nextElement) {
        return innerSchema.isEndOfStream(rowConverter.toInternal(nextElement));
    }

    @Override
    public TypeInformation<Row> getProducedType() {
        return producedTypeInfo;
    }

    @Override
    public void open(InitializationContext context) throws Exception {
        innerSchema.open(context);
    }

}
