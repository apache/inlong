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
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.util.DataFormatConverters;
import org.apache.flink.types.Row;
import org.apache.inlong.sort.protocol.FieldInfo;

import static org.apache.flink.shaded.guava18.com.google.common.base.Preconditions.checkNotNull;
import static org.apache.inlong.sort.singletenant.flink.utils.CommonUtils.createRowConverter;

public class RowToRowDataSerializationSchemaWrapper implements SerializationSchema<Row> {

    private static final long serialVersionUID = 2496100417131340005L;

    private final SerializationSchema<RowData> innerSchema;

    private final DataFormatConverters.RowConverter rowConverter;

    public RowToRowDataSerializationSchemaWrapper(SerializationSchema<RowData> innerSchema, FieldInfo[] fieldInfos) {
        this.innerSchema = checkNotNull(innerSchema);
        this.rowConverter = createRowConverter(checkNotNull(fieldInfos));
    }

    @Override
    public void open(InitializationContext context) throws Exception {
        innerSchema.open(context);
    }

    @Override
    public byte[] serialize(Row element) {
        return innerSchema.serialize(rowConverter.toInternal(element));
    }

}
