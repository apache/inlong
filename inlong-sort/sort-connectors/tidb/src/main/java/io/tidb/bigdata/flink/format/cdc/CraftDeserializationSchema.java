/*
 * Copyright 2021 TiDB Project Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.tidb.bigdata.flink.format.cdc;

import io.tidb.bigdata.cdc.Codec;
import io.tidb.bigdata.cdc.Key.Type;
import java.io.IOException;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.table.data.RowData;
import org.apache.flink.util.Collector;

public class CraftDeserializationSchema extends CDCDeserializationSchema
        implements DeserializationSchema<RowData> {

    private static final long serialVersionUID = 1L;

    CraftDeserializationSchema(
            final CDCSchemaAdapter schema,
            @Nullable final Set<Type> eventTypes,
            @Nullable final Set<String> schemas,
            @Nullable final Set<String> tables,
            final long startTs,
            final boolean ignoreParseErrors,
            final boolean appendMode) {
        super(Codec.craft(), schema, eventTypes, schemas, tables, startTs, ignoreParseErrors, appendMode);
    }

    @Override
    public void deserialize(final byte[] message, final Collector<RowData> out) throws IOException {
        deserialize(null, message, out);
    }

    @Override
    public RowData deserialize(final byte[] message) {
        throw new IllegalStateException("A collector is required for deserializing.");
    }
}