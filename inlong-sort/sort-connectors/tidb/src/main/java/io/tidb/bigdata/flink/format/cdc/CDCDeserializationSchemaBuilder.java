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

import io.tidb.bigdata.cdc.Key;
import java.util.Set;
import java.util.function.Function;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.util.Preconditions;

public class CDCDeserializationSchemaBuilder {

    public CDCDeserializationSchemaBuilder(final DataType physicalDataType,
            final Function<DataType, TypeInformation<RowData>> typeInfoFactory) {
        this.physicalDataType = Preconditions.checkNotNull(physicalDataType);
        this.typeInfoFactory = Preconditions.checkNotNull(typeInfoFactory);
    }

    private final DataType physicalDataType;
    private final Function<DataType, TypeInformation<RowData>> typeInfoFactory;
    private CDCMetadata[] metadata;
    private Set<Key.Type> eventTypes;
    private Set<String> schemas;
    private Set<String> tables;
    private long startTs;
    private boolean ignoreParseErrors;
    private boolean appendMode;

    public CDCDeserializationSchemaBuilder metadata(final CDCMetadata[] metadata) {
        this.metadata = metadata;
        return this;
    }

    public CDCDeserializationSchemaBuilder types(final Set<Key.Type> eventTypes) {
        this.eventTypes = eventTypes;
        return this;
    }

    public CDCDeserializationSchemaBuilder schemas(final Set<String> schemas) {
        this.schemas = schemas;
        return this;
    }

    public CDCDeserializationSchemaBuilder tables(final Set<String> tables) {
        this.tables = tables;
        return this;
    }

    public CDCDeserializationSchemaBuilder startTs(final long ts) {
        this.startTs = ts;
        return this;
    }

    public CDCDeserializationSchemaBuilder ignoreParseErrors(boolean ignore) {
        this.ignoreParseErrors = ignore;
        return this;
    }

    public CDCDeserializationSchemaBuilder appendMode(final boolean appendMode) {
        this.appendMode = appendMode;
        return this;
    }

    public CraftDeserializationSchema craft() {
        return new CraftDeserializationSchema(
                new CDCSchemaAdapter(physicalDataType, typeInfoFactory, metadata), eventTypes,
                schemas, tables, startTs, ignoreParseErrors, appendMode);
    }

    public JsonDeserializationSchema json() {
        return new JsonDeserializationSchema(
                new CDCSchemaAdapter(physicalDataType, typeInfoFactory, metadata), eventTypes,
                schemas, tables, startTs, ignoreParseErrors, appendMode);
    }
}