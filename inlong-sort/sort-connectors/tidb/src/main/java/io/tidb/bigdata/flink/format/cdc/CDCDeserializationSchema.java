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
import io.tidb.bigdata.cdc.Event;
import io.tidb.bigdata.cdc.Key.Type;
import io.tidb.bigdata.cdc.RowChangedValue;
import java.io.IOException;
import java.io.Serializable;
import java.util.Objects;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.Collector;

public abstract class CDCDeserializationSchema
        implements Serializable, ResultTypeQueryable<RowData> {

    private static final long serialVersionUID = 1L;

    /**
     * Only read changelogs of some specific types.
     */
    @Nullable
    private final Set<Type> eventTypes;

    /**
     * Only read changelogs from the specific schemas.
     */
    @Nullable
    private final Set<String> schemas;

    /**
     * Only read changelogs from the specific tables.
     */
    @Nullable
    private final Set<String> tables;

    /**
     * Only read changelogs with commit ts equals or greater than this
     */
    private final long startTs;

    /**
     * Flag indicating whether to ignore invalid fields/rows (default: throw an exception).
     */
    private final boolean ignoreParseErrors;

    /**
     * Adapter for CDC data model and Flink Schema
     */
    private final CDCSchemaAdapter schema;

    private final Codec codec;

    protected boolean appendMode;

    protected CDCDeserializationSchema(
            final Codec codec,
            final CDCSchemaAdapter schema,
            @Nullable final Set<Type> eventTypes,
            @Nullable final Set<String> schemas,
            @Nullable final Set<String> tables,
            final long startTs,
            final boolean ignoreParseErrors,
            final boolean appendMode) {
        this.schema = schema;
        this.codec = codec;
        this.eventTypes = eventTypes;
        this.schemas = schemas;
        this.tables = tables;
        this.startTs = startTs;
        this.ignoreParseErrors = ignoreParseErrors;
        this.appendMode = appendMode;
    }

    private boolean acceptEvent(Event event) {
        if (event.getTs() < startTs) {
            // skip not events that have ts older than specific
            return false;
        }
        // skip not interested types
        return eventTypes == null || eventTypes.contains(event.getType());
    }

    private boolean acceptSchemaAndTable(String schema, String table) {
        if (schemas != null && !schemas.contains(schema)) {
            return false;
        }
        return tables == null || tables.contains(table);
    }

    private void collectRowChanged(final Event event, final Collector<RowData> out) {
        final RowChangedValue value = event.asRowChanged();
        final RowChangedValue.Type type = value.getType();

        GenericRowData row = null;
        GenericRowData row2 = null;
        switch (type) {
            case DELETE:
                row = schema.convert(event, value.getOldValue()).delete();
                break;
            case INSERT:
                row = schema.convert(event, value.getNewValue()).insert();
                break;
            case UPDATE:
                CDCSchemaAdapter.RowBuilder builder = schema.convert(event, value.getOldValue());
                CDCSchemaAdapter.RowBuilder builder2 = schema.convert(event, value.getNewValue());
                if (Objects.equals(builder, builder2)) {
                    // All selected data remain the same, skip this event
                    return;
                }
                row = builder.updateBefore();
                row2 = builder2.updateAfter();
                break;
            default:
                if (!ignoreParseErrors) {
                    throw new IllegalArgumentException("Unknown row changed event " + type);
                }
        }

        if (appendMode) {
            row.setRowKind(RowKind.INSERT);
        }

        out.collect(row);
        if (row2 != null) {
            if (appendMode) {
                row2.setRowKind(RowKind.INSERT);
            }
            out.collect(row2);
        }
    }

    private void collectDDL(final Event event, final Collector<RowData> out) {
        out.collect(schema.convert(event).insert());
    }

    private void collectResolved(final Event event, final Collector<RowData> out) {
        out.collect(schema.convert(event).insert());
    }

    // ------------------------------------------------------------------------------------------
    private void collectEvent(final Event event, final Collector<RowData> out) {
        if (!acceptEvent(event)) {
            return;
        }
        Type type = event.getType();
        if (type == Type.RESOLVED) {
            // Resolved event is always eligible even though they don't have schema and table specified
            collectResolved(event, out);
            return;
        }
        if (!acceptSchemaAndTable(event.getSchema(), event.getTable())) {
            return;
        }
        switch (type) {
            case ROW_CHANGED:
                collectRowChanged(event, out);
                break;
            case DDL:
                collectDDL(event, out);
                break;
            default:
                throw new IllegalStateException("Unknown event type: " + event.getType());
        }
    }

    public void deserialize(
            final byte[] key, final byte[] value, final Collector<RowData> out) throws IOException {
        try {
            for (final Event event : codec.decode(key, value)) {
                collectEvent(event, out);
            }
        } catch (Throwable throwable) {
            if (!ignoreParseErrors) {
                throw new IOException("Corrupted TiCDC craft message.", throwable);
            }
        }
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        CDCDeserializationSchema that = (CDCDeserializationSchema) o;
        return Objects.equals(schema, that.schema)
                && Objects.equals(eventTypes, that.eventTypes)
                && Objects.equals(schemas, that.schemas)
                && Objects.equals(tables, that.tables)
                && (startTs == that.startTs)
                && (ignoreParseErrors == that.ignoreParseErrors);
    }

    @Override
    public int hashCode() {
        return Objects.hash(schema, eventTypes, schemas, tables, startTs, ignoreParseErrors);
    }

    public boolean isEndOfStream(final RowData nextElement) {
        return false;
    }

    @Override
    public TypeInformation<RowData> getProducedType() {
        return schema.getProducedType();
    }
}