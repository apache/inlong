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

package io.tidb.bigdata.flink.connector.source;

import io.tidb.bigdata.cdc.Key;
import io.tidb.bigdata.flink.format.cdc.CDCDeserializationSchemaBuilder;
import io.tidb.bigdata.flink.format.cdc.CDCMetadata;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Set;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.connector.source.SourceSplit;
import org.apache.flink.shaded.guava18.com.google.common.collect.ImmutableSet;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.flink.table.data.RowData;
import org.tikv.common.meta.TiTimestamp;

public abstract class CDCSourceBuilder<SplitT extends SourceSplit, EnumChkT> implements
        Serializable {

    public enum Type {
        KAFKA,
    }

    private static final Set<Key.Type> ROW_CHANGED_EVENT = ImmutableSet.of(Key.Type.ROW_CHANGED);

    public abstract Type type();

    protected abstract CDCSource<SplitT, EnumChkT>
    doBuild(DeserializationSchema<RowData> schema);

    protected abstract CDCSource<SplitT, EnumChkT>
    doBuild(KafkaDeserializationSchema<RowData> schema);

    public CDCSource<SplitT, EnumChkT> craft() {
        return doBuild(builder.craft());
    }

    public CDCSource<SplitT, EnumChkT> json() {
        return doBuild(builder.json());
    }

    private final CDCDeserializationSchemaBuilder builder;

    protected CDCSourceBuilder(CDCDeserializationSchemaBuilder builder) {
        this.builder = builder;
    }

    public static KafkaCDCSourceBuilder
    kafka(String database,
            String table,
            TiTimestamp ts,
            TiDBSchemaAdapter schema,
            boolean appendMode) {
        CDCMetadata[] cdcMetadata = null;
        TiDBMetadata[] metadata = schema.getMetadata();
        if (metadata != null) {
            cdcMetadata = Arrays.stream(metadata).map(TiDBMetadata::toCraft).toArray(CDCMetadata[]::new);
        }
        return new KafkaCDCSourceBuilder(
                new CDCDeserializationSchemaBuilder(schema.getPhysicalDataType(),
                        (ignored) -> schema.getProducedType())
                        .startTs(ts.getVersion())
                        .metadata(cdcMetadata)
                        .types(ROW_CHANGED_EVENT)
                        .schemas(ImmutableSet.of(database))
                        .tables(ImmutableSet.of(table))
                        .appendMode(appendMode));
    }

    public <T extends CDCSourceBuilder<SplitT, EnumChkT>> T ignoreParseErrors(boolean ignore) {
        this.builder.ignoreParseErrors(ignore);
        return (T) this;
    }
}