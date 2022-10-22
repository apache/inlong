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
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.flink.table.data.RowData;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;

public class JsonDeserializationSchema extends CDCDeserializationSchema
        implements KafkaDeserializationSchema<RowData> {

    private static final long serialVersionUID = 1L;

    public JsonDeserializationSchema(
            final CDCSchemaAdapter schema,
            @Nullable final Set<Type> eventTypes,
            @Nullable final Set<String> schemas,
            @Nullable final Set<String> tables,
            final long startTs,
            final boolean ignoreParseErrors,
            final boolean appendMode) {
        super(Codec.json(), schema, eventTypes, schemas, tables, startTs,
                ignoreParseErrors, appendMode);
    }

    @Override
    public RowData deserialize(ConsumerRecord<byte[], byte[]> consumerRecord) {
        throw new IllegalStateException("A collector is required for deserializing.");
    }

    @Override
    public void deserialize(ConsumerRecord<byte[], byte[]> message, Collector<RowData> out)
            throws Exception {
        deserialize(message.key(), message.value(), out);
    }

}
