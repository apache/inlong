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

package org.apache.inlong.sort.redis.sink;

import static org.apache.flink.api.java.ClosureCleaner.ensureSerializable;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisConfigBase;
import org.apache.flink.table.data.RowData;
import org.apache.flink.types.Row;

/**
 * The Flink Redis Producer.
 */
public class RedisPlainSinkFunction
        extends
            AbstractRedisSinkFunction<Tuple3<Boolean, byte[], byte[]>> {

    private static final long serialVersionUID = 1L;

    public RedisPlainSinkFunction(
            SerializationSchema<RowData> serializationSchema,
            String address,
            long batchSize,
            Duration flushInterval,
            Duration configuration,
            FlinkJedisConfigBase flinkJedisConfigBase) {
        super(
                TypeInformation.of(new TypeHint<Tuple3<Boolean, byte[], byte[]>>() {
                }),
                serializationSchema,
                address,
                batchSize,
                flushInterval,
                configuration,
                flinkJedisConfigBase);
        checkNotNull(serializationSchema, "The serialization schema must not be null.");
        ensureSerializable(serializationSchema);
    }

    @Override
    protected List<Tuple3<Boolean, byte[], byte[]>> serialize(Tuple2<Boolean, Row> in) {
        Row row = in.f1;

        Object rawKey = row.getField(0);
        checkState(rawKey instanceof String,
                "The first field type that considered data key of redis must be String.");
        byte[] keyBytes = ((String) rawKey).getBytes(StandardCharsets.UTF_8);

        Row value = new Row(row.getArity() - 1);
        for (int i = 0; i < row.getArity() - 1; ++i) {
            value.setField(i, row.getField(i + 1));
        }

        byte[] valueBytes = serializationSchema.serialize(value);
        return Collections.singletonList(Tuple3.of(in.f0, keyBytes, valueBytes));
    }

    @Override
    protected void flushInternal(List<Tuple3<Boolean, byte[], byte[]>> rows) {
        client.setOrDelByPipeline(rows, expireTime);
    }
}
