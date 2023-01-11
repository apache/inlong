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

import static org.apache.flink.shaded.guava18.com.google.common.base.Preconditions.checkState;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisConfigBase;
import org.apache.flink.table.data.RowData;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The Flink Redis Producer.
 */
public class RedisHashStaticKvPairSinkFunction
        extends
            AbstractRedisSinkFunction<Tuple4<Boolean, byte[], byte[], byte[]>> {

    public static final Logger LOG = LoggerFactory.getLogger(RedisHashStaticKvPairSinkFunction.class);

    public RedisHashStaticKvPairSinkFunction(
            SerializationSchema<RowData> serializationSchema,
            String address,
            long batchSize,
            Duration flushInterval,
            Duration configuration,
            FlinkJedisConfigBase flinkJedisConfigBase) {
        super(TypeInformation.of(new TypeHint<Tuple4<Boolean, byte[], byte[], byte[]>>() {
        }),
                serializationSchema,
                address,
                batchSize,
                flushInterval,
                configuration,
                flinkJedisConfigBase);
        LOG.info("Creating RedisHashStaticKvPairSinkFunction ...");
    }

    @Override
    public void open(Configuration parameters) {
        super.open(parameters);
        LOG.info("Open RedisHashStaticKvPairSinkFunction ...");
    }

    @Override
    protected List<Tuple4<Boolean, byte[], byte[], byte[]>> serialize(Tuple2<Boolean, Row> in) {
        Boolean upsert = in.f0;
        Row row = in.f1;

        Object rawKey = row.getField(0);
        Preconditions.checkState(rawKey instanceof String,
                "The first field type that considered data key of redis must be String.");
        byte[] keyBytes = ((String) rawKey).getBytes(StandardCharsets.UTF_8);

        checkState(row.getArity() % 2 == 1,
                "The number of elements must be odd, and even elements are field.");

        return IntStream.range(0, row.getArity() / 2)
                .boxed()
                .map(i -> {
                    Object fieldName = row.getField(i);

                    // Skip field name null condition.
                    if (fieldName == null) {
                        return null;
                    }

                    checkState(fieldName instanceof String,
                            "The field name type that considered data file of redis must be String.");
                    byte[] fieldBytes = ((String) fieldName).getBytes(StandardCharsets.UTF_8);

                    Object fieldValue = row.getField(i + 1);
                    byte[] valueBytes = null;
                    if (fieldValue != null) {
                        checkState(fieldValue instanceof String,
                                "The field value type that considered data file of redis must be String.");
                        valueBytes = ((String) fieldValue).getBytes(StandardCharsets.UTF_8);
                    }
                    return Tuple4.of(upsert, keyBytes, fieldBytes, valueBytes);
                }).collect(Collectors.toList());
    }

    @Override
    protected void flushInternal(List<Tuple4<Boolean, byte[], byte[], byte[]>> rows) {
        client.hashSetOrDelByPipeline(rows, expireTime);
    }
}
