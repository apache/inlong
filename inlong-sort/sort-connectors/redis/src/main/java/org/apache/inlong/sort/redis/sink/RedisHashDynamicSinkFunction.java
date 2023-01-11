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
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
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
public class RedisHashDynamicSinkFunction
        extends
            AbstractRedisSinkFunction<Tuple4<Boolean, byte[], byte[], byte[]>> {

    public static final Logger LOG = LoggerFactory.getLogger(RedisBitmapStaticKvPairSinkFunction.class);

    public RedisHashDynamicSinkFunction(
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

        LOG.info("Creating RedisHashDynamicSinkFunction ...");
    }

    @Override
    public void open(Configuration parameters) {
        super.open(parameters);
        LOG.info("Open RedisHashDynamicSinkFunction ...");
    }

    @Override
    protected List<Tuple4<Boolean, byte[], byte[], byte[]>> serialize(Tuple2<Boolean, Row> in) {
        Boolean upsert = in.f0;
        Row row = in.f1;

        Object rawKey = row.getField(0);
        Preconditions.checkState(rawKey instanceof String,
                "The first field type that considered data key of redis must be String.");
        byte[] keyBytes = ((String) rawKey).getBytes(StandardCharsets.UTF_8);

        Object secondField = row.getField(1);
        checkState(secondField instanceof Map, "The second field type that considered data file of redis " +
                "must be Map.");

        Map<String, String> fieldMap = (Map<String, String>) secondField;

        return fieldMap
                .entrySet()
                .stream()
                .map(entry -> {
                    byte[] fieldKey = entry.getKey().getBytes(StandardCharsets.UTF_8);
                    String value = entry.getValue();

                    byte[] fieldValue = null;
                    if (StringUtils.isNotEmpty(value)) {
                        fieldValue = value.getBytes(StandardCharsets.UTF_8);
                    }
                    return Tuple4.of(upsert, keyBytes, fieldKey, fieldValue);
                })
                .collect(Collectors.toList());

    }

    @Override
    protected void flushInternal(List<Tuple4<Boolean, byte[], byte[], byte[]>> rows) {
        client.hashSetOrDelByPipeline(rows, expireTime);
    }
}
