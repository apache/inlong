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
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisConfigBase;
import org.apache.flink.table.data.RowData;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Protocol;

/**
 * The Flink Redis Producer.
 */
public class RedisBitmapStaticKvPairSinkFunction
        extends
            AbstractRedisSinkFunction<Tuple3<Boolean, byte[], Map<Long, byte[]>>> {

    public static final Logger LOG = LoggerFactory.getLogger(RedisBitmapStaticKvPairSinkFunction.class);

    public RedisBitmapStaticKvPairSinkFunction(
            SerializationSchema<RowData> serializationSchema,
            String address,
            long batchSize,
            Duration flushInterval,
            Duration configuration,
            FlinkJedisConfigBase flinkJedisConfigBase) {
        super(TypeInformation.of(new TypeHint<Tuple3<Boolean, byte[], Map<Long, byte[]>>>() {
        }),
                serializationSchema,
                address,
                batchSize,
                flushInterval,
                configuration,
                flinkJedisConfigBase);
        LOG.info("Creating RedisBitmapStaticKvPairSinkFunction ...");
    }

    @Override
    public void open(Configuration parameters) {
        super.open(parameters);
        LOG.info("Opening RedisBitmapStaticKvPairSinkFunction ...");
    }

    @Override
    protected List<Tuple3<Boolean, byte[], Map<Long, byte[]>>> serialize(Tuple2<Boolean, Row> in) {
        Boolean rowKind = in.f0;
        Row row = in.f1;

        Object rawKey = row.getField(0);
        Preconditions.checkState(rawKey instanceof String,
                "The first field type that considered data key of redis must be String.");
        byte[] keyBytes = ((String) rawKey).getBytes(StandardCharsets.UTF_8);

        checkState(row.getArity() % 2 == 1,
                "The number of elements must be odd, and all even elements are field.");

        Map<Long, byte[]> map = new HashMap<>(row.getArity() / 2);
        long dataByteSize = keyBytes.length;
        for (int i = 1; i + 1 < row.getArity(); i += 2) {
            Object offsetName = row.getField(i);

            // Skip field name null condition.
            if (offsetName == null) {
                continue;
            }

            checkState(offsetName instanceof Long,
                    "The field name type that considered data file of redis must be Long.");
            Long offset = Long.parseLong(offsetName.toString());
            dataByteSize += Long.SIZE;

            Object fieldValue = row.getField(i + 1);
            byte[] valueBytes = null;
            if (fieldValue != null) {
                checkState(fieldValue instanceof Boolean,
                        "The field value type that considered data file of redis must be Boolean.");
                valueBytes = Protocol.toByteArray((Boolean) fieldValue);
                dataByteSize += valueBytes.length;
            }

            map.put(offset, valueBytes);
        }

        return Collections.singletonList(Tuple3.of(rowKind, keyBytes, map));
    }

    @Override
    protected void flushInternal(List<Tuple3<Boolean, byte[], Map<Long, byte[]>>> rows) {
        client.bitSetOrDelByPipeline(rows, expireTime);
    }
}
