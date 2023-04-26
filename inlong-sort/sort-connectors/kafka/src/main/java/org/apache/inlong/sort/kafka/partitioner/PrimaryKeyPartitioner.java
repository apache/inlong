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

package org.apache.inlong.sort.kafka.partitioner;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkKafkaPartitioner;
import org.apache.flink.util.Preconditions;
import org.apache.inlong.sort.base.format.AbstractDynamicSchemaFormat;
import org.apache.inlong.sort.base.format.DynamicSchemaFormatFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;

/**
 * The PrimaryKey Partitioner is used to extract primary key from single table messages:
 *
 * @param <T>
 */
public class PrimaryKeyPartitioner<T> extends FlinkKafkaPartitioner<T> {

    private static final Logger LOG = LoggerFactory.getLogger(PrimaryKeyPartitioner.class);
    private static final long serialVersionUID = 1L;

    @Override
    public void open(int parallelInstanceId, int parallelInstances) {
        super.open(parallelInstanceId, parallelInstances);
    }

    @Override
    @SuppressWarnings({"unchecked"})
    public int partition(T record, byte[] key, byte[] value, String targetTopic, int[] partitions) {
        Preconditions.checkArgument(
                partitions != null && partitions.length > 0,
                "Partitions of the target topic is empty.");
        // single table currently support: avro/csv/json/canal
        try {
            // used for avro/csv/json formats
            if (key != null) {
                return partitions[(Arrays.hashCode(key) & Integer.MAX_VALUE) % partitions.length];
            } else {
                // used for canal-json formats, since single table does not support debezium-json for now.
                AbstractDynamicSchemaFormat dynamicSchemaFormat = DynamicSchemaFormatFactory.getFormat("canal-json");
                List<String> values = dynamicSchemaFormat.extractPrimaryKeyValues(value);
                if (values == null || values.isEmpty()) {
                    return 0;
                }
                return partitions[(StringUtils.join(values, "").hashCode()
                        & Integer.MAX_VALUE) % partitions.length];
            }
        } catch (Exception e) {
            LOG.warn("Extract partition failed", e);
        }
        // If kafka can't parse, then at least write everything to the first partition.
        return 0;
    }

    @Override
    public boolean equals(Object o) {
        return o instanceof PrimaryKeyPartitioner;
    }

    @Override
    public int hashCode() {
        return PrimaryKeyPartitioner.class.hashCode();
    }
}
