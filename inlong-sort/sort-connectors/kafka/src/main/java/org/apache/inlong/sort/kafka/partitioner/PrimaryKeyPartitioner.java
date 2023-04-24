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

import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkKafkaPartitioner;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The PrimaryKey Partitioner is used to extract primary key from single table messages:
 *
 * @param <T>
 */
public class PrimaryKeyPartitioner<T> extends FlinkKafkaPartitioner<T> {

    private static final Logger LOG = LoggerFactory.getLogger(RawDataHashPartitioner.class);
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
        try {
            return partitions[(key.hashCode() & Integer.MAX_VALUE) % partitions.length];
        } catch (Exception e) {
            LOG.warn("Extract partition failed", e);
        }
        // If kafka can't parse, then at least write everything to the first partition.
        return 0;
    }

    @Override
    public boolean equals(Object o) {
        return o instanceof RawDataHashPartitioner;
    }

    @Override
    public int hashCode() {
        return RawDataHashPartitioner.class.hashCode();
    }
}
