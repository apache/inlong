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

package org.apache.inlong.sort.flink.hive.partition;

import java.io.IOException;
import java.io.Serializable;
import java.util.Arrays;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.inlong.sort.util.InstantiationUtil;

/**
 * It represents partitions of Hive.
 */
public class HivePartition implements Serializable {

    private static final long serialVersionUID = 7328081124432216741L;

    private Tuple2<String, String>[] partitions;

    public HivePartition() {

    }

    public Tuple2<String, String>[] getPartitions() {
        return partitions;
    }

    public void setPartitions(Tuple2<String, String>[] partitions) {
        this.partitions = partitions;
    }

    public String generatePartitionPath() {
        return PartitionPathUtils.generatePartitionPath(this);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        HivePartition that = (HivePartition) o;
        return Arrays.equals(partitions, that.partitions);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(partitions);
    }

    @Override
    public String toString() {
        return generatePartitionPath();
    }

    public static class VersionedSerializer implements SimpleVersionedSerializer<HivePartition> {

        public static final VersionedSerializer INSTANCE = new VersionedSerializer();

        @Override
        public int getVersion() {
            return 1;
        }

        @Override
        public byte[] serialize(HivePartition hivePartition) throws IOException {
            return InstantiationUtil.serializeObject(hivePartition);
        }

        @Override
        public HivePartition deserialize(int version, byte[] serialized) throws IOException {
            try {
                return InstantiationUtil.deserializeObject(serialized, Thread.currentThread().getContextClassLoader());
            } catch (ClassNotFoundException e) {
                throw new IOException(e);
            }
        }
    }
}
