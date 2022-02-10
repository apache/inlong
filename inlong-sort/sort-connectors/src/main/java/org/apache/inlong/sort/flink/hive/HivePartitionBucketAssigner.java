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

package org.apache.inlong.sort.flink.hive;

import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.inlong.sort.flink.filesystem.BucketAssigner;
import org.apache.inlong.sort.flink.hive.partition.HivePartition;
import org.apache.inlong.sort.flink.hive.partition.HivePartition.VersionedSerializer;
import org.apache.inlong.sort.flink.hive.partition.PartitionComputer;

public class HivePartitionBucketAssigner<T> implements BucketAssigner<T, HivePartition> {

    private static final long serialVersionUID = -7943062469842167739L;

    private final PartitionComputer<T> computer;

    public HivePartitionBucketAssigner(PartitionComputer<T> computer) {
        this.computer = computer;
    }

    @Override
    public HivePartition getBucketId(T element, Context context) {
        try {
            return computer.generatePartValues(element);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public SimpleVersionedSerializer<HivePartition> getSerializer() {
        return VersionedSerializer.INSTANCE;
    }
}