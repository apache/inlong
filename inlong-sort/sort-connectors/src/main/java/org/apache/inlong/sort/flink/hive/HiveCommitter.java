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

import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.inlong.sort.configuration.Constants.SINK_HIVE_COMMITTED_PARTITIONS_CACHE_SIZE;

import java.util.LinkedHashMap;
import java.util.Map;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.inlong.sort.configuration.Configuration;
import org.apache.inlong.sort.flink.hive.partition.HivePartition;
import org.apache.inlong.sort.flink.hive.partition.JdbcHivePartitionCommitPolicy;
import org.apache.inlong.sort.flink.hive.partition.PartitionCommitInfo;
import org.apache.inlong.sort.flink.hive.partition.PartitionCommitPolicy;
import org.apache.inlong.sort.protocol.sink.HiveSinkInfo;

/**
 * Hive sink committer.
 */
public class HiveCommitter extends RichSinkFunction<PartitionCommitInfo> {

    private static final long serialVersionUID = 3070346386139023521L;

    private final Configuration configuration;

    private final HiveSinkInfo hiveSinkInfo;

    private final PartitionCommitPolicy.Factory commitPolicyFactory;

    private transient Map<HivePartition, Boolean> committedPartitions;

    private transient PartitionCommitPolicy policy;

    private transient CommitPolicyContextImpl commitPolicyContext;

    public HiveCommitter(
            Configuration configuration,
            HiveSinkInfo hiveSinkInfo,
            PartitionCommitPolicy.Factory commitPolicyFactory) {
        this.configuration = configuration;
        this.hiveSinkInfo = hiveSinkInfo;
        this.commitPolicyFactory = checkNotNull(commitPolicyFactory);
    }

    public HiveCommitter(
            Configuration configuration,
            HiveSinkInfo hiveSinkInfo) {
        this(configuration, hiveSinkInfo, new JdbcHivePartitionCommitPolicy.Factory());
    }

    @Override
    public void open(org.apache.flink.configuration.Configuration parameters) throws Exception {
        commitPolicyContext = new CommitPolicyContextImpl();
        policy = commitPolicyFactory.create(configuration, hiveSinkInfo);
        final int cacheSize = configuration.getInteger(SINK_HIVE_COMMITTED_PARTITIONS_CACHE_SIZE);
        //noinspection serial
        committedPartitions = new LinkedHashMap<HivePartition, Boolean>(cacheSize, 0.75f, true) {
            @Override
            protected boolean removeEldestEntry(Map.Entry<HivePartition, Boolean> eldest) {
                return size() > cacheSize;
            }
        };
    }

    @Override
    public void close() throws Exception {
        committedPartitions.clear();
        policy.close();
    }

    @Override
    public void invoke(PartitionCommitInfo value, Context context) throws Exception {
        for (HivePartition partition : value.getPartitions()) {
            if (committedPartitions.get(partition) == null) {
                committedPartitions.put(partition, true);
                policy.commit(commitPolicyContext.setHivePartition(partition));
            }
        }
    }

    private class CommitPolicyContextImpl implements PartitionCommitPolicy.Context {

        private HivePartition hivePartition;

        public CommitPolicyContextImpl setHivePartition(HivePartition hivePartition) {
            this.hivePartition = hivePartition;
            return this;
        }

        @Override
        public String databaseName() {
            return hiveSinkInfo.getDatabaseName();
        }

        @Override
        public String tableName() {
            return hiveSinkInfo.getTableName();
        }

        @Override
        public HivePartition partition() {
            return hivePartition;
        }
    }
}
