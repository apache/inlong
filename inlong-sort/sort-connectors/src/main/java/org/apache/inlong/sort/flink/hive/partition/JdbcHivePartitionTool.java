/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */

package org.apache.inlong.sort.flink.hive.partition;

import java.util.ArrayList;
import java.util.List;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.inlong.sort.configuration.Configuration;
import org.apache.inlong.sort.flink.hive.partition.PartitionCommitPolicy.Context;
import org.apache.inlong.sort.protocol.FieldInfo;
import org.apache.inlong.sort.protocol.sink.HiveSinkInfo;
import org.apache.inlong.sort.protocol.sink.HiveSinkInfo.HiveFieldPartitionInfo;
import org.apache.inlong.sort.protocol.sink.HiveSinkInfo.HivePartitionInfo;
import org.apache.inlong.sort.protocol.sink.HiveSinkInfo.TextFileFormat;
import org.apache.inlong.sort.util.ParameterTool;

public class JdbcHivePartitionTool {
    public static void main(String[] args) throws Exception {
        final Configuration config = ParameterTool.fromArgs(args).getConfiguration();
        final List<HivePartitionInfo> partitions = new ArrayList<>();
        final List<String> partitionValues = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            String partitionName = config.getString("partition_" + i + "_name", null);
            if (partitionName != null) {
                partitions.add(new HiveFieldPartitionInfo(partitionName));
                partitionValues.add(config.getString("partition_" + i + "_value", ""));
            }
        }
        final String database = config.getString("database", null);
        final String table = config.getString("table", null);
        HiveSinkInfo hiveSinkInfo =
                new HiveSinkInfo(
                        new FieldInfo[0],
                        config.getString("metastore_address", null),
                        database,
                        table,
                        config.getString("username", null),
                        config.getString("password", null),
                        config.getString("root_path", null),
                        partitions.toArray(new HivePartitionInfo[0]),
                        new TextFileFormat("\t".charAt(0)));
        JdbcHivePartitionCommitPolicy committer = new JdbcHivePartitionCommitPolicy(config, hiveSinkInfo);
        try {
            committer.commit(new Context() {
                @Override
                public String databaseName() {
                    return database;
                }

                @Override
                public String tableName() {
                    return table;
                }

                @Override
                public HivePartition partition() {
                    HivePartition hivePartition = new HivePartition();
                    List<Tuple2<String, String>> partitionPairs = new ArrayList<>();
                    for (int i = 0; i < partitions.size(); i++) {
                        partitionPairs.add(Tuple2.of(partitions.get(i).getFieldName(), partitionValues.get(i)));
                    }
                    //noinspection unchecked
                    hivePartition.setPartitions(partitionPairs.toArray(new Tuple2[0]));
                    return hivePartition;
                }
            });
        } finally {
            committer.close();
        }
    }
}
