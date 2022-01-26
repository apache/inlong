/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.inlong.sort.flink.hive.partition;

import java.io.Serializable;
import org.apache.flink.annotation.Experimental;
import org.apache.inlong.sort.configuration.Configuration;
import org.apache.inlong.sort.protocol.sink.HiveSinkInfo;

/**
 * Policy for commit a partition.
 */
@Experimental
public interface PartitionCommitPolicy {

    /**
     * Commit a partition.
     */
    void commit(Context context) throws Exception;

    void close() throws Exception;

    /**
     * Context of policy, including table information and partition information.
     */
    interface Context {
        /**
         * Database name of this table.
         */
        String databaseName();

        /**
         * Table name.
         */
        String tableName();

        /**
         * Hive partitions.
         */
        HivePartition partition();
    }

    interface Factory extends Serializable {
        PartitionCommitPolicy create(Configuration configuration, HiveSinkInfo hiveSinkInfo) throws Exception;
    }
}
