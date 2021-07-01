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

package org.apache.inlong.sort.protocol.sink;

import com.google.common.base.Preconditions;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.inlong.sort.protocol.FieldInfo;

public class ClickHouseSinkInfo extends SinkInfo {

    public enum PartitionStrategy {
        BALANCE,
        RANDOM,
        HASH
    }

    private static final long serialVersionUID = 1L;

    @JsonProperty("url")
    private final String url;

    @JsonProperty("database")
    private final String databaseName;

    @JsonProperty("table")
    private final String tableName;

    @JsonProperty("username")
    private final String username;

    @JsonProperty("password")
    private final String password;

    @JsonProperty("distributed_table")
    private final boolean isDistributedTable;

    @JsonProperty("partition_strategy")
    private final PartitionStrategy partitionStrategy;

    @JsonProperty("partition_key")
    private final String partitionKey;

    @JsonProperty("key_field_names")
    private final String[] keyFieldNames;

    @JsonProperty("flush_interval")
    private final int flushInterval;

    @JsonProperty("flush_record_number")
    private final int flushRecordNumber;

    @JsonProperty("write_max_retry_times")
    private final int writeMaxRetryTimes;

    @JsonCreator
    public ClickHouseSinkInfo(
            @JsonProperty("url") String url,
            @JsonProperty("database") String databaseName,
            @JsonProperty("table") String tableName,
            @JsonProperty("username") String username,
            @JsonProperty("password") String password,
            @JsonProperty("distributed_table") boolean isDistributedTable,
            @JsonProperty("partition_strategy") PartitionStrategy partitionStrategy,
            @JsonProperty("partition_key") String partitionKey,
            @JsonProperty("fields") FieldInfo[] fields,
            @JsonProperty("key_field_names") String[] keyFieldNames,
            @JsonProperty("flush_interval") int flushInterval,
            @JsonProperty("flush_record_number") int flushRecordNumber,
            @JsonProperty("write_max_retry_times") int writeMaxRetryTimes) {
        super(fields);
        this.url = Preconditions.checkNotNull(url);
        this.databaseName = Preconditions.checkNotNull(databaseName);
        this.tableName = Preconditions.checkNotNull(tableName);
        this.username = username;
        this.password = password;
        this.isDistributedTable = isDistributedTable;
        this.partitionStrategy = Preconditions.checkNotNull(partitionStrategy);
        this.partitionKey = Preconditions.checkNotNull(partitionKey);
        this.keyFieldNames = Preconditions.checkNotNull(keyFieldNames);
        this.flushInterval = flushInterval;
        this.flushRecordNumber = flushRecordNumber;
        this.writeMaxRetryTimes = writeMaxRetryTimes;
    }

    @JsonProperty("url")
    public String getUrl() {
        return url;
    }

    @JsonProperty("database")
    public String getDatabaseName() {
        return databaseName;
    }

    @JsonProperty("table")
    public String getTableName() {
        return tableName;
    }

    @JsonProperty("username")
    public String getUsername() {
        return username;
    }

    @JsonProperty("password")
    public String getPassword() {
        return password;
    }

    @JsonProperty("distributed_table")
    public boolean isDistributedTable() {
        return isDistributedTable;
    }

    @JsonProperty("partition_strategy")
    public PartitionStrategy getPartitionStrategy() {
        return partitionStrategy;
    }

    @JsonProperty("partition_key")
    public String getPartitionKey() {
        return partitionKey;
    }

    @JsonProperty("key_field_names")
    public String[] getKeyFieldNames() {
        return keyFieldNames;
    }

    @JsonProperty("flush_interval")
    public int getFlushInterval() {
        return flushInterval;
    }

    @JsonProperty("flush_record_number")
    public int getFlushRecordNumber() {
        return flushRecordNumber;
    }

    @JsonProperty("write_max_retry_times")
    public int getWriteMaxRetryTimes() {
        return writeMaxRetryTimes;
    }
}
