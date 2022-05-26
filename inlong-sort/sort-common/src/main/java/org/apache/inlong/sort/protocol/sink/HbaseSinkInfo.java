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

package org.apache.inlong.sort.protocol.sink;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.inlong.sort.protocol.FieldInfo;

import javax.annotation.Nullable;

/**
 * hbase sink resource info
 */
public class HbaseSinkInfo extends SinkInfo {

    private static final long serialVersionUID = -7651732809476005186L;

    @JsonProperty("zk_address")
    private final String zkAddress;

    @JsonProperty("zk_node")
    private final String zkNode;

    @JsonProperty("namespace")
    private final String namespace;

    @JsonProperty("table")
    private final String tableName;

    @JsonProperty("flush_max_size")
    private final String flushMaxSize;

    @JsonProperty("flush_max_rows")
    private final String flushMaxRows;

    @JsonProperty("flush_interval")
    private final String flushInterval;

    @JsonCreator
    public HbaseSinkInfo(
            @JsonProperty("fields") FieldInfo[] fields,
            @JsonProperty("zk_address") String zkAddress,
            @JsonProperty("zk_node") String zkNode,
            @JsonProperty("namespace") @Nullable String namespace,
            @JsonProperty("table") String tableName,
            @JsonProperty("flush_max_size") @Nullable String flushMaxSize,
            @JsonProperty("flush_max_rows") @Nullable String flushMaxRows,
            @JsonProperty("flush_interval") @Nullable String flushInterval) {
        super(fields);
        this.zkAddress = zkAddress;
        this.zkNode = zkNode;
        this.namespace = namespace;
        this.tableName = tableName;
        this.flushMaxSize = flushMaxSize;
        this.flushMaxRows = flushMaxRows;
        this.flushInterval = flushInterval;
    }

    @JsonProperty("zk_address")
    public String getZkAddress() {
        return zkAddress;
    }

    @JsonProperty("zk_node")
    public String getZkNode() {
        return zkNode;
    }

    @JsonProperty("namespace")
    public String getNamespace() {
        return namespace;
    }

    @JsonProperty("table")
    public String getTableName() {
        return tableName;
    }

    @JsonProperty("flush_max_size")
    public String getFlushMaxSize() {
        return flushMaxSize;
    }

    @JsonProperty("flush_max_rows")
    public String getFlushMaxRows() {
        return flushMaxRows;
    }

    @JsonProperty("flush_interval")
    public String getFlushInterval() {
        return flushInterval;
    }

}
