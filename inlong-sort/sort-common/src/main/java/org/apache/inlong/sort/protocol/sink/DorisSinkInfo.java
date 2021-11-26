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

public class DorisSinkInfo extends SinkInfo {



    private static final long serialVersionUID = 1L;

    @JsonProperty("table_identifier")
    private final String tableIdentifier;


    @JsonProperty("username")
    private final String username;

    @JsonProperty("password")
    private final String password;


    @JsonProperty("fe_nodes")
    private final String fenodes;


    @JsonProperty("batch_size")
    private final int batchSize;

    @JsonProperty("max_retries")
    private final int maxRetries;

    @JsonProperty("flush_interval_second")
    private final int flushIntervalSecond;

    @JsonCreator
    public DorisSinkInfo(
            @JsonProperty("table_identifier") String tableIdentifier,
            @JsonProperty("username") String username,
            @JsonProperty("password") String password,
            @JsonProperty("fe_nodes") String fenodes,
            @JsonProperty("fields") FieldInfo[] fields,
            @JsonProperty("batch_size") int batchSize,
            @JsonProperty("max_retries") int maxRetries,
            @JsonProperty("flush_interval_second") int flushIntervalSecond) {
        super(fields);
        this.tableIdentifier = Preconditions.checkNotNull(tableIdentifier);
        this.username = username;
        this.password = password;
        this.fenodes = Preconditions.checkNotNull(fenodes);
        this.batchSize = batchSize;
        this.maxRetries = maxRetries;
        this.flushIntervalSecond = flushIntervalSecond;
    }



    @JsonProperty("username")
    public String getUsername() {
        return username;
    }

    @JsonProperty("password")
    public String getPassword() {
        return password;
    }

    @JsonProperty("table_identifier")
    public String getTableIdentifier() {
        return tableIdentifier;
    }

    @JsonProperty("fe_nodes")
    public String getFenodes() {
        return fenodes;
    }

    @JsonProperty("batch_size")
    public int getBatchSize() {
        return batchSize;
    }

    @JsonProperty("max_retries")
    public int getMaxRetries() {
        return maxRetries;
    }

    @JsonProperty("flush_interval_second")
    public int getFlushIntervalSecond() {
        return flushIntervalSecond;
    }
}
