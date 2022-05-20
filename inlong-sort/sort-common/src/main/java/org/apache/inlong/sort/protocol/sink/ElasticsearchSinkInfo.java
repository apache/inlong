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

public class ElasticsearchSinkInfo extends SinkInfo {

    private static final long serialVersionUID = 1L;

    @JsonProperty("host")
    private final String host;

    @JsonProperty("port")
    private final Integer port;

    @JsonProperty("indexName")
    private final String indexName;

    @JsonProperty("username")
    private final String username;

    @JsonProperty("password")
    private final String password;

    @JsonProperty("flush_interval")
    private final int flushInterval;

    @JsonProperty("flush_record_number")
    private final int flushRecordNumber;

    @JsonProperty("write_max_retry_times")
    private final int writeMaxRetryTimes;

    @JsonCreator
    public ElasticsearchSinkInfo(
            @JsonProperty("host") String host,
            @JsonProperty("port") Integer port,
            @JsonProperty("indexName") String indexName,
            @JsonProperty("username") String username,
            @JsonProperty("password") String password,
            @JsonProperty("fields") FieldInfo[] fields,
            @JsonProperty("key_field_names") String[] keyFieldNames,
            @JsonProperty("flush_interval") int flushInterval,
            @JsonProperty("flush_record_number") int flushRecordNumber,
            @JsonProperty("write_max_retry_times") int writeMaxRetryTimes) {
        super(fields);
        this.host = Preconditions.checkNotNull(host);
        this.port = port;
        this.indexName = Preconditions.checkNotNull(indexName);
        this.username = username;
        this.password = password;
        this.flushInterval = flushInterval;
        this.flushRecordNumber = flushRecordNumber;
        this.writeMaxRetryTimes = writeMaxRetryTimes;
    }

    @JsonProperty("host")
    public String getHost() {
        return host;
    }

    @JsonProperty("port")
    public Integer getPort() {
        return port;
    }

    @JsonProperty("indexName")
    public String getIndexName() {
        return indexName;
    }

    @JsonProperty("username")
    public String getUsername() {
        return username;
    }

    @JsonProperty("password")
    public String getPassword() {
        return password;
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
