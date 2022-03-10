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

package org.apache.inlong.manager.client.api.sink;

import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import org.apache.inlong.manager.client.api.DataFormat;
import org.apache.inlong.manager.client.api.SinkField;
import org.apache.inlong.manager.client.api.StreamSink;
import org.apache.inlong.manager.client.api.auth.DefaultAuthentication;
import org.apache.inlong.manager.common.enums.SinkType;

import java.util.List;

@Data
@EqualsAndHashCode(callSuper = true)
@NoArgsConstructor
@AllArgsConstructor
@ApiModel("Clickhouse sink configuration")
public class ClickHouseSink extends StreamSink {

    @ApiModelProperty(value = "Sink type", required = true)
    private SinkType sinkType = SinkType.CLICKHOUSE;

    @ApiModelProperty("ClickHouse JDBC URL")
    private String jdbcUrl;

    @ApiModelProperty("Target database name")
    private String databaseName;

    @ApiModelProperty("Target table name")
    private String tableName;

    @ApiModelProperty("Authentication for clickhouse")
    private DefaultAuthentication authentication;

    @ApiModelProperty("Whether distributed table")
    private Boolean distributedTable;

    @ApiModelProperty("Partition strategy,support: BALANCE, RANDOM, HASH")
    private String partitionStrategy;

    @ApiModelProperty("Partition key")
    private String partitionKey;

    @ApiModelProperty("Key field names")
    private String[] keyFieldNames;

    @ApiModelProperty("Flush interval")
    private Integer flushInterval;

    @ApiModelProperty("Flush record number")
    private Integer flushRecordNumber;

    @ApiModelProperty("Write max retry times")
    private Integer writeMaxRetryTimes;

    @ApiModelProperty("Create topic or not")
    private boolean needCreated;

    @ApiModelProperty("Field definitions for clickhouse")
    private List<SinkField> sinkFields;

    @Override
    public DataFormat getDataFormat() {
        return DataFormat.NONE;
    }
}
