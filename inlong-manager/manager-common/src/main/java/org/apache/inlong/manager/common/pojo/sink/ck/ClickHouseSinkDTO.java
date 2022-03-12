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

package org.apache.inlong.manager.common.pojo.sink.ck;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.swagger.annotations.ApiModelProperty;
import java.util.Map;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.inlong.manager.common.enums.ErrorCodeEnum;
import org.apache.inlong.manager.common.exceptions.BusinessException;

import javax.validation.constraints.NotNull;

/**
 * Sink info of ClickHouse
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class ClickHouseSinkDTO {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    @ApiModelProperty("ClickHouse JDBC URL")
    private String jdbcUrl;

    @ApiModelProperty("Target database name")
    private String databaseName;

    @ApiModelProperty("Target table name")
    private String tableName;

    @ApiModelProperty("Username for JDBC URL")
    private String username;

    @ApiModelProperty("User password")
    private String password;

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

    @ApiModelProperty("Properties for clickhouse")
    private Map<String, Object> properties;

    /**
     * Get the dto instance from the request
     */
    public static ClickHouseSinkDTO getFromRequest(ClickHouseSinkRequest request) {
        return ClickHouseSinkDTO.builder()
                .jdbcUrl(request.getJdbcUrl())
                .username(request.getUsername())
                .password(request.getPassword())
                .databaseName(request.getDatabaseName())
                .tableName(request.getTableName())
                .distributedTable(request.getDistributedTable())
                .partitionStrategy(request.getPartitionStrategy())
                .partitionKey(request.getPartitionKey())
                .keyFieldNames(request.getKeyFieldNames())
                .flushInterval(request.getFlushInterval())
                .flushRecordNumber(request.getFlushRecordNumber())
                .writeMaxRetryTimes(request.getWriteMaxRetryTimes())
                .properties(request.getProperties())
                .build();
    }

    public static ClickHouseSinkDTO getFromJson(@NotNull String extParams) {
        try {
            OBJECT_MAPPER.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
            return OBJECT_MAPPER.readValue(extParams, ClickHouseSinkDTO.class);
        } catch (Exception e) {
            throw new BusinessException(ErrorCodeEnum.SINK_INFO_INCORRECT.getMessage());
        }
    }

}
