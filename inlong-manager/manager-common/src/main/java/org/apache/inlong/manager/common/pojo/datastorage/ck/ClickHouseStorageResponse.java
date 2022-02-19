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

package org.apache.inlong.manager.common.pojo.datastorage.ck;

import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import org.apache.inlong.manager.common.enums.BizConstant;
import org.apache.inlong.manager.common.pojo.datastorage.StorageResponse;

/**
 * Response of the ClickHouse storage
 */
@Data
@ToString(callSuper = true)
@EqualsAndHashCode(callSuper = true)
@ApiModel(value = "Response of the ClickHouse storage")
public class ClickHouseStorageResponse extends StorageResponse {

    private String storageType = BizConstant.STORAGE_CLICKHOUSE;

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

}
