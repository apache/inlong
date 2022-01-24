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

package org.apache.inlong.manager.common.pojo.datastorage;

import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import lombok.Data;
import lombok.EqualsAndHashCode;

/**
 * Response of Hive storage list
 */
@Data
@EqualsAndHashCode(callSuper = true)
@ApiModel("Response of Hive storage paging list")
public class StorageHiveListResponse extends BaseStorageListResponse {

    @ApiModelProperty("target database name")
    private String dbName;

    @ApiModelProperty("target table name")
    private String tableName;

    @ApiModelProperty("username")
    private String username;

    @ApiModelProperty("JDBC URL")
    private String jdbcUrl;

    @ApiModelProperty("HDFS defaultFS")
    private String hdfsDefaultFs;

    @ApiModelProperty("partition type, like: H-hour, D-day, W-week, M-month, O-once, R-regulation")
    private String partitionType;

    @ApiModelProperty("storage period, unit: day")
    private Integer storagePeriod;

}
