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

package org.apache.inlong.manager.common.pojo.source;

import com.fasterxml.jackson.annotation.JsonFormat;
import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import java.util.Date;
import lombok.Data;

/**
 * DB source details
 */
@Data
@ApiModel("DB source details")
public class SourceDbDetailInfo {

    @ApiModelProperty(value = "Primary key")
    private Integer id;

    @ApiModelProperty(value = "Inlong group id")
    private String inlongGroupId;

    @ApiModelProperty(value = "Inlong stream id")
    private String inlongStreamId;

    @ApiModelProperty(value = "Collection type, with Agent, DataProxy client, LoadProxy")
    private String accessType;

    @ApiModelProperty(value = "Database name")
    private String dbName;

    @ApiModelProperty(value = "Transfer IP")
    private String transferIp;

    @ApiModelProperty(value = "The name of the database connection")
    private String connectionName;

    @ApiModelProperty(value = "Timed scheduling expression, required for full amount")
    private String crontab;

    @ApiModelProperty(value = "SQL statement to collect source data, required for full amount")
    private String dataSql;

    @ApiModelProperty(value = "Data table name, required for increment")
    private String tableName;

    @ApiModelProperty(value = "Data table fields, separated by commas, need to be incremented")
    private String tableFields;

    @ApiModelProperty(value = "Source status")
    private Integer status;

    @ApiModelProperty(value = "Previous status")
    private Integer previousStatus;

    @ApiModelProperty(value = "is deleted? 0: deleted, 1: not deleted")
    private Integer isDeleted = 0;

    private String creator;

    private String modifier;

    @JsonFormat(pattern = "yyyy-MM-dd HH:mm:ss")
    private Date createTime;

    @JsonFormat(pattern = "yyyy-MM-dd HH:mm:ss")
    private Date modifyTime;

    @ApiModelProperty(value = "Temporary view, string in JSON format")
    private String tempView;

}