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

package org.apache.inlong.manager.common.pojo.source.sqlserver;

import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import lombok.Data;
import lombok.EqualsAndHashCode;
import org.apache.inlong.manager.common.enums.SourceType;
import org.apache.inlong.manager.common.pojo.source.SourceListResponse;

/**
 * Response info of sqlserver source list
 */
@Data
@EqualsAndHashCode(callSuper = true)
@ApiModel("Response of binlog source paging list")
public class SqlServerSourceListResponse extends SourceListResponse {

    @ApiModelProperty("Username of the Sqlserver")
    private String username;

    @ApiModelProperty("Password of the Sqlserver")
    private String password;

    @ApiModelProperty("Hostname of the Sqlserver")
    private String hostname;

    @ApiModelProperty("Exposed port of the Sqlserver")
    private int port;

    @ApiModelProperty("database of the Sqlserver")
    private String database;

    @ApiModelProperty("schemaName of the Sqlserver")
    private String schemaName;

    @ApiModelProperty("tableName of the Sqlserver")
    private String tableName;

    @ApiModelProperty("Database time zone, Default is UTC")
    private String serverTimezone;

    @ApiModelProperty("Whether to migrate all databases")
    private boolean allMigration;

    @ApiModelProperty(value = "Primary key must be shared by all tables")
    private String primaryKey;

    public SqlServerSourceListResponse() {
        this.setSourceType(SourceType.SQLSERVER.getType());
    }

}
