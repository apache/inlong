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

package org.apache.inlong.manager.client.api;

import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
@ApiModel("Base configuration for MySQL collection")
public class MySQLSource extends StreamSource {

    @ApiModelProperty(value = "DataSource type", required = true)
    private SourceType sourceType = SourceType.DB;

    @ApiModelProperty("SyncType for MySQL")
    private SyncType syncType;

    @ApiModelProperty("Database name")
    private String dbName;

    @ApiModelProperty("Data table name, required for increment")
    private String tableName;

    @ApiModelProperty("Db server username")
    private String username;

    @ApiModelProperty("Db password")
    private String password;

    @ApiModelProperty("DB Server IP")
    private String dbServerIp;

    @ApiModelProperty("DB Server port")
    private int port;

    @ApiModelProperty("SQL statement to collect source data, required for full amount")
    private String dataSql;

}
