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

package org.apache.inlong.manager.client.api.source;

import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import org.apache.inlong.manager.common.auth.DefaultAuthentication;
import org.apache.inlong.manager.common.enums.DataFormat;
import org.apache.inlong.manager.common.enums.SourceType;
import org.apache.inlong.manager.common.pojo.stream.StreamSource;

/**
 * Oracle  source.
 */
@Data
@EqualsAndHashCode(callSuper = true)
@AllArgsConstructor
@NoArgsConstructor
@ApiModel("Base configuration for Oracle collection")
public class OracleSource extends StreamSource {

    @ApiModelProperty(value = "DataSource type", required = true)
    private SourceType sourceType = SourceType.BINLOG;

    @ApiModelProperty("SyncType for Oracle")
    private SyncType syncType;

    @ApiModelProperty("Data format type for oracle")
    private DataFormat dataFormat = DataFormat.NONE;

    @ApiModelProperty("Auth for oracle")
    private DefaultAuthentication authentication;

    @ApiModelProperty("Hostname of the DB server, for example: 127.0.0.1")
    private String hostname;

    @ApiModelProperty("Exposed port of the DB server")
    private int port = 1521;

    @ApiModelProperty("Database name")
    private String database;

    @ApiModelProperty("table name")
    private String tableName;

    @ApiModelProperty("Schema name")
    private String schemaName;

    @ApiModelProperty("Scan startup mode")
    private String scanStartupMode;

    @ApiModelProperty("Need transfer total database")
    private boolean allMigration = false;

    @ApiModelProperty(value = "Primary key must be shared by all tables")
    private String primaryKey;
}
