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
import org.apache.inlong.manager.client.api.DataFormat;
import org.apache.inlong.manager.client.api.StreamSource;
import org.apache.inlong.manager.client.api.auth.DefaultAuthentication;
import org.apache.inlong.manager.common.enums.SourceType;

import java.util.List;

@Data
@EqualsAndHashCode(callSuper = true)
@AllArgsConstructor
@NoArgsConstructor
@ApiModel("Base configuration for MySQL binlog collection")
public class MySQLBinlogSource extends StreamSource {

    @ApiModelProperty(value = "DataSource type", required = true)
    private SourceType sourceType = SourceType.BINLOG;

    @ApiModelProperty("SyncType for MySQL")
    private SyncType syncType;

    @ApiModelProperty("Data format type for binlog")
    private DataFormat dataFormat = DataFormat.NONE;

    @ApiModelProperty("Auth for binlog")
    private DefaultAuthentication authentication;

    @ApiModelProperty("Whether include schema, default is 'false'")
    private String includeSchema;

    @ApiModelProperty("Hostname of the DB server, for example: 127.0.0.1")
    private String hostname;

    @ApiModelProperty("Exposed port of the DB server")
    private int port = 3306;

    @ApiModelProperty(value = "List of DBs to be collected, supporting regular expressions, "
            + "separate them with commas, for example: db1,test_db*",
            notes = "DBs not in this list are excluded. If not set, all DBs are monitored")
    private List<String> dbNames;

    @ApiModelProperty(value = "List of tables to be collected, supporting regular expressions, "
            + "separate them with commas, for example: tb1,user*",
            notes = "Tables not in this list are excluded. By default, all tables are monitored")
    private List<String> tableNames;

    @ApiModelProperty("Database time zone, Default is UTC")
    private String serverTimezone = "UTF";

    @ApiModelProperty("The interval for recording an offset")
    private String intervalMs;

    @ApiModelProperty("Snapshot mode, supports: initial, when_needed, never, schema_only, schema_only_recovery")
    private String snapshotMode;

    @ApiModelProperty("The file path to store offset info")
    private String offsetFilename;

    @ApiModelProperty("The file path to store history info")
    private String historyFilename;

    @ApiModelProperty("Whether to monitor the DDL, default is 'false'")
    private String monitoredDdl;

    @ApiModelProperty("Timestamp standard for binlog: SQL, ISO_8601")
    private String timestampFormatStandard = "SQL";

    @ApiModelProperty("Need transfer total database")
    private boolean allMigration = false;
}
