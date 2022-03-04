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

package org.apache.inlong.manager.common.pojo.source.binlog;

import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import org.apache.inlong.manager.common.enums.Constant;
import org.apache.inlong.manager.common.pojo.source.SourceResponse;

/**
 * Response of the binlog source
 */
@Data
@ToString(callSuper = true)
@EqualsAndHashCode(callSuper = true)
@ApiModel(value = "Response of the binlog source")
public class BinlogSourceResponse extends SourceResponse {

    private String sourceType = Constant.SOURCE_BINLOG;

    @ApiModelProperty("Username of the DB server")
    private String user;

    @ApiModelProperty("Password of the DB server")
    private String password;

    @ApiModelProperty("Hostname of the DB server")
    private String hostname;

    @ApiModelProperty("Exposed port of the DB server")
    private int port;

    @ApiModelProperty(value = "List of DBs to be collected, supporting regular expressions")
    private String whitelist;

    @ApiModelProperty("Database time zone, Default is UTC")
    private String timeZone;

    @ApiModelProperty("The file path to store history info")
    private String storeHistoryFilename;

    @ApiModelProperty("Offset of the task")
    private String offset;

    @ApiModelProperty("The interval for recording an offset")
    private String intervalMs;

    @ApiModelProperty("Snapshot mode, supports: initial, when_needed, never, schema_only, schema_only_recovery")
    private String snapshotMode;

    @ApiModelProperty("Timestamp standard for binlog: SQL, ISO_8601")
    private String timestampFormatStandard = "SQL";

    @ApiModelProperty("Need transfer total database")
    private boolean migrationTransfer;
}
