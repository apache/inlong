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
import java.util.List;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.inlong.manager.client.api.DataFormat;
import org.apache.inlong.manager.client.api.StreamSource;
import org.apache.inlong.manager.common.enums.SourceType;

@Data
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

    @ApiModelProperty("Username of the DB server")
    private String user;

    @ApiModelProperty("Password of the DB server")
    private String password;

    @ApiModelProperty("Hostname of the DB server")
    private String hostname;

    @ApiModelProperty(value = "List of DBs to be collected, for example: db1.tb1,db2.tb2",
            notes = "DBs not in this list are excluded. By default, all DBs are monitored")
    private List<String> dbNames;

    @ApiModelProperty("Database time zone, Default is UTC")
    private String timeZone = "UTF";

    @ApiModelProperty("Timestamp standard for binlog: SQL, ISO_8601")
    private String timestampFormatStandard = "SQL";
}
