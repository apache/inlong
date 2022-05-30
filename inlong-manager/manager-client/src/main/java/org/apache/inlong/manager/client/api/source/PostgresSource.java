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
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import org.apache.inlong.manager.common.enums.DataFormat;
import org.apache.inlong.manager.common.enums.SourceType;
import org.apache.inlong.manager.common.pojo.source.StreamSource;

/**
 * Postgres source.
 */
@Data
@EqualsAndHashCode(callSuper = true)
@AllArgsConstructor
@NoArgsConstructor
@ApiModel("Base configuration for Postgres collection")
public class PostgresSource extends StreamSource {

    @ApiModelProperty(value = "DataSource type", required = true)
    private SourceType sourceType = SourceType.POSTGRES;

    @ApiModelProperty("SyncType")
    private SyncType syncType = SyncType.INCREMENT;

    @ApiModelProperty("Data format type")
    private DataFormat dataFormat = DataFormat.CSV;

    @ApiModelProperty("Db server username")
    private String username;

    @ApiModelProperty("Db password")
    private String password;

    @ApiModelProperty("DB Server hostname")
    private String hostname;

    @ApiModelProperty("DB Server port")
    private int port;

    @ApiModelProperty("Database name")
    private String dbName;

    @ApiModelProperty("schema info")
    private String schema;

    @ApiModelProperty("Data table name list")
    private List<String> tableNameList;

    @ApiModelProperty("decoding pulgin name")
    private String decodingPluginName;

    @ApiModelProperty("Primary key")
    private String primaryKey;
}
