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
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
@ApiModel("Hive storage configuration")
public class HiveStorage extends DataStorage {

    @ApiModelProperty(value = "Data storage type", required = true)
    private StorageType type = StorageType.HIVE;

    @ApiModelProperty("Hive JDBC URL")
    private String jdbcUrl;

    @ApiModelProperty("Username for JDBC URL")
    private String username;

    @ApiModelProperty("User password")
    private String password;

    @ApiModelProperty("Target database name")
    private String dbName;

    @ApiModelProperty("Target table name")
    private String tableName;

    @ApiModelProperty("HDFS defaultFS")
    private String hdfsDefaultFs;

    @ApiModelProperty("Warehouse directory")
    private String warehouseDir;

    @ApiModelProperty("Data encoding format: UTF-8, GBK")
    private Charset charset = StandardCharsets.UTF_8;

    @ApiModelProperty("Data separator, stored as ASCII code")
    private DataSeparator dataSeparator = DataSeparator.VERTICAL_BAR;

    public enum FileFormat {
        TextFile, RCFile, SequenceFile, Avro;
    }

    @ApiModelProperty("File format, support: TextFile, RCFile, SequenceFile, Avro")
    private FileFormat fileFormat;

    @ApiModelProperty("Other properties if need")
    private Map<String, String> properties;
}

