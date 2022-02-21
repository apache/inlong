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

package org.apache.inlong.manager.common.pojo.commonserver;

import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import java.util.Date;
import lombok.Data;

/**
 * File source list
 */
@Data
@ApiModel("File source list")
public class CommonDbServerListVo {

    private int id;

    @ApiModelProperty("Collection type, Agent, DataProxy Client, LoadProxy")
    private String accessType;

    @ApiModelProperty("The name of the database connection")
    private String connectionName;

    @ApiModelProperty("DB type, such as MySQL, Oracle")
    private String dbType;

    @ApiModelProperty("DB Server IP")
    private String dbServerIp;

    @ApiModelProperty("Port Number")
    private int port;

    @ApiModelProperty("Target database name")
    private String dbName;

    @ApiModelProperty("DB description")
    private String dbDescription;

    @ApiModelProperty("Status, 0: normal, 1: invalid")
    private Integer status;

    private String modifier;

    private Date modifyTime;
}
