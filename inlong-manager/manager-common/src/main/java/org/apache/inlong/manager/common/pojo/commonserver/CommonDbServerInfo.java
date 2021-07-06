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
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@ApiModel("Common DB server info")
public class CommonDbServerInfo {

    private int id;

    @ApiModelProperty("access type, support: Agent, DataProxy Client, LoadProxy")
    private String accessType;

    @ApiModelProperty("connection alias name")
    private String connectionName;

    @ApiModelProperty("DB type, like: MySQL、Oracle")
    private String dbType;

    @ApiModelProperty("DB Server IP")
    private String dbServerIp;

    @ApiModelProperty("DB Server port")
    private int port;

    @ApiModelProperty("target database name")
    private String dbName;

    @ApiModelProperty("Db server username")
    private String username;

    @ApiModelProperty("password")
    private String password;

    @ApiModelProperty("has select permission for DB, 0: no, 1: yes")
    private boolean hasSelect;

    @ApiModelProperty("has insert permission for DB, 0: no, 1: yes")
    private boolean hasInsert;

    @ApiModelProperty("has update permission for DB, 0: no, 1: yes")
    private boolean hasUpdate;

    @ApiModelProperty("has delete permission for DB, 0: no, 1: yes")
    private boolean hasDelete;

    @ApiModelProperty("DB in charges, separate with commas(\",\") when multiple")
    private String inCharges;

    @ApiModelProperty("description for DB")
    private String dbDescription;

    @ApiModelProperty("backup DB host IP")
    private String backupDbServerIp;

    @ApiModelProperty("backup DB host port")
    private int backupDbPort;

    @ApiModelProperty("status, 0: invalid, 1: normal")
    private Integer status;

    @ApiModelProperty("is deleted? 0: deleted, 1: not deleted")
    private Integer isDeleted;

    private String creator;

    private String modifier;

    private Date createTime;

    private Date modifyTime;

    @ApiModelProperty("visible person, separate with commas(\",\") when multiple")
    private String visiblePerson;

    @ApiModelProperty("visible group, separate with commas(\",\") when multiple")
    private String visibleGroup;

}
