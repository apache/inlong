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

package org.apache.inlong.manager.common.pojo.cluster;

import com.fasterxml.jackson.annotation.JsonFormat;
import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import lombok.Data;

import java.util.Date;

/**
 * DataProxy cluster information
 */
@Data
@ApiModel("DataProxy cluster information")
public class DataProxyClusterInfo {

    @ApiModelProperty(value = "Primary key")
    private Integer id;

    @ApiModelProperty(value = "Cluster name")
    private String name;

    @ApiModelProperty(value = "Cluster description")
    private String description;

    @ApiModelProperty(value = "Cluster address")
    private String address;

    @ApiModelProperty(value = "Access port number, multiple ports are separated by a comma")
    private String port;

    @ApiModelProperty(value = "Whether it is a backup cluster, 0: no, 1: yes")
    private Integer isBackup;

    @ApiModelProperty(value = "Name of its mqSetName")
    private String mqSetName;

    @ApiModelProperty(value = "Extended params, string in JSON format")
    private String extParams;

    @ApiModelProperty(value = "Name of responsible person, separated by commas")
    private String inCharges;

    @ApiModelProperty(value = "Cluster status")
    private Integer status;

    @JsonFormat(pattern = "yyyy-MM-dd HH:mm:ss")
    private Date createTime;

    @JsonFormat(pattern = "yyyy-MM-dd HH:mm:ss")
    private Date modifyTime;

}
