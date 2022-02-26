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

import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import lombok.Data;

/**
 * Common cluster information
 */
@Data
@ApiModel("Common cluster information")
public class ClusterInfo {

    @ApiModelProperty(value = "Incremental primary key")
    private Integer id;

    @ApiModelProperty(value = "cluster name")
    private String name;

    @ApiModelProperty(value = "Cluster type, including TUBE, ZK, etc.")
    private String type;

    @ApiModelProperty(value = "Cluster IP address")
    private String ip;

    @ApiModelProperty(value = "Cluster port number")
    private Integer port;

    @ApiModelProperty(value = "Name of in charges, separated by commas")
    private String inCharges;

    @ApiModelProperty(value = "Cluster URL address")
    private String url;

    @ApiModelProperty(value = "Whether it is a backup cluster, 0: no, 1: yes")
    private Integer isBackup;

    @ApiModelProperty(value = "Cluster status")
    private Integer status;

    @ApiModelProperty(value = "MQ set name")
    private String mqSetName;

    @ApiModelProperty(value = "MQ config info")
    private String extParams;

}
