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

package org.apache.inlong.manager.pojo.cluster;

import com.fasterxml.jackson.annotation.JsonFormat;
import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Date;

/**
 * Inlong cluster node response
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@ApiModel("Cluster node response")
public class ClusterNodeResponse {

    @ApiModelProperty(value = "Primary key")
    private Integer id;

    @ApiModelProperty(value = "ID of the parent cluster")
    private Integer parentId;

    @ApiModelProperty(value = "Cluster type, including AGENT, DATAPROXY, etc.")
    private String type;

    @ApiModelProperty(value = "Cluster IP")
    private String ip;

    @ApiModelProperty(value = "Cluster port")
    private Integer port;

    @ApiModelProperty(value = "Username")
    private String username;

    @ApiModelProperty(value = "password")
    private String password;

    @ApiModelProperty(value = "SSH port")
    private Integer sshPort;

    @ApiModelProperty(value = "Cluster protocol type")
    private String protocolType;

    @ApiModelProperty(value = "Load value of the node")
    private Integer nodeLoad;

    @ApiModelProperty(value = "Cluster node tag, separated by commas")
    private String nodeTags;

    @ApiModelProperty(value = "Extended params")
    private String extParams;

    @ApiModelProperty(value = "Operate log")
    private String operateLog;

    @ApiModelProperty(value = "Description of the cluster node")
    private String description;

    @ApiModelProperty(value = "Cluster status")
    private Integer status;

    @ApiModelProperty(value = "Name of in creator")
    private String creator;

    @ApiModelProperty(value = "Name of in modifier")
    private String modifier;

    @JsonFormat(pattern = "yyyy-MM-dd'T'HH:mm:ss.SSSZ", timezone = "GMT+8")
    private Date createTime;

    @JsonFormat(pattern = "yyyy-MM-dd'T'HH:mm:ss.SSSZ", timezone = "GMT+8")
    private Date modifyTime;

    @ApiModelProperty(value = "Version number")
    private Integer version;

}
