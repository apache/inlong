/*
 *  Licensed to the Apache Software Foundation (ASF) under one or more
 *  contributor license agreements. See the NOTICE file distributed with
 *  this work for additional information regarding copyright ownership.
 *  The ASF licenses this file to You under the Apache License, Version 2.0
 *  (the "License"); you may not use this file except in compliance with
 *  the License. You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */

package org.apache.inlong.manager.pojo.cluster;


import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import lombok.Data;

import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotNull;
import java.util.List;

/**
 * Inlong cluster node bind or unbind tag request
 */
@Data
@ApiModel("Cluster node bind and unbind tag request")
public class ClusterNodeBindTagRequest {

    @NotBlank(message = "Cluster nodeTag cannot be blank")
    @ApiModelProperty(value = "Cluster node tag")
    private String clusterNodeTag;

    @NotBlank(message = "clusterName cannot be blank")
    @ApiModelProperty(value = "Cluster name")
    private String clusterName;

    @NotBlank(message = "type cannot be blank")
    @ApiModelProperty(value = "Cluster type, including AGENT, DATAPROXY, etc.")
    private String type;

    @ApiModelProperty(value = "Cluster node ip list which needs to bind tag")
    private List<String> bindClusterNodes;

    @ApiModelProperty(value = "Cluster node ip list which needs to unbind tag")
    private List<String> unbindClusterNodes;

}
