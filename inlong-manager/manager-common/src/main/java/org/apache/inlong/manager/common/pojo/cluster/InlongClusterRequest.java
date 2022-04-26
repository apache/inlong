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

import javax.validation.constraints.NotBlank;

/**
 * Inlong cluster request
 */
@Data
@ApiModel("Inlong cluster request")
public class InlongClusterRequest {

    @ApiModelProperty(value = "Primary key")
    private Integer id;

    @NotBlank
    @ApiModelProperty(value = "Cluster name")
    private String name;

    @NotBlank
    @ApiModelProperty(value = "Cluster type, including TUBE, PULSAR, DATA_PROXY, etc.")
    private String type;

    @ApiModelProperty(value = "Cluster url")
    private String url;

    @NotBlank
    @ApiModelProperty(value = "Cluster tag")
    private String clusterTag;

    @ApiModelProperty(value = "Cluster zone tag")
    private String zoneTag;

    @ApiModelProperty(value = "Cluster heartbeat info")
    private String heartbeat;

    @ApiModelProperty(value = "Extended params")
    private String extParams;

    @ApiModelProperty(value = "Name of responsible person, separated by commas")
    private String inCharges;

}
