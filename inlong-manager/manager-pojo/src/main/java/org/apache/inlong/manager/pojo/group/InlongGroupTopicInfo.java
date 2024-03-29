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

package org.apache.inlong.manager.pojo.group;

import org.apache.inlong.manager.pojo.cluster.ClusterInfo;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import lombok.Data;

import java.util.List;

/**
 * Inlong group and topic info
 */
@Data
@ApiModel("Inlong group and topic info")
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, visible = true, property = "mqType")
public abstract class InlongGroupTopicInfo {

    @ApiModelProperty(value = "Inlong group id", required = true)
    private String inlongGroupId;

    @ApiModelProperty(value = "MQ type, high throughput: TUBEMQ, high consistency: PULSAR, or KAFKA")
    private String mqType;

    @ApiModelProperty(value = "Inlong cluster tag of the current InlongGroup")
    private String inlongClusterTag;

    @ApiModelProperty(value = "MQ cluster info list")
    private List<? extends ClusterInfo> clusterInfos;

}
