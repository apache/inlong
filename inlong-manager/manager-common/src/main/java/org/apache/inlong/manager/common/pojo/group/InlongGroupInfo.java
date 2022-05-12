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

package org.apache.inlong.manager.common.pojo.group;

import com.fasterxml.jackson.annotation.JsonFormat;
import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.inlong.manager.common.util.CommonBeanUtils;

import javax.validation.constraints.NotNull;
import java.util.Date;
import java.util.List;

/**
 * Inlong group info
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@ApiModel("Inlong group info")
public class InlongGroupInfo {

    @ApiModelProperty(value = "Primary key")
    private Integer id;

    @ApiModelProperty(value = "Inlong group id", required = true)
    private String inlongGroupId;

    @ApiModelProperty(value = "Inlong group name", required = true)
    private String name;

    @ApiModelProperty(value = "Inlong group description")
    private String description;

    @NotNull(message = "middlewareType cannot be null")
    @ApiModelProperty(value = "Middleware type, high throughput: TUBE, high consistency: PULSAR")
    private String mqType;

    @ApiModelProperty(value = "MQ resource object, in inlong group",
            notes = "Tube corresponds to Topic, Pulsar corresponds to Namespace/Topic")
    private String mqResource;

    @ApiModelProperty(value = "Number of access items per day, unit: 10,000 items per day")
    private Integer dailyRecords;

    @ApiModelProperty(value = "Access size per day, unit: GB per day")
    private Integer dailyStorage;

    @ApiModelProperty(value = "peak access per second, unit: bars per second")
    private Integer peakRecords;

    @ApiModelProperty(value = "The maximum length of a single piece of data, unit: Byte")
    private Integer maxLength;

    @ApiModelProperty(value = "Name of responsible person, separated by commas")
    private String inCharges;

    @ApiModelProperty(value = "Name of followers, separated by commas")
    private String followers;

    @ApiModelProperty(value = "Extend params")
    private String extParams;

    private Integer status;

    private Integer previousStatus;

    private String creator;

    private String modifier;

    @JsonFormat(pattern = "yyyy-MM-dd HH:mm:ss")
    private Date createTime;

    @JsonFormat(pattern = "yyyy-MM-dd HH:mm:ss")
    private Date modifyTime;

    @ApiModelProperty(value = "Need create resource support, 0 false 1 true")
    private Integer enableCreateResource;

    @ApiModelProperty(value = "Need zookeeper support, 0 false 1 true")
    private Integer enableZookeeper;
	
    @ApiModelProperty(value = "The cluster tag, which links to inlong_cluster table")
    private String inlongClusterTag;

    @ApiModelProperty(value = "Queue model of Pulsar, parallel: multiple partitions, high throughput, out-of-order "
            + "messages; serial: single partition, low throughput, and orderly messages")
    private String queueModule;

    @ApiModelProperty(value = "The number of partitions of Pulsar Topic, 1-20")
    private Integer topicPartitionNum;

    @ApiModelProperty(value = "Tube master URL")
    private String tubeMaster;

    @ApiModelProperty(value = "Pulsar admin URL")
    private String pulsarAdminUrl;

    @ApiModelProperty(value = "Pulsar service URL")
    private String pulsarServiceUrl;

    @ApiModelProperty(value = "Data type name")
    private String schemaName;

    @ApiModelProperty(value = "Temporary view, string in JSON format")
    private String tempView;

    @ApiModelProperty(value = "Inlong group Extension properties")
    private List<InlongGroupExtInfo> extList;

    @ApiModelProperty(value = "The extension info for MQ")
    private InlongGroupMqExtBase mqExtInfo;

    public InlongGroupRequest genRequest() {
        return CommonBeanUtils.copyProperties(this, InlongGroupRequest::new);
    }

    public InlongGroupResponse genResponse() {
        return CommonBeanUtils.copyProperties(this, InlongGroupResponse::new);
    }
}
