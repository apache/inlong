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

package org.apache.inlong.manager.common.pojo.agent;

import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import lombok.Data;

@ApiModel("Data config")
@Data
public class DataConfig {

    @ApiModelProperty(value = "Operation type")
    private int opType;

    @ApiModelProperty(value = "WorkflowTask id")
    private int id;

    @ApiModelProperty(value = "Agent IP")
    private String ip;

    @ApiModelProperty(value = "Collection file directory")
    private String dataName;

    @ApiModelProperty(value = "Time offset")
    private String timeOffset;

    @ApiModelProperty(value = "Message topic")
    private String topic;

    @ApiModelProperty(value = "Data interface id")
    private String interfaceId;

    @ApiModelProperty(value = "Message queue type")
    private String mqType;

    @ApiModelProperty(value = "Cluster id")
    private int clusterId;

    @ApiModelProperty(value = "Inlong group id")
    private String inlongGroupId;

    @ApiModelProperty(value = "Schedule time")
    private String scheduleTime;

    @ApiModelProperty(value = "Additional info that needs to be added to the message header when writing to DataProxy")
    private String additionalAttr;

    @ApiModelProperty(value = "Field splitter")
    private String fieldSplitter;

    @ApiModelProperty(value = "WorkflowTask type")
    private int taskType;
}
