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

import com.fasterxml.jackson.annotation.JsonIgnore;
import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import java.util.Date;
import lombok.Data;

/**
 * File agent task config
 */
@ApiModel("File agent task config")
@Data
public class FileAgentTaskConfig {

    private String inlongGroupId;

    private String inlongStreamId;

    @ApiModelProperty(value = "Operation type")
    private String op;

    @ApiModelProperty(value = "Command delivery time")
    private Date deliveryTime;

    @ApiModelProperty(value = "Task id")
    private int taskId;

    @ApiModelProperty(value = "Agent IP")
    private String ip;

    @ApiModelProperty(value = "Collection file directory")
    private String dataName;

    @ApiModelProperty(value = "Time offset")
    private String timeOffset;

    @ApiModelProperty(value = "Message topic")
    private String topic;

    @ApiModelProperty(value = "Schedule time")
    private String scheduleTime;

    @ApiModelProperty(value = "Middleware type")
    private String middlewareType;

    @ApiModelProperty(value = "Field splitter")
    private String fieldSplitter;

    @ApiModelProperty(value = "Message queue master address")
    private String mqMasterAddress;

    @ApiModelProperty(value = "Additional attribute")
    private String additionalAttr;

    @JsonIgnore
    @ApiModelProperty(value = "Data generate rule")
    private String dataGenerateRule;

    @JsonIgnore
    private String sortType;

    @JsonIgnore
    private int status;

}
