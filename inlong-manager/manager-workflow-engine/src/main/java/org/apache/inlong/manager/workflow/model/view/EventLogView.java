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

package org.apache.inlong.manager.workflow.model.view;

import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import java.util.Date;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Event log
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@ApiModel("Workflow event log query conditions")
public class EventLogView {

    @ApiModelProperty("id")
    private Integer id;

    @ApiModelProperty("process ID")
    private Integer processInstId;

    @ApiModelProperty("process name ")
    private String processName;

    @ApiModelProperty("Process display name")
    private String processDisplayName;

    @ApiModelProperty("business ID")
    private String businessId;

    @ApiModelProperty("task ID")
    private Integer taskInstId;

    @ApiModelProperty("the name of the element that triggered the event")
    private String elementName;

    @ApiModelProperty("The name of the element that triggered the event")
    private String elementDisplayName;

    @ApiModelProperty("event type")
    private String eventType;

    @ApiModelProperty("event")
    private String event;

    @ApiModelProperty("Listener name")
    private String listener;

    @ApiModelProperty("status")
    private Integer state;

    @ApiModelProperty("Is it synchronized")
    private Boolean async;

    @ApiModelProperty("Execute IP")
    private String ip;

    @ApiModelProperty("start time ")
    private Date startTime;

    @ApiModelProperty("end time")
    private Date endTime;

    @ApiModelProperty("exception")
    private String exception;

}
