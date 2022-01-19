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

package org.apache.inlong.manager.common.model.view;

import org.apache.inlong.manager.common.model.TaskState;

import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;

import java.util.Date;
import java.util.Set;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import org.springframework.format.annotation.DateTimeFormat;

/**
 * Task query
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@ApiModel("Workflow-task query conditions")
public class TaskQuery extends PageQuery {

    @ApiModelProperty("task ID")
    private Integer id;

    @ApiModelProperty("process ID")
    private Integer processInstId;

    @ApiModelProperty("task type")
    private String type;

    @ApiModelProperty("task name")
    private String name;

    @ApiModelProperty("task display name")
    private String displayName;

    @ApiModelProperty("applicant")
    private String applicant;

    @ApiModelProperty("approver")
    private String approver;

    @ApiModelProperty("actual operator")
    private String operator;

    @ApiModelProperty("status")
    private Set<TaskState> states;

    @ApiModelProperty("start time-lower limit yyyy-MM-dd HH:mm:ss")
    @DateTimeFormat(pattern = "yyyy-MM-dd HH:mm:ss")
    private Date startTimeBegin;

    @ApiModelProperty("start time-upper limit yyyy-MM-dd HH:mm:ss")
    @DateTimeFormat(pattern = "yyyy-MM-dd HH:mm:ss")
    private Date startTimeEnd;

    @ApiModelProperty("end time-upper limit yyyy-MM-dd HH:mm:ss")
    @DateTimeFormat(pattern = "yyyy-MM-dd HH:mm:ss")
    private Date endTimeBegin;

    @ApiModelProperty("end time-lower time yyyy-MM-dd HH:mm:ss")
    @DateTimeFormat(pattern = "yyyy-MM-dd HH:mm:ss")
    private Date endTimeEnd;
}
