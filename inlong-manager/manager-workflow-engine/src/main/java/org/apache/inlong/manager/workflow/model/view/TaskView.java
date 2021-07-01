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
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.inlong.manager.common.util.JsonUtils;
import org.apache.inlong.manager.workflow.model.TaskState;
import org.apache.inlong.manager.workflow.model.instance.TaskInstance;

/**
 * task information
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@ApiModel("Approval task information")
public class TaskView {

    /**
     * Task ID
     */
    @ApiModelProperty(value = "approval task ID")
    private Integer id;

    /**
     * Task type
     */
    @ApiModelProperty(value = "Task type")
    private String type;

    /**
     * Application form ID
     */
    @ApiModelProperty(value = "application form ID")
    private Integer processInstId;

    @ApiModelProperty(value = "process name")
    private String processName;

    @ApiModelProperty(value = "process display name")
    private String processDisplayName;

    /**
     * Task name
     */
    @ApiModelProperty(value = "task name-english key")
    private String name;

    /**
     * Chinese name of the task
     */
    @ApiModelProperty(value = "task display name")
    private String displayName;

    @ApiModelProperty(value = "applicant")
    private String applicant;
    /**
     * Approver
     */
    @ApiModelProperty(value = "set approver")
    private List<String> approvers;

    /**
     * Task operator
     */
    @ApiModelProperty(value = "actual operation approver")
    private String operator;

    /**
     * Task status
     */
    @ApiModelProperty(value = "task status")
    private TaskState state;

    /**
     * Remarks information
     */
    @ApiModelProperty(value = "remarks information")
    private String remark;

    /**
     * Form information
     */
    @ApiModelProperty(value = "current task form information")
    private Object formData;

    /**
     * Start time
     */
    @ApiModelProperty(value = "start time")
    private Date startTime;

    /**
     * End time
     */
    @ApiModelProperty(value = "end time")
    private Date endTime;

    /**
     * Extended Information
     */
    @ApiModelProperty(value = "extended Information")
    private Object ext;

    public static TaskView fromTaskInstance(TaskInstance taskInstance) {
        return TaskView.builder()
                .id(taskInstance.getId())
                .type(taskInstance.getType())
                .processInstId(taskInstance.getProcessInstId())
                .processName(taskInstance.getProcessName())
                .processDisplayName(taskInstance.getProcessDisplayName())
                .name(taskInstance.getName())
                .displayName(taskInstance.getDisplayName())
                .applicant(taskInstance.getApplicant())
                .approvers(Arrays.asList(taskInstance.getApprovers().split(TaskInstance.APPROVERS_DELIMITER)))
                .operator(taskInstance.getOperator())
                .state(TaskState.valueOf(taskInstance.getState()))
                .remark(taskInstance.getRemark())
                .formData(JsonUtils.parse(taskInstance.getFormData()))
                .startTime(taskInstance.getStartTime())
                .endTime(taskInstance.getEndTime())
                .ext(JsonUtils.parse(taskInstance.getExt()))
                .build();
    }
}
