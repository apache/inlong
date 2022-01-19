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

import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;

import java.util.List;

/**
 * Application form details
 */
@ApiModel("Application form details")
public class ProcessDetail {

    @ApiModelProperty(value = "Application form details")
    private ProcessView processInfo;

    @ApiModelProperty(value = "Tasks currently to be done")
    private TaskView currentTask;

    @ApiModelProperty(value = "Approval history")
    private List<TaskView> taskHistory;

    @ApiModelProperty(value = "Workflow definition")
    private WorkflowView workflow;

    public ProcessView getProcessInfo() {
        return processInfo;
    }

    public ProcessDetail setProcessInfo(ProcessView processInfo) {
        this.processInfo = processInfo;
        return this;
    }

    public TaskView getCurrentTask() {
        return currentTask;
    }

    public ProcessDetail setCurrentTask(TaskView currentTask) {
        this.currentTask = currentTask;
        return this;
    }

    public List<TaskView> getTaskHistory() {
        return taskHistory;
    }

    public ProcessDetail setTaskHistory(
            List<TaskView> taskHistory) {
        this.taskHistory = taskHistory;
        return this;
    }

    public WorkflowView getWorkflow() {
        return workflow;
    }

    public ProcessDetail setWorkflow(WorkflowView workflow) {
        this.workflow = workflow;
        return this;
    }
}
