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

package org.apache.inlong.manager.common.pojo.workflow;

import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import java.util.List;
import java.util.stream.Collectors;
import lombok.Data;
import org.apache.inlong.manager.common.model.WorkflowContext;
import org.apache.inlong.manager.common.model.view.ProcessView;
import org.apache.inlong.manager.common.model.view.TaskView;

/**
 * Workflow results
 *
 */
@Data
@ApiModel("Workflow interface response interface")
public class WorkflowResult {

    @ApiModelProperty(value = "Application form information")
    private ProcessView processInfo;

    @ApiModelProperty(value = "Newly generated tasks")
    private List<TaskView> newTasks;

    public static WorkflowResult of(WorkflowContext context) {
        if (context == null) {
            return null;
        }
        WorkflowResult workflowResult = new WorkflowResult();
        workflowResult.setProcessInfo(ProcessView.fromProcessInstance(context.getProcessInstance()));
        workflowResult.setNewTasks(
                context.getNewTaskInstances().stream().map(TaskView::fromTaskInstance).collect(Collectors.toList())
        );
        return workflowResult;
    }
}
