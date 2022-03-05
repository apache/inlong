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

package org.apache.inlong.manager.workflow.util;

import org.apache.inlong.manager.common.enums.ProcessStatus;
import org.apache.inlong.manager.common.enums.TaskStatus;
import org.apache.inlong.manager.common.pojo.workflow.ProcessResponse;
import org.apache.inlong.manager.common.pojo.workflow.TaskResponse;
import org.apache.inlong.manager.common.pojo.workflow.WorkflowResult;
import org.apache.inlong.manager.common.util.JsonUtils;
import org.apache.inlong.manager.dao.entity.WorkflowProcessEntity;
import org.apache.inlong.manager.dao.entity.WorkflowTaskEntity;
import org.apache.inlong.manager.workflow.WorkflowContext;
import org.apache.inlong.manager.common.pojo.workflow.form.ProcessForm;
import org.apache.inlong.manager.workflow.definition.WorkflowProcess;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Workflow bean copy utils
 */
public class WorkflowBeanUtils {

    private static final Logger LOGGER = LoggerFactory.getLogger(WorkflowBeanUtils.class);

    /**
     * Build workflow context from WorkflowProcess and WorkflowProcessEntity
     */
    public static WorkflowContext buildContext(WorkflowProcess process, WorkflowProcessEntity processEntity) {
        ProcessForm processForm = null;
        try {
            processForm = WorkflowFormParserUtils.parseProcessForm(processEntity.getFormData(), process);
        } catch (Exception e) {
            LOGGER.error("build context from process form failed with id: {}", processEntity.getId(), e);
        }

        return new WorkflowContext().setProcess(process)
                .setApplicant(processEntity.getApplicant())
                .setProcessForm(processForm)
                .setProcessEntity(processEntity);
    }

    /**
     * Get task response from task entity
     */
    public static TaskResponse fromTaskEntity(WorkflowTaskEntity taskEntity) {
        if (taskEntity == null) {
            return null;
        }
        return TaskResponse.builder()
                .id(taskEntity.getId())
                .type(taskEntity.getType())
                .processId(taskEntity.getProcessId())
                .processName(taskEntity.getProcessName())
                .processDisplayName(taskEntity.getProcessDisplayName())
                .name(taskEntity.getName())
                .displayName(taskEntity.getDisplayName())
                .applicant(taskEntity.getApplicant())
                .approvers(Arrays.asList(taskEntity.getApprovers().split(WorkflowTaskEntity.APPROVERS_DELIMITER)))
                .operator(taskEntity.getOperator())
                .status(TaskStatus.valueOf(taskEntity.getStatus()))
                .remark(taskEntity.getRemark())
                .startTime(taskEntity.getStartTime())
                .endTime(taskEntity.getEndTime())
                .build();
    }

    /**
     * Get process response from process entity
     */
    public static ProcessResponse fromProcessEntity(WorkflowProcessEntity entity) {
        if (entity == null) {
            return null;
        }
        return ProcessResponse.builder()
                .id(entity.getId())
                .name(entity.getName())
                .displayName(entity.getDisplayName())
                .type(entity.getType())
                .title(entity.getTitle())
                .applicant(entity.getApplicant())
                .status(ProcessStatus.valueOf(entity.getStatus()))
                .startTime(entity.getStartTime())
                .endTime(entity.getEndTime())
                .formData(JsonUtils.parse(entity.getFormData()))
                .extParams(JsonUtils.parse(entity.getExtParams()))
                .build();
    }

    /**
     * Get the workflow result from the given workflow context
     */
    public static WorkflowResult result(WorkflowContext context) {
        if (context == null) {
            return null;
        }
        WorkflowResult workflowResult = new WorkflowResult();
        workflowResult.setProcessInfo(WorkflowBeanUtils.fromProcessEntity(context.getProcessEntity()));
        List<TaskResponse> taskList = context.getNewTaskList().stream().map(WorkflowBeanUtils::fromTaskEntity)
                .collect(Collectors.toList());
        workflowResult.setNewTasks(taskList);
        return workflowResult;
    }

}
