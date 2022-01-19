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

package org.apache.inlong.manager.workflow.core.impl;

import com.google.common.collect.Maps;

import org.apache.inlong.manager.common.workflow.QueryService;
import org.apache.inlong.manager.common.workflow.WorkflowDataAccessor;
import org.apache.inlong.manager.common.exceptions.WorkflowException;
import org.apache.inlong.manager.common.model.ProcessState;
import org.apache.inlong.manager.common.model.TaskState;
import org.apache.inlong.manager.common.model.WorkflowContext;
import org.apache.inlong.manager.common.model.definition.Element;
import org.apache.inlong.manager.common.model.definition.NextableElement;
import org.apache.inlong.manager.common.model.definition.Process;
import org.apache.inlong.manager.common.model.definition.ProcessForm;
import org.apache.inlong.manager.common.model.definition.Task;
import org.apache.inlong.manager.common.model.definition.TaskForm;
import org.apache.inlong.manager.common.model.definition.UserTask;
import org.apache.inlong.manager.common.model.instance.EventLog;
import org.apache.inlong.manager.common.model.instance.ProcessInstance;
import org.apache.inlong.manager.common.model.instance.TaskInstance;
import org.apache.inlong.manager.common.model.view.CountByKey;
import org.apache.inlong.manager.common.model.view.ElementView;
import org.apache.inlong.manager.common.model.view.EventLogQuery;
import org.apache.inlong.manager.common.model.view.ProcessDetail;
import org.apache.inlong.manager.common.model.view.ProcessQuery;
import org.apache.inlong.manager.common.model.view.ProcessSummaryQuery;
import org.apache.inlong.manager.common.model.view.ProcessSummaryView;
import org.apache.inlong.manager.common.model.view.ProcessView;
import org.apache.inlong.manager.common.model.view.TaskQuery;
import org.apache.inlong.manager.common.model.view.TaskSummaryQuery;
import org.apache.inlong.manager.common.model.view.TaskSummaryView;
import org.apache.inlong.manager.common.model.view.TaskView;
import org.apache.inlong.manager.common.model.view.WorkflowView;
import org.apache.inlong.manager.workflow.util.WorkflowFormParserUtils;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import lombok.extern.slf4j.Slf4j;

/**
 * Query service
 */
@Slf4j
public class QueryServiceImpl implements QueryService {

    private WorkflowDataAccessor workflowDataAccessor;

    public QueryServiceImpl(WorkflowDataAccessor workflowDataAccessor) {
        this.workflowDataAccessor = workflowDataAccessor;
    }

    @Override
    public ProcessInstance getProcessInstance(Integer processInstId) {
        return workflowDataAccessor.processInstanceStorage().get(processInstId);
    }

    @Override
    public List<TaskInstance> listApproveHistory(Integer processInstId) {
        return workflowDataAccessor.taskInstanceStorage().listByQuery(
                TaskQuery.builder()
                        .processInstId(processInstId)
                        .states(TaskState.COMPLETED_STATES)
                        .build()
        );
    }

    @Override
    public TaskInstance getTaskInstance(Integer taskInstId) {
        return workflowDataAccessor.taskInstanceStorage().get(taskInstId);
    }

    @Override
    public List<ProcessInstance> listProcess(ProcessQuery processQuery) {
        return this.workflowDataAccessor.processInstanceStorage().listByQuery(processQuery);
    }

    @Override
    public List<TaskInstance> listTask(TaskQuery taskQuery) {
        return this.workflowDataAccessor.taskInstanceStorage().listByQuery(taskQuery);
    }

    @Override
    public ProcessSummaryView processSummary(ProcessSummaryQuery query) {
        List<CountByKey> result = this.workflowDataAccessor.processInstanceStorage()
                .countByState(query);

        Map<String, Integer> countByState = result.stream()
                .collect(Collectors.toMap(CountByKey::getKey, CountByKey::getValue));

        return ProcessSummaryView.builder()
                .totalApplyCount(countByState.values().stream().mapToInt(c -> c).sum())
                .totalApproveCount(countByState.getOrDefault(ProcessState.COMPLETED.name(), 0))
                .totalRejectCount(countByState.getOrDefault(ProcessState.REJECTED.name(), 0))
                .totalProcessingCount(countByState.getOrDefault(ProcessState.PROCESSING.name(), 0))
                .totalCancelCount(countByState.getOrDefault(ProcessState.CANCELED.name(), 0))
                .build();
    }

    @Override
    public TaskSummaryView taskSummary(TaskSummaryQuery query) {
        List<CountByKey> result = this.workflowDataAccessor.taskInstanceStorage()
                .countByState(query);

        Map<String, Integer> countByState = result.stream()
                .collect(Collectors.toMap(CountByKey::getKey, CountByKey::getValue));

        return TaskSummaryView.builder()
                .totalApproveCount(countByState.getOrDefault(TaskState.APPROVED.name(), 0))
                .totalPendingCount(countByState.getOrDefault(TaskState.PENDING.name(), 0))
                .totalRejectCount(countByState.getOrDefault(TaskState.REJECTED.name(), 0))
                .totalTransferCount(countByState.getOrDefault(TaskState.TRANSFERED.name(), 0))
                .build();
    }

    @Override
    public ProcessDetail detail(Integer processInstId, Integer taskInstId) {
        ProcessInstance processInstance = this.getProcessInstance(processInstId);
        if (processInstance == null) {
            return null;
        }
        Process process = workflowDataAccessor.processDefinitionStorage().get(processInstance.getName());

        List<TaskView> history = this.listApproveHistory(processInstId)
                .stream().map(TaskView::fromTaskInstance).collect(Collectors.toList());

        TaskView currentTask = Optional.ofNullable(taskInstId)
                .map(this::getTaskInstance)
                .map(TaskView::fromTaskInstance)
                .orElse(null);

        if (currentTask != null && process != null && TaskState.PENDING.equals(currentTask.getState())) {
            Task task = process.getTaskByName(currentTask.getName());
            currentTask.setFormData(getEmptyTaskForm(task));
        }

        if (currentTask != null && !processInstId.equals(currentTask.getProcessInstId())) {
            throw new WorkflowException("task: " + taskInstId + " not belong to process:" + processInstId);
        }

        WorkflowView workflowView = getWorkflowView(processInstance);

        ProcessDetail processDetail = new ProcessDetail()
                .setProcessInfo(ProcessView.fromProcessInstance(processInstance))
                .setCurrentTask(currentTask)
                .setTaskHistory(history)
                .setWorkflow(workflowView);

        if (process == null || process.getProcessDetailHandler() == null) {
            return processDetail;
        }

        return process.getProcessDetailHandler().handle(processDetail);
    }

    @Override
    public EventLog getEventLog(Integer id) {
        return workflowDataAccessor.eventLogStorage().get(id);
    }

    @Override
    public List<EventLog> listEventLog(EventLogQuery query) {
        return workflowDataAccessor.eventLogStorage().list(query);
    }

    private WorkflowContext buildContext(Process process, ProcessInstance processInstance) {
        ProcessForm processForm = null;
        try {
            processForm = WorkflowFormParserUtils.parseProcessForm(processInstance.getFormData(), process);
        } catch (Exception e) {
            log.error("build context parse process form failed! processId : {}", processInstance.getId(), e);
        }

        return new WorkflowContext().setProcess(process)
                .setApplicant(processInstance.getApplicant())
                .setProcessForm(processForm)
                .setProcessInstance(processInstance);
    }

    private WorkflowView getWorkflowView(ProcessInstance processInstance) {
        Process process = workflowDataAccessor.processDefinitionStorage().get(processInstance.getName());
        if (process == null) {
            return null;
        }

        WorkflowContext workflowContext = buildContext(process, processInstance);

        Map<String, TaskState> taskName2TaskStateMap = getTaskName2TaskStateMap(processInstance);

        ElementView start = new ElementView()
                .setName(process.getStartEvent().getName())
                .setDisplayName(process.getStartEvent().getDisplayName());

        addNext(process.getStartEvent(), start, workflowContext, taskName2TaskStateMap);

        return new WorkflowView()
                .setName(process.getName())
                .setDisplayName(process.getDisplayName())
                .setType(process.getType())
                .setStartEvent(start);
    }

    private void addNext(NextableElement nextableElement, ElementView elementView, WorkflowContext context,
                         Map<String, TaskState> taskName2TaskStateMap) {
        for (Element element : nextableElement.getNextList(context)) {
            ElementView nextElement = new ElementView()
                    .setName(element.getName())
                    .setDisplayName(element.getDisplayName());

            if (element instanceof UserTask) {
                nextElement.setApprovers(((UserTask) element).getApproverAssign().assign(context));
                nextElement.setState(taskName2TaskStateMap.get(element.getName()));
            }

            elementView.getNext().add(nextElement);

            if (!(element instanceof NextableElement)) {
                continue;
            }
            addNext((NextableElement) element, nextElement, context, taskName2TaskStateMap);
        }

    }

    private Map<String, TaskState> getTaskName2TaskStateMap(ProcessInstance processInstance) {
        List<TaskInstance> allTaskInstances = workflowDataAccessor.taskInstanceStorage().listByQuery(
                TaskQuery.builder().processInstId(processInstance.getId()).build())
                .stream()
                .sorted(Comparator.comparing(TaskInstance::getId)
                        .thenComparing(Comparator.nullsLast(Comparator.comparing(TaskInstance::getEndTime))))
                .collect(Collectors.toList());

        Map<String, TaskState> taskName2TaskState = Maps.newHashMap();
        allTaskInstances.forEach(taskInstance ->
                taskName2TaskState.put(taskInstance.getName(), TaskState.valueOf(taskInstance.getState())));
        return taskName2TaskState;
    }

    private TaskForm getEmptyTaskForm(Task task) {
        if (!(task instanceof UserTask)) {
            return null;
        }

        UserTask userTask = (UserTask) task;

        if (userTask.getFormClass() == null) {
            return null;
        }

        try {
            return userTask.getFormClass().newInstance();
        } catch (Exception e) {
            throw new WorkflowException("get form name failed " + userTask.getFormClass().getName());
        }
    }

}
