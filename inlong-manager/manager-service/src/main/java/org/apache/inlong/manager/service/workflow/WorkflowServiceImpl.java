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

package org.apache.inlong.manager.service.workflow;

import com.github.pagehelper.Page;
import com.github.pagehelper.PageHelper;
import com.github.pagehelper.PageInfo;
import com.google.common.collect.Maps;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import javax.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import org.apache.inlong.manager.common.util.Preconditions;
import org.apache.inlong.manager.service.workflow.WorkflowTaskExecuteLog.ListenerExecutorLog;
import org.apache.inlong.manager.service.workflow.WorkflowTaskExecuteLog.TaskExecutorLog;
import org.apache.inlong.manager.common.workflow.QueryService;
import org.apache.inlong.manager.common.workflow.WorkflowEngine;
import org.apache.inlong.manager.common.exceptions.WorkflowNoRollbackException;
import org.apache.inlong.manager.common.model.TaskState;
import org.apache.inlong.manager.common.model.definition.Process;
import org.apache.inlong.manager.common.model.definition.ProcessForm;
import org.apache.inlong.manager.common.model.definition.TaskForm;
import org.apache.inlong.manager.common.model.definition.UserTask;
import org.apache.inlong.manager.common.model.instance.ProcessInstance;
import org.apache.inlong.manager.common.model.instance.TaskInstance;
import org.apache.inlong.manager.common.model.view.EventLogQuery;
import org.apache.inlong.manager.common.model.view.ProcessDetail;
import org.apache.inlong.manager.common.model.view.ProcessListView;
import org.apache.inlong.manager.common.model.view.ProcessQuery;
import org.apache.inlong.manager.common.model.view.ProcessSummaryQuery;
import org.apache.inlong.manager.common.model.view.ProcessSummaryView;
import org.apache.inlong.manager.common.model.view.TaskListView;
import org.apache.inlong.manager.common.model.view.TaskQuery;
import org.apache.inlong.manager.common.model.view.TaskSummaryQuery;
import org.apache.inlong.manager.common.model.view.TaskSummaryView;
import org.apache.inlong.manager.workflow.util.WorkflowFormParserUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.CollectionUtils;

/**
 * Workflow service
 */
@Slf4j
@Service
public class WorkflowServiceImpl implements WorkflowService {

    private WorkflowEngine workflowEngine;

    @Autowired
    private List<WorkflowDefinition> workflowDefinitions;

    @Autowired
    public WorkflowServiceImpl(WorkflowEngine workflowEngine) {
        this.workflowEngine = workflowEngine;
    }

    @PostConstruct
    private void init() {
        log.info("start init workflow service");
        workflowDefinitions.forEach(definition -> {
            workflowEngine.processDefinitionService().register(definition.defineProcess());
            log.info("success register workflow definition: {}", definition.getProcessName());
        });
        log.info("success init workflow service");
    }

    @Override
    @Transactional(noRollbackFor = WorkflowNoRollbackException.class, rollbackFor = Exception.class)
    public WorkflowResult start(ProcessName process, String applicant, ProcessForm form) {
        return WorkflowResult.of(workflowEngine.processService().start(process.name(), applicant, form));
    }

    @Override
    @Transactional(noRollbackFor = WorkflowNoRollbackException.class, rollbackFor = Exception.class)
    public WorkflowResult cancel(Integer processInstId, String operator, String remark) {
        return WorkflowResult.of(workflowEngine.processService().cancel(processInstId, operator, remark));
    }

    @Override
    @Transactional(noRollbackFor = WorkflowNoRollbackException.class, rollbackFor = Exception.class)
    public WorkflowResult approve(Integer taskId, String remark, TaskForm form, String operator) {
        return WorkflowResult.of(workflowEngine.taskService().approve(taskId, remark, form, operator));
    }

    @Override
    @Transactional(noRollbackFor = WorkflowNoRollbackException.class, rollbackFor = Exception.class)
    public WorkflowResult reject(Integer taskId, String remark, String operator) {
        return WorkflowResult.of(workflowEngine.taskService().reject(taskId, remark, operator));
    }

    @Override
    @Transactional(noRollbackFor = WorkflowNoRollbackException.class, rollbackFor = Exception.class)
    public WorkflowResult transfer(Integer taskId, String remark, List<String> to, String operator) {
        return WorkflowResult.of(workflowEngine.taskService().transfer(taskId, remark, to, operator));
    }

    @Override
    @Transactional(noRollbackFor = WorkflowNoRollbackException.class, rollbackFor = Exception.class)
    public WorkflowResult complete(Integer taskId, String remark, String operator) {
        return WorkflowResult.of(workflowEngine.taskService().complete(taskId, remark, operator));
    }

    @Override
    public ProcessDetail detail(Integer processInstId, Integer taskInstId) {
        return workflowEngine.queryService().detail(processInstId, taskInstId);
    }

    @Override
    public PageInfo<ProcessListView> listProcess(ProcessQuery query) {
        PageHelper.startPage(query.getPageNum(), query.getPageSize());
        Page<ProcessInstance> result = (Page<ProcessInstance>) workflowEngine.queryService().listProcess(query);
        PageInfo<ProcessListView> pageInfo = result.toPageInfo(processInstance -> {
            ProcessListView processListView = ProcessListView.fromProcessInstance(processInstance);
            if (query.isIncludeShowInList()) {
                processListView.setShowInList(getShowInList(processInstance));
            }
            return processListView;
        });

        pageInfo.setTotal(result.getTotal());

        if (query.isIncludeCurrentTask()) {
            TaskQuery baseTaskQuery = TaskQuery.builder()
                    .type(UserTask.class.getSimpleName())
                    .states(Collections.singleton(TaskState.PENDING))
                    .build();
            PageHelper.startPage(0, 100);
            pageInfo.getList().forEach(addCurrentTask(baseTaskQuery));
        }
        return pageInfo;
    }

    @Override
    public PageInfo<TaskListView> listTask(TaskQuery query) {
        PageHelper.startPage(query.getPageNum(), query.getPageSize());
        Page<TaskInstance> result = (Page<TaskInstance>) workflowEngine.queryService().listTask(query);
        PageInfo<TaskListView> pageInfo = result.toPageInfo(TaskListView::fromTaskInstance);
        addShowInListForEachTask(pageInfo.getList());
        pageInfo.setTotal(result.getTotal());

        return pageInfo;
    }

    @Override
    public ProcessSummaryView processSummary(ProcessSummaryQuery query) {
        return workflowEngine.queryService().processSummary(query);
    }

    @Override
    public TaskSummaryView taskSummary(TaskSummaryQuery query) {
        return workflowEngine.queryService().taskSummary(query);
    }

    @Override
    public PageInfo<WorkflowTaskExecuteLog> listTaskExecuteLogs(WorkflowTaskExecuteLogQuery query) {
        Preconditions.checkNotNull(query, "workflow task execute log query params cannot be null");

        String groupId = query.getInlongGroupId();
        List<String> processNameList = query.getProcessNames();
        Preconditions.checkNotEmpty(groupId, "inlong group id cannot be null");
        Preconditions.checkNotEmpty(processNameList, "process name list cannot be null");

        ProcessQuery processQuery = new ProcessQuery();
        processQuery.setInlongGroupId(groupId);
        processQuery.setNameList(processNameList);
        processQuery.setHidden(true);

        // Paging query process instance, construct process execution log
        QueryService queryService = workflowEngine.queryService();
        PageHelper.startPage(query.getPageNum(), query.getPageSize());
        Page<ProcessInstance> instanceList = (Page<ProcessInstance>) queryService.listProcess(processQuery);

        PageInfo<WorkflowTaskExecuteLog> pageInfo = instanceList.toPageInfo(inst -> WorkflowTaskExecuteLog.builder()
                .processInstId(inst.getId())
                .processDisplayName(inst.getDisplayName())
                .state(inst.getState())
                .startTime(inst.getStartTime())
                .endTime(inst.getEndTime())
                .build()
        );

        // According to the process execution log, query the execution log of each task in the process
        for (WorkflowTaskExecuteLog executeLog : pageInfo.getList()) {
            TaskQuery taskQuery = new TaskQuery();
            taskQuery.setProcessInstId(executeLog.getProcessInstId());
            taskQuery.setType(query.getTaskType());
            List<TaskExecutorLog> taskExecutorLogs = queryService.listTask(taskQuery)
                    .stream()
                    .map(TaskExecutorLog::buildFromTaskInst)
                    .collect(Collectors.toList());

            // Set the execution log of the task's listener
            for (TaskExecutorLog taskExecutorLog : taskExecutorLogs) {
                EventLogQuery eventLogQuery = new EventLogQuery();
                eventLogQuery.setTaskInstId(taskExecutorLog.getTaskInstId());
                List<ListenerExecutorLog> logs = queryService.listEventLog(eventLogQuery)
                        .stream()
                        .map(ListenerExecutorLog::fromEventLog)
                        .collect(Collectors.toList());
                taskExecutorLog.setListenerExecutorLogs(logs);
            }

            executeLog.setTaskExecutorLogs(taskExecutorLogs);
        }

        log.info("success to page list task execute logs for " + query);
        pageInfo.setTotal(instanceList.getTotal());
        return pageInfo;
    }

    private List<TaskExecutorLog> getTaskExecutorLogs(Integer processInstId, String taskType) {
        return workflowEngine.queryService().listTask(TaskQuery.builder()
                        .processInstId(processInstId).type(taskType)
                        .build())
                .stream()
                .map(TaskExecutorLog::buildFromTaskInst)
                .collect(Collectors.toList());
    }

    private List<ListenerExecutorLog> getListenerExecutorLogs(TaskExecutorLog taskExecutorLog) {
        return workflowEngine.queryService()
                .listEventLog(EventLogQuery.builder().taskInstId(taskExecutorLog.getTaskInstId()).build())
                .stream()
                .map(ListenerExecutorLog::fromEventLog)
                .collect(Collectors.toList());
    }

    private Consumer<ProcessListView> addCurrentTask(TaskQuery baseTaskQuery) {
        return plv -> {
            baseTaskQuery.setProcessInstId(plv.getId());
            plv.setCurrentTasks(this.listTask(baseTaskQuery).getList());
        };
    }

    private Map<String, Object> getShowInList(ProcessInstance processInstance) {
        Process process = workflowEngine.processDefinitionService().getByName(processInstance.getName());
        if (process == null || process.getFormClass() == null) {
            return null;
        }

        try {
            ProcessForm processForm = WorkflowFormParserUtils.parseProcessForm(processInstance.getFormData(), process);
            return processForm.showInList();
        } catch (Exception e) {
            log.error("get showIn list err", e);
        }
        return null;
    }

    private void addShowInListForEachTask(List<TaskListView> taskList) {
        if (CollectionUtils.isEmpty(taskList)) {
            return;
        }
        PageHelper.clearPage();
        List<Integer> processInstIds = taskList.stream().map(TaskListView::getProcessInstId)
                .distinct().collect(Collectors.toList());
        List<ProcessInstance> processInstances = this.workflowEngine.queryService().listProcess(
                ProcessQuery.builder().idList(processInstIds).build());
        Map<Integer, Map<String, Object>> process2ShowInListMap = Maps.newHashMap();
        processInstances.forEach(p -> process2ShowInListMap.put(p.getId(), getShowInList(p)));
        taskList.forEach(task -> task.setShowInList(process2ShowInListMap.get(task.getProcessInstId())));
    }
}
