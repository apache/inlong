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

package org.apache.inlong.manager.web.controller;

import com.github.pagehelper.PageInfo;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiImplicitParam;
import io.swagger.annotations.ApiImplicitParams;
import io.swagger.annotations.ApiOperation;
import lombok.extern.slf4j.Slf4j;
import org.apache.inlong.manager.common.beans.Response;
import org.apache.inlong.manager.common.enums.OperationType;
import org.apache.inlong.manager.common.util.LoginUserUtil;
import org.apache.inlong.manager.service.core.operationlog.OperationLog;
import org.apache.inlong.manager.service.workflow.WorkflowOperation;
import org.apache.inlong.manager.service.workflow.WorkflowResult;
import org.apache.inlong.manager.service.workflow.WorkflowService;
import org.apache.inlong.manager.service.workflow.WorkflowTaskExecuteLog;
import org.apache.inlong.manager.service.workflow.WorkflowTaskExecuteLogQuery;
import org.apache.inlong.manager.service.workflow.WorkflowTaskOperation;
import org.apache.inlong.manager.common.model.view.ProcessDetail;
import org.apache.inlong.manager.common.model.view.ProcessListView;
import org.apache.inlong.manager.common.model.view.ProcessQuery;
import org.apache.inlong.manager.common.model.view.ProcessSummaryQuery;
import org.apache.inlong.manager.common.model.view.ProcessSummaryView;
import org.apache.inlong.manager.common.model.view.TaskListView;
import org.apache.inlong.manager.common.model.view.TaskQuery;
import org.apache.inlong.manager.common.model.view.TaskSummaryQuery;
import org.apache.inlong.manager.common.model.view.TaskSummaryView;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * Workflow controller
 */
@Slf4j
@RestController
@RequestMapping("/workflow")
@Api(tags = "Workflow Config")
public class WorkflowController {

    @Autowired
    private WorkflowService workflowService;

    @PostMapping("start")
    @OperationLog(operation = OperationType.CREATE)
    @ApiOperation(value = "Initiation process")
    public Response<WorkflowResult> start(@RequestBody WorkflowOperation operation) {
        String applicant = LoginUserUtil.getLoginUserDetail().getUserName();
        return Response.success(workflowService.start(operation.getName(), applicant, operation.getForm()));
    }

    @PostMapping("cancel/{processId}")
    @OperationLog(operation = OperationType.UPDATE)
    @ApiOperation(value = "Cancellation process")
    @ApiImplicitParam(name = "processId", value = "Process ID", dataTypeClass = Integer.class, required = true)
    public Response<WorkflowResult> cancel(@PathVariable Integer processId, @RequestBody WorkflowOperation operation) {
        String operator = LoginUserUtil.getLoginUserDetail().getUserName();
        return Response.success(workflowService.cancel(processId, operator, operation.getRemark()));
    }

    @PostMapping("approve/{taskId}")
    @OperationLog(operation = OperationType.UPDATE)
    @ApiOperation(value = "Approval and consent")
    @ApiImplicitParam(name = "taskId", value = "Task ID", dataTypeClass = Integer.class, required = true)
    public Response<WorkflowResult> approve(@PathVariable Integer taskId,
            @RequestBody WorkflowTaskOperation operation) {
        String operator = LoginUserUtil.getLoginUserDetail().getUserName();
        return Response.success(workflowService.approve(taskId, operation.getRemark(), operation.getForm(), operator));
    }

    @PostMapping("reject/{taskId}")
    @OperationLog(operation = OperationType.UPDATE)
    @ApiOperation(value = "Approval rejected")
    @ApiImplicitParam(name = "taskId", value = "Task ID", dataTypeClass = Integer.class, required = true)
    public Response<WorkflowResult> reject(@PathVariable Integer taskId, @RequestBody WorkflowTaskOperation operation) {
        String operator = LoginUserUtil.getLoginUserDetail().getUserName();
        return Response.success(workflowService.reject(taskId, operation.getRemark(), operator));
    }

    @PostMapping("transfer/{taskId}")
    @OperationLog(operation = OperationType.UPDATE)
    @ApiOperation(value = "Turn to do", notes = "Change approver")
    @ApiImplicitParam(name = "taskId", value = "Task ID", dataTypeClass = Integer.class, required = true)
    public Response<WorkflowResult> transfer(@PathVariable Integer taskId,
            @RequestBody WorkflowTaskOperation operation) {
        String operator = LoginUserUtil.getLoginUserDetail().getUserName();
        return Response.success(workflowService.transfer(taskId, operation.getRemark(),
                operation.getTransferTo(), operator));
    }

    @PostMapping("complete/{taskId}")
    @OperationLog(operation = OperationType.UPDATE)
    @ApiOperation(value = "Finish")
    @ApiImplicitParam(name = "taskId", value = "Task ID", dataTypeClass = Integer.class, required = true)
    public Response<WorkflowResult> complete(@PathVariable Integer taskId,
            @RequestBody WorkflowTaskOperation operation) {
        String operator = LoginUserUtil.getLoginUserDetail().getUserName();
        return Response.success(workflowService.complete(taskId, operation.getRemark(), operator));
    }

    @GetMapping("/detail/{id}")
    @ApiOperation(value = "Get application details")
    @ApiImplicitParams({
            @ApiImplicitParam(name = "id", value = "Process ticket ID", dataTypeClass = Integer.class, required = true),
            @ApiImplicitParam(name = "taskInstId", value = "Task ID", dataTypeClass = Integer.class)
    })
    public Response<ProcessDetail> detail(@PathVariable(name = "id") Integer id,
            @RequestParam(required = false) Integer taskInstId) {
        return Response.success(workflowService.detail(id, taskInstId));
    }

    @GetMapping("/listProcess")
    @ApiOperation(value = "Get my list of process sheets")
    public Response<PageInfo<ProcessListView>> listProcess(ProcessQuery query) {
        query.setApplicant(LoginUserUtil.getLoginUserDetail().getUserName());
        return Response.success(workflowService.listProcess(query));
    }

    @GetMapping("/listTask")
    @ApiOperation(value = "Get my task list")
    public Response<PageInfo<TaskListView>> listTask(TaskQuery query) {
        query.setApprover(LoginUserUtil.getLoginUserDetail().getUserName());
        return Response.success(workflowService.listTask(query));
    }

    @GetMapping("/processSummary")
    @ApiOperation(value = "Obtain application form statistics")
    public Response<ProcessSummaryView> processSummary(ProcessSummaryQuery query) {
        query.setApplicant(LoginUserUtil.getLoginUserDetail().getUserName());
        return Response.success(workflowService.processSummary(query));
    }

    @GetMapping("/taskSummary")
    @ApiOperation(value = "Get approval task statistics")
    public Response<TaskSummaryView> taskSummary(TaskSummaryQuery query) {
        query.setApprover(LoginUserUtil.getLoginUserDetail().getUserName());
        return Response.success(workflowService.taskSummary(query));
    }

    @GetMapping("/listTaskExecuteLogs")
    @ApiOperation(value = "Get task execution log")
    public Response<PageInfo<WorkflowTaskExecuteLog>> listTaskExecuteLogs(WorkflowTaskExecuteLogQuery query) {
        return Response.success(workflowService.listTaskExecuteLogs(query));
    }

}
