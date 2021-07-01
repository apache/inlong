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

import org.apache.inlong.manager.common.beans.PageResult;
import org.apache.inlong.manager.common.beans.Response;
import org.apache.inlong.manager.common.enums.OperationType;
import org.apache.inlong.manager.service.core.WorkflowEventService;
import org.apache.inlong.manager.service.core.operationlog.OperationLog;
import org.apache.inlong.manager.workflow.core.event.process.ProcessEvent;
import org.apache.inlong.manager.workflow.core.event.task.TaskEvent;
import org.apache.inlong.manager.workflow.model.view.EventLogQuery;
import org.apache.inlong.manager.workflow.model.view.EventLogView;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * Workflow event related interface
 */
@RestController
@RequestMapping("/workflow/event")
@Api(tags = "Workflow Event")
public class WorkflowEventController {

    @Autowired
    private WorkflowEventService workflowEventService;

    @GetMapping("/detail/{id}")
    @ApiOperation(value = "Get event details")
    public Response<EventLogView> get(
            @ApiParam(value = "Event ID", required = true) @PathVariable Integer id) {
        return Response.success(workflowEventService.get(id));
    }

    @GetMapping("/list")
    @ApiOperation(value = "Query event list based on conditions")
    public Response<PageResult<EventLogView>> list(EventLogQuery query) {
        return Response.success(workflowEventService.list(query));
    }

    @PostMapping("executeEventListener/{id}")
    @OperationLog(operation = OperationType.UPDATE)
    @ApiOperation(value = "Execute the listener based on the log ID")
    public Response executeEventListener(@ApiParam(value = "Event ID", required = true) @PathVariable Integer id) {
        workflowEventService.executeEventListener(id);
        return Response.success();
    }

    @PostMapping("executeProcessEventListener")
    @OperationLog(operation = OperationType.UPDATE)
    @ApiOperation(value = "Re-execute the specified listener according to the process ID")
    public Response executeProcessEventListener(
            @ApiParam(value = "Process ID", required = true) @RequestParam Integer processInstId,
            @ApiParam(value = "Listener name", required = true) @RequestParam String listenerName) {
        workflowEventService.executeProcessEventListener(processInstId, listenerName);
        return Response.success();
    }

    @PostMapping("executeTaskEventListener")
    @OperationLog(operation = OperationType.UPDATE)
    @ApiOperation(value = "Re-execute the specified listener based on the task ID")
    public Response executeTaskEventListener(
            @ApiParam(value = "Task ID", required = true) Integer taskInstId,
            @ApiParam(value = "Listener name", required = true) String listenerName) {
        workflowEventService.executeTaskEventListener(taskInstId, listenerName);
        return Response.success();
    }

    @PostMapping("triggerProcessEvent")
    @OperationLog(operation = OperationType.UPDATE)
    @ApiOperation(value = "Re-trigger the process event based on the process ID")
    public Response triggerProcessEvent(
            @ApiParam(value = "Process ID", required = true) Integer processInstId,
            @ApiParam(value = "Process event", required = true) ProcessEvent processEvent) {
        workflowEventService.triggerProcessEvent(processInstId, processEvent);
        return Response.success();
    }

    @PostMapping("triggerTaskEvent")
    @OperationLog(operation = OperationType.UPDATE)
    @ApiOperation(value = "Re-trigger the task event based on the task ID")
    public Response triggerTaskEvent(
            @ApiParam(value = "Task ID", required = true) Integer taskInstId,
            @ApiParam(value = "Task event", required = true) TaskEvent taskEvent) {
        workflowEventService.triggerTaskEvent(taskInstId, taskEvent);
        return Response.success();
    }
}
