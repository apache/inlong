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
import org.apache.inlong.manager.common.beans.Response;
import org.apache.inlong.manager.common.enums.OperationType;
import org.apache.inlong.manager.service.core.WorkflowEventService;
import org.apache.inlong.manager.service.core.operationlog.OperationLog;
import org.apache.inlong.manager.common.event.process.ProcessEvent;
import org.apache.inlong.manager.common.event.task.TaskEvent;
import org.apache.inlong.manager.common.model.view.EventLogQuery;
import org.apache.inlong.manager.common.model.view.EventLogView;
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
    @ApiImplicitParam(name = "id", value = "event id", dataTypeClass = Integer.class, required = true)
    public Response<EventLogView> get(@PathVariable Integer id) {
        return Response.success(workflowEventService.get(id));
    }

    @GetMapping("/list")
    @ApiOperation(value = "Query event list based on conditions")
    public Response<PageInfo<EventLogView>> list(EventLogQuery query) {
        return Response.success(workflowEventService.list(query));
    }

    @PostMapping("executeEventListener/{id}")
    @OperationLog(operation = OperationType.UPDATE)
    @ApiOperation(value = "Execute the listener based on the log ID")
    @ApiImplicitParam(name = "id", value = "event id", dataTypeClass = Integer.class, required = true)
    public Response<Object> executeEventListener(@PathVariable Integer id) {
        workflowEventService.executeEventListener(id);
        return Response.success();
    }

    @PostMapping("executeProcessEventListener")
    @OperationLog(operation = OperationType.UPDATE)
    @ApiOperation(value = "Re-execute the specified listener according to the process ID")
    @ApiImplicitParams({
            @ApiImplicitParam(name = "processInstId", value = "process id", dataTypeClass = Integer.class),
            @ApiImplicitParam(name = "listenerName", value = "listener name", dataTypeClass = String.class)
    })
    public Response<Object> executeProcessEventListener(@RequestParam Integer processInstId,
            @RequestParam String listenerName) {
        workflowEventService.executeProcessEventListener(processInstId, listenerName);
        return Response.success();
    }

    @PostMapping("executeTaskEventListener")
    @OperationLog(operation = OperationType.UPDATE)
    @ApiOperation(value = "Re-execute the specified listener based on the task ID")
    @ApiImplicitParams({
            @ApiImplicitParam(name = "taskInstId", value = "task id", dataTypeClass = Integer.class),
            @ApiImplicitParam(name = "listenerName", value = "listener name", dataTypeClass = String.class)
    })
    public Response<Object> executeTaskEventListener(Integer taskInstId, String listenerName) {
        workflowEventService.executeTaskEventListener(taskInstId, listenerName);
        return Response.success();
    }

    @PostMapping("triggerProcessEvent")
    @OperationLog(operation = OperationType.UPDATE)
    @ApiOperation(value = "Re-trigger the process event based on the process ID")
    @ApiImplicitParams({
            @ApiImplicitParam(name = "processInstId", value = "process id", dataTypeClass = Integer.class),
            @ApiImplicitParam(name = "processEvent", value = "process event", dataTypeClass = ProcessEvent.class)
    })
    public Response<Object> triggerProcessEvent(Integer processInstId, ProcessEvent processEvent) {
        workflowEventService.triggerProcessEvent(processInstId, processEvent);
        return Response.success();
    }

    @PostMapping("triggerTaskEvent")
    @OperationLog(operation = OperationType.UPDATE)
    @ApiOperation(value = "Re-trigger the task event based on the task ID")
    @ApiImplicitParams({
            @ApiImplicitParam(name = "taskInstId", value = "task id", dataTypeClass = Integer.class),
            @ApiImplicitParam(name = "taskEvent", value = "task event", dataTypeClass = TaskEvent.class)
    })
    public Response<Object> triggerTaskEvent(Integer taskInstId, TaskEvent taskEvent) {
        workflowEventService.triggerTaskEvent(taskInstId, taskEvent);
        return Response.success();
    }

}
