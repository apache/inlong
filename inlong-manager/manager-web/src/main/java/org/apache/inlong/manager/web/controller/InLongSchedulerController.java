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

import org.apache.inlong.manager.common.enums.OperationTarget;
import org.apache.inlong.manager.common.enums.OperationType;
import org.apache.inlong.manager.common.validation.UpdateValidation;
import org.apache.inlong.manager.pojo.common.Response;
import org.apache.inlong.manager.pojo.schedule.ScheduleInfo;
import org.apache.inlong.manager.pojo.schedule.ScheduleInfoRequest;
import org.apache.inlong.manager.pojo.user.LoginUserUtils;
import org.apache.inlong.manager.service.operationlog.OperationLog;
import org.apache.inlong.manager.service.schedule.ScheduleOperator;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiImplicitParam;
import io.swagger.annotations.ApiOperation;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api")
@Api(tags = "Inlong-Schedule-API")
public class InLongSchedulerController {

    @Autowired
    private ScheduleOperator scheduleOperator;

    @RequestMapping(value = "/schedule/save", method = RequestMethod.POST)
    @OperationLog(operation = OperationType.CREATE, operationTarget = OperationTarget.SCHEDULE)
    @ApiOperation(value = "Save schedule info")
    public Response<Integer> save(@RequestBody ScheduleInfoRequest request) {
        int scheduleInfoId = scheduleOperator.saveOpt(request, LoginUserUtils.getLoginUser().getName());
        return Response.success(scheduleInfoId);
    }

    @RequestMapping(value = "/schedule/exist/{groupId}", method = RequestMethod.GET)
    @ApiOperation(value = "Is the schedule info exists for inlong group")
    @ApiImplicitParam(name = "groupId", dataTypeClass = String.class, required = true)
    public Response<Boolean> exist(@PathVariable String groupId) {
        return Response.success(scheduleOperator.scheduleInfoExist(groupId));
    }

    @RequestMapping(value = "/schedule/get", method = RequestMethod.GET)
    @ApiOperation(value = "Get schedule info for inlong group")
    @ApiImplicitParam(name = "groupId", dataTypeClass = String.class, required = true)
    public Response<ScheduleInfo> get(@RequestParam String groupId) {
        return Response.success(scheduleOperator.getScheduleInfo(groupId));
    }

    @RequestMapping(value = "/schedule/update", method = RequestMethod.POST)
    @OperationLog(operation = OperationType.UPDATE, operationTarget = OperationTarget.SCHEDULE)
    @ApiOperation(value = "Update schedule info")
    public Response<Boolean> update(@Validated(UpdateValidation.class) @RequestBody ScheduleInfoRequest request) {
        return Response.success(scheduleOperator.updateOpt(request, LoginUserUtils.getLoginUser().getName()));
    }

    @RequestMapping(value = "/schedule/updateAndRegister", method = RequestMethod.POST)
    @OperationLog(operation = OperationType.UPDATE, operationTarget = OperationTarget.SCHEDULE)
    @ApiOperation(value = "Update schedule info and register to schedule engine")
    public Response<Boolean> updateAndRegister(
            @Validated(UpdateValidation.class) @RequestBody ScheduleInfoRequest request) {
        return Response.success(scheduleOperator.updateAndRegister(request, LoginUserUtils.getLoginUser().getName()));
    }

    @RequestMapping(value = "/schedule/delete/{groupId}", method = RequestMethod.DELETE)
    @ApiOperation(value = "Delete schedule info")
    @OperationLog(operation = OperationType.DELETE, operationTarget = OperationTarget.SCHEDULE)
    @ApiImplicitParam(name = "groupId", value = "Inlong group id", dataTypeClass = String.class, required = true)
    public Response<Boolean> delete(@PathVariable String groupId) {
        String operator = LoginUserUtils.getLoginUser().getName();
        return Response.success(scheduleOperator.deleteByGroupIdOpt(groupId, operator));
    }

}
