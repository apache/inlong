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
import io.swagger.annotations.ApiOperation;
import org.apache.inlong.manager.common.enums.OperationType;
import org.apache.inlong.manager.pojo.common.Response;
import org.apache.inlong.manager.pojo.consumption.ConsumptionListVo;
import org.apache.inlong.manager.pojo.consumption.ConsumptionQuery;
import org.apache.inlong.manager.pojo.consumption.ConsumptionSummary;
import org.apache.inlong.manager.pojo.consumption.InlongConsumeInfo;
import org.apache.inlong.manager.pojo.consumption.InlongConsumeRequest;
import org.apache.inlong.manager.service.consume.InlongConsumeService;
import org.apache.inlong.manager.service.group.InlongGroupProcessService;
import org.apache.inlong.manager.service.group.InlongGroupService;
import org.apache.inlong.manager.service.operationlog.OperationLog;
import org.apache.inlong.manager.service.user.LoginUserUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

/**
 * Inlong consume control layer
 */
@RestController
@RequestMapping("/api")
@Api(tags = "Inlong-Consume-API")
public class InlongConsumeController {

    @Autowired
    private InlongGroupService groupService;
    @Autowired
    private InlongGroupProcessService groupProcessOperation;
    @Autowired
    private InlongConsumeService consumeService;

    @RequestMapping(value = "/consume/save", method = RequestMethod.POST)
    @OperationLog(operation = OperationType.CREATE)
    @ApiOperation(value = "Save inlong consume")
    public Response<Integer> save(@Validated @RequestBody InlongConsumeRequest consumeRequest) {
        String operator = LoginUserUtils.getLoginUser().getName();
        return Response.success(consumeService.save(consumeRequest, operator));
    }

    @PostMapping("/consume/update/{id}")
    @OperationLog(operation = OperationType.UPDATE)
    @ApiOperation(value = "Update data consumption")
    public Response<String> update(@Validated @RequestBody InlongConsumeRequest consumeRequest) {
        consumeService.update(consumeRequest, LoginUserUtils.getLoginUser().getName());
        return Response.success();
    }

    @GetMapping("/consume/get/{id}")
    @ApiOperation(value = "Get consumption details")
    @ApiImplicitParam(name = "id", value = "Consumption ID", dataTypeClass = Integer.class, required = true)
    public Response<InlongConsumeInfo> getDetail(@PathVariable(name = "id") Integer id) {
        return Response.success(consumeService.get(id));
    }

    @DeleteMapping("/consume/delete/{id}")
    @OperationLog(operation = OperationType.DELETE)
    @ApiOperation(value = "Delete data consumption")
    @ApiImplicitParam(name = "id", value = "Consumption ID", dataTypeClass = Integer.class, required = true)
    public Response<Object> delete(@PathVariable(name = "id") Integer id) {
        this.consumeService.delete(id, LoginUserUtils.getLoginUser().getName());
        return Response.success();
    }

    @GetMapping("/consume/list")
    @ApiOperation(value = "List data consumptions")
    public Response<PageInfo<ConsumptionListVo>> list(ConsumptionQuery query) {
        query.setUsername(LoginUserUtils.getLoginUser().getName());
        return Response.success(consumeService.list(query));
    }

    @GetMapping("/consume/summary")
    @ApiOperation(value = "Get data consumption summary")
    public Response<ConsumptionSummary> getSummary(ConsumptionQuery query) {
        query.setUsername(LoginUserUtils.getLoginUser().getName());
        return Response.success(consumeService.getSummary(query));
    }

}