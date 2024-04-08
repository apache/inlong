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

import org.apache.inlong.manager.common.validation.SaveValidation;
import org.apache.inlong.manager.common.validation.UpdateValidation;
import org.apache.inlong.manager.pojo.common.PageResult;
import org.apache.inlong.manager.pojo.common.Response;
import org.apache.inlong.manager.pojo.module.ModulePageRequest;
import org.apache.inlong.manager.pojo.module.ModuleRequest;
import org.apache.inlong.manager.pojo.module.ModuleResponse;
import org.apache.inlong.manager.pojo.user.LoginUserUtils;
import org.apache.inlong.manager.service.module.ModuleService;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiImplicitParam;
import io.swagger.annotations.ApiOperation;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

/**
 * Inlong module control layer
 */
@RestController
@RequestMapping("/api")
@Api(tags = "Inlong-Module-API")
public class ModuleController {

    @Autowired
    private ModuleService moduleService;

    @RequestMapping(value = "/module/save", method = RequestMethod.POST)
    @ApiOperation(value = "Save inlong module")
    public Response<Integer> save(@Validated(SaveValidation.class) @RequestBody ModuleRequest request) {
        String operator = LoginUserUtils.getLoginUser().getName();
        return Response.success(moduleService.save(request, operator));
    }

    @RequestMapping(value = "/module/update", method = RequestMethod.POST)
    @ApiOperation(value = "Update inlong module")
    public Response<Boolean> update(@Validated(UpdateValidation.class) @RequestBody ModuleRequest request) {
        return Response.success(moduleService.update(request, LoginUserUtils.getLoginUser().getName()));
    }

    @RequestMapping(value = "/module/get/{id}", method = RequestMethod.GET)
    @ApiOperation(value = "Get module config")
    @ApiImplicitParam(name = "id", dataTypeClass = Integer.class, required = true)
    public Response<ModuleResponse> get(@PathVariable Integer id) {
        return Response.success(moduleService.get(id, LoginUserUtils.getLoginUser()));
    }

    @RequestMapping(value = "/module/list", method = RequestMethod.POST)
    @ApiOperation(value = "List module by paginating")
    public Response<PageResult<ModuleResponse>> listByCondition(@RequestBody ModulePageRequest request) {
        return Response.success(moduleService.listByCondition(request));
    }

    @RequestMapping(value = "/module/delete/{id}", method = RequestMethod.DELETE)
    @ApiOperation(value = "Delete module config")
    public Response<Boolean> delete(@PathVariable Integer id) {
        return Response.success(moduleService.delete(id, LoginUserUtils.getLoginUser().getName()));
    }

}