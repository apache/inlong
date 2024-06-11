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
import org.apache.inlong.manager.pojo.common.PageResult;
import org.apache.inlong.manager.pojo.common.Response;
import org.apache.inlong.manager.pojo.stream.TemplateInfo;
import org.apache.inlong.manager.pojo.stream.TemplatePageRequest;
import org.apache.inlong.manager.pojo.stream.TemplateRequest;
import org.apache.inlong.manager.pojo.user.LoginUserUtils;
import org.apache.inlong.manager.pojo.user.UserRoleCode;
import org.apache.inlong.manager.service.operationlog.OperationLog;
import org.apache.inlong.manager.service.stream.TemplateService;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiImplicitParam;
import io.swagger.annotations.ApiOperation;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * Inlong Template control layer
 */
@Slf4j
@RestController
@RequestMapping("/api")
@Api(tags = "Inlong-Template-API")
public class TemplateController {

    @Autowired
    private TemplateService templateService;

    @RequestMapping(value = "/template/save", method = RequestMethod.POST)
    @OperationLog(operation = OperationType.CREATE, operationTarget = OperationTarget.TEMPLATE)
    @ApiOperation(value = "Save inlong template")
    public Response<Integer> save(@RequestBody TemplateRequest request) {
        int result = templateService.save(request, LoginUserUtils.getLoginUser().getName());
        return Response.success(result);
    }

    @RequestMapping(value = "/template/exist/{templateName}", method = RequestMethod.GET)
    @ApiOperation(value = "Is the inlong template exists")
    @ApiImplicitParam(name = "templateName", dataTypeClass = String.class, required = true)
    public Response<Boolean> exist(@PathVariable String templateName) {
        return Response.success(templateService.exist(templateName));
    }

    @RequestMapping(value = "/template/get", method = RequestMethod.GET)
    @ApiOperation(value = "Get inlong template")
    @ApiImplicitParam(name = "templateName", dataTypeClass = String.class, required = true)
    public Response<TemplateInfo> get(@RequestParam String templateName) {
        return Response.success(templateService.get(templateName, LoginUserUtils.getLoginUser().getName()));
    }

    @RequestMapping(value = "/template/list", method = RequestMethod.POST)
    @ApiOperation(value = "List inlong template briefs by paginating")
    public Response<PageResult<TemplateInfo>> listByCondition(@RequestBody TemplatePageRequest request) {
        request.setCurrentUser(LoginUserUtils.getLoginUser().getName());
        request.setIsAdminRole(LoginUserUtils.getLoginUser().getRoles().contains(UserRoleCode.TENANT_ADMIN));
        return Response.success(templateService.list(request));
    }

    @RequestMapping(value = "/template/update", method = RequestMethod.POST)
    @OperationLog(operation = OperationType.UPDATE, operationTarget = OperationTarget.TEMPLATE)
    @ApiOperation(value = "Update inlong templater")
    public Response<Boolean> update(@Validated(UpdateValidation.class) @RequestBody TemplateRequest request) {
        String username = LoginUserUtils.getLoginUser().getName();
        return Response.success(templateService.update(request, username));
    }

    @Deprecated
    @RequestMapping(value = "/template/delete", method = RequestMethod.DELETE)
    @OperationLog(operation = OperationType.DELETE, operationTarget = OperationTarget.TEMPLATE)
    @ApiOperation(value = "Delete inlong template")
    @ApiImplicitParam(name = "templateName", dataTypeClass = String.class, required = true)
    public Response<Boolean> delete(@RequestParam String templateName) {
        String username = LoginUserUtils.getLoginUser().getName();
        return Response.success(templateService.delete(templateName, username));
    }

}
