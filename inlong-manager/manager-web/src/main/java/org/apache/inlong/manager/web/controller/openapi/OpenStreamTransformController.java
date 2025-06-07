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

package org.apache.inlong.manager.web.controller.openapi;

import org.apache.inlong.manager.common.enums.ErrorCodeEnum;
import org.apache.inlong.manager.common.enums.OperationTarget;
import org.apache.inlong.manager.common.enums.OperationType;
import org.apache.inlong.manager.common.util.Preconditions;
import org.apache.inlong.manager.common.validation.UpdateValidation;
import org.apache.inlong.manager.pojo.common.PageResult;
import org.apache.inlong.manager.pojo.common.Response;
import org.apache.inlong.manager.pojo.transform.DeleteTransformRequest;
import org.apache.inlong.manager.pojo.transform.TransformFunctionDocRequest;
import org.apache.inlong.manager.pojo.transform.TransformFunctionDocResponse;
import org.apache.inlong.manager.pojo.transform.TransformRequest;
import org.apache.inlong.manager.pojo.transform.TransformResponse;
import org.apache.inlong.manager.pojo.user.LoginUserUtils;
import org.apache.inlong.manager.service.operationlog.OperationLog;
import org.apache.inlong.manager.service.transform.StreamTransformService;
import org.apache.inlong.manager.service.transform.TransformFunctionDocService;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import javax.annotation.Resource;

import java.util.List;

/**
 * Open InLong Stream transform controller
 */
@RestController
@RequestMapping("/openapi")
@Api(tags = "Open-StreamTransform-API")
public class OpenStreamTransformController {

    @Autowired
    protected StreamTransformService streamTransformService;

    @Resource
    private TransformFunctionDocService transformFunctionDocService;

    @RequestMapping(value = "/transform/list", method = RequestMethod.GET)
    @ApiOperation(value = "Get stream transform list")
    public Response<List<TransformResponse>> list(@RequestParam("inlongGroupId") String groupId,
            @RequestParam("inlongStreamId") String streamId) {
        Preconditions.expectNotBlank(groupId, ErrorCodeEnum.INVALID_PARAMETER, "groupId cannot be blank");
        Preconditions.expectNotNull(LoginUserUtils.getLoginUser(), ErrorCodeEnum.LOGIN_USER_EMPTY);
        return Response.success(streamTransformService.listTransform(groupId, streamId));
    }

    @RequestMapping(value = "/transform/save", method = RequestMethod.POST)
    @OperationLog(operation = OperationType.CREATE, operationTarget = OperationTarget.STREAM)
    @ApiOperation(value = "Save stream transform")
    public Response<Integer> save(@Validated @RequestBody TransformRequest request) {
        Preconditions.expectNotNull(request, ErrorCodeEnum.INVALID_PARAMETER, "request cannot be null");
        Preconditions.expectNotNull(LoginUserUtils.getLoginUser(), ErrorCodeEnum.LOGIN_USER_EMPTY);
        return Response.success(
                streamTransformService.save(request, LoginUserUtils.getLoginUser().getName()));
    }

    @RequestMapping(value = "/transform/update", method = RequestMethod.POST)
    @OperationLog(operation = OperationType.UPDATE, operationTarget = OperationTarget.STREAM)
    @ApiOperation(value = "Update stream transform")
    public Response<Boolean> update(@Validated(UpdateValidation.class) @RequestBody TransformRequest request) {
        Preconditions.expectNotNull(request, ErrorCodeEnum.INVALID_PARAMETER, "request cannot be null");
        Preconditions.expectNotNull(LoginUserUtils.getLoginUser(), ErrorCodeEnum.LOGIN_USER_EMPTY);
        return Response.success(streamTransformService.update(request, LoginUserUtils.getLoginUser().getName()));
    }

    @RequestMapping(value = "/transform/delete", method = RequestMethod.DELETE)
    @OperationLog(operation = OperationType.UPDATE, operationTarget = OperationTarget.STREAM)
    @ApiOperation(value = "Delete stream transform")
    public Response<Boolean> delete(@Validated DeleteTransformRequest request) {
        Preconditions.expectNotNull(request, ErrorCodeEnum.INVALID_PARAMETER, "request cannot be null");
        Preconditions.expectNotNull(LoginUserUtils.getLoginUser(), ErrorCodeEnum.LOGIN_USER_EMPTY);
        return Response.success(streamTransformService.delete(request, LoginUserUtils.getLoginUser().getName()));
    }

    @RequestMapping(value = "/transform/function/list", method = RequestMethod.POST)
    @ApiOperation(value = "Get transform function docs list with optional type filtering and pagination")
    public Response<PageResult<TransformFunctionDocResponse>> listDocs(
            @Validated @RequestBody TransformFunctionDocRequest request) {
        return Response.success(transformFunctionDocService.listByCondition(request));
    }

    @RequestMapping(value = "/transform/parseTransformSql", method = RequestMethod.POST)
    @ApiOperation(value = "Parse stream transform sql")
    public Response<String> parseTransformSql(@Validated @RequestBody TransformRequest request) {
        return Response.success(
                streamTransformService.parseTransformSql(request, LoginUserUtils.getLoginUser().getName()));
    }
}
