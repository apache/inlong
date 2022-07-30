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
import org.apache.inlong.manager.common.beans.Response;
import org.apache.inlong.manager.common.enums.OperationType;
import org.apache.inlong.manager.common.pojo.common.UpdateValidation;
import org.apache.inlong.manager.common.pojo.node.DataNodePageRequest;
import org.apache.inlong.manager.common.pojo.node.DataNodeRequest;
import org.apache.inlong.manager.common.pojo.node.DataNodeResponse;
import org.apache.inlong.manager.common.pojo.user.UserRoleCode;
import org.apache.inlong.manager.common.util.LoginUserUtils;
import org.apache.inlong.manager.service.core.DataNodeService;
import org.apache.inlong.manager.service.core.operationlog.OperationLog;
import org.apache.shiro.authz.annotation.RequiresRoles;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * Data node controller
 */
@RestController
@RequestMapping("/api")
@Api(tags = "Data-Node-API")
public class DataNodeController {

    @Autowired
    private DataNodeService dataNodeService;

    @PostMapping(value = "/node/save")
    @ApiOperation(value = "Save node")
    @OperationLog(operation = OperationType.CREATE)
    @RequiresRoles(value = UserRoleCode.ADMIN)
    public Response<Integer> save(@Validated @RequestBody DataNodeRequest request) {
        String currentUser = LoginUserUtils.getLoginUser().getName();
        return Response.success(dataNodeService.save(request, currentUser));
    }

    @GetMapping(value = "/node/get/{id}")
    @ApiOperation(value = "Get node by id")
    @ApiImplicitParam(name = "id", value = "Data node ID", dataTypeClass = Integer.class, required = true)
    public Response<DataNodeResponse> get(@PathVariable Integer id) {
        return Response.success(dataNodeService.get(id));
    }

    @PostMapping(value = "/node/list")
    @ApiOperation(value = "List data node")
    public Response<PageInfo<DataNodeResponse>> list(@RequestBody DataNodePageRequest request) {
        return Response.success(dataNodeService.list(request));
    }

    @PostMapping(value = "/node/update")
    @OperationLog(operation = OperationType.UPDATE)
    @ApiOperation(value = "Update data node")
    public Response<Boolean> update(@Validated(UpdateValidation.class) @RequestBody DataNodeRequest request) {
        String username = LoginUserUtils.getLoginUser().getName();
        return Response.success(dataNodeService.update(request, username));
    }

    @DeleteMapping(value = "/node/delete/{id}")
    @ApiOperation(value = "Delete data node by id")
    @OperationLog(operation = OperationType.DELETE)
    @ApiImplicitParam(name = "id", value = "Data node ID", dataTypeClass = Integer.class, required = true)
    @RequiresRoles(value = UserRoleCode.ADMIN)
    public Response<Boolean> delete(@PathVariable Integer id) {
        return Response.success(dataNodeService.delete(id, LoginUserUtils.getLoginUser().getName()));
    }

    @PostMapping("/node/testConnection")
    @ApiOperation(value = "Test connection for data node")
    public Response<Boolean> testConnection(@Validated @RequestBody DataNodeRequest request) {
        return Response.success(dataNodeService.testConnection(request));
    }

}
