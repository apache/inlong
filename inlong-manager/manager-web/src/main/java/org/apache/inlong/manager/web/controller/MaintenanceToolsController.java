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

import org.apache.inlong.manager.pojo.common.Response;
import org.apache.inlong.manager.pojo.consume.SortConsumerInfo;
import org.apache.inlong.manager.pojo.user.UserRoleCode;
import org.apache.inlong.manager.service.maintenanceTools.MaintenanceToolsService;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiImplicitParam;
import io.swagger.annotations.ApiImplicitParams;
import io.swagger.annotations.ApiOperation;
import org.apache.shiro.authz.annotation.RequiresRoles;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.multipart.MultipartFile;

import java.util.List;

/**
 * Maintenance tools controller.
 */
@RestController
@RequestMapping("/api")
@Api(tags = "Maintenanct tools-API")
public class MaintenanceToolsController {

    @Autowired
    private MaintenanceToolsService maintenanceToolsService;

    @PostMapping("/maintenanceTools/getSortConsumer")
    @ApiOperation(value = "get sort consumer info")
    @ApiImplicitParams({
            @ApiImplicitParam(name = "file", value = "file object", required = true, dataType = "__FILE", dataTypeClass = MultipartFile.class, paramType = "query")
    })
    @RequiresRoles(value = UserRoleCode.INLONG_ADMIN)
    public Response<List<SortConsumerInfo>> getSortConsumer(@RequestParam(value = "file") MultipartFile file) {
        return Response.success(maintenanceToolsService.getSortConsumer(file));
    }

    @PostMapping("/maintenanceTools/resetCursor")
    @ApiOperation(value = "reset cursor consumer")
    @ApiImplicitParams({
            @ApiImplicitParam(name = "file", value = "file object", required = true, dataType = "__FILE", dataTypeClass = MultipartFile.class, paramType = "query"),
            @ApiImplicitParam(name = "resetTime", dataTypeClass = String.class, required = true)
    })
    @RequiresRoles(value = UserRoleCode.INLONG_ADMIN)
    public Response<Boolean> resetCursor(@RequestParam(value = "file") MultipartFile file,
            @RequestParam(value = "resetTime") String resetTime) {
        return Response.success(maintenanceToolsService.resetCursor(file, resetTime));
    }

}
