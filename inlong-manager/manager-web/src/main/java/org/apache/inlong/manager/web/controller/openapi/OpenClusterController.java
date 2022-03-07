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

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiImplicitParam;
import io.swagger.annotations.ApiOperation;
import org.apache.inlong.manager.common.beans.Response;
import org.apache.inlong.manager.common.enums.OperationType;
import org.apache.inlong.manager.common.pojo.cluster.ClusterInfo;
import org.apache.inlong.manager.common.pojo.cluster.ClusterRequest;
import org.apache.inlong.manager.common.util.LoginUserUtils;
import org.apache.inlong.manager.service.core.DataProxyClusterService;
import org.apache.inlong.manager.service.core.ThirdPartyClusterService;
import org.apache.inlong.manager.service.core.operationlog.OperationLog;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

/**
 * Cluster controller
 */
@RestController
@RequestMapping("/openapi/cluster")
@Api(tags = "Cluster Config")
public class OpenClusterController {

    @Autowired
    private ThirdPartyClusterService thirdPartyClusterService;
    @Autowired
    private DataProxyClusterService dataProxyClusterService;

    @PostMapping(value = "/save")
    @ApiOperation(value = "Save cluster info")
    @OperationLog(operation = OperationType.CREATE)
    public Response<Integer> save(@RequestBody ClusterInfo clusterInfo) {
        String currentUser = LoginUserUtils.getLoginUserDetail().getUserName();
        return Response.success(thirdPartyClusterService.save(clusterInfo, currentUser));
    }

    @GetMapping(value = "/get/{id}")
    @ApiOperation(value = "Get cluster info by id")
    @ApiImplicitParam(name = "id", value = "common cluster ID", dataTypeClass = Integer.class, required = true)
    public Response<ClusterInfo> get(@PathVariable Integer id) {
        return Response.success(thirdPartyClusterService.get(id));
    }

    @PostMapping(value = "/list")
    @ApiOperation(value = "List clusters by condition")
    public Response<List<ClusterInfo>> list(@RequestBody ClusterRequest request) {
        return Response.success(thirdPartyClusterService.list(request));
    }

    @PostMapping(value = "/update")
    @OperationLog(operation = OperationType.UPDATE)
    @ApiOperation(value = "Update cluster info")
    public Response<Boolean> update(@RequestBody ClusterInfo clusterInfo) {
        String username = LoginUserUtils.getLoginUserDetail().getUserName();
        return Response.success(thirdPartyClusterService.update(clusterInfo, username));
    }

    @DeleteMapping(value = "/delete/{id}")
    @ApiOperation(value = "Delete cluster info by id")
    @OperationLog(operation = OperationType.DELETE)
    @ApiImplicitParam(name = "id", value = "Cluster ID", dataTypeClass = Integer.class, required = true)
    public Response<Boolean> delete(@PathVariable Integer id) {
        return Response.success(thirdPartyClusterService.delete(id, LoginUserUtils.getLoginUserDetail().getUserName()));
    }

}
