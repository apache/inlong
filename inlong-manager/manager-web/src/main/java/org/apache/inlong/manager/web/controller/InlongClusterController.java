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
import org.apache.inlong.manager.common.pojo.cluster.ClusterNodeRequest;
import org.apache.inlong.manager.common.pojo.cluster.ClusterNodeResponse;
import org.apache.inlong.manager.common.pojo.cluster.ClusterTagPageRequest;
import org.apache.inlong.manager.common.pojo.cluster.ClusterTagRequest;
import org.apache.inlong.manager.common.pojo.cluster.ClusterTagResponse;
import org.apache.inlong.manager.common.pojo.cluster.InlongClusterInfo;
import org.apache.inlong.manager.common.pojo.cluster.InlongClusterPageRequest;
import org.apache.inlong.manager.common.pojo.cluster.InlongClusterRequest;
import org.apache.inlong.manager.common.pojo.user.UserRoleCode;
import org.apache.inlong.manager.common.util.LoginUserUtils;
import org.apache.inlong.manager.service.cluster.InlongClusterService;
import org.apache.inlong.manager.service.core.operationlog.OperationLog;
import org.apache.shiro.authz.annotation.RequiresRoles;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

/**
 * Inlong cluster controller
 */
@RestController
@Api(tags = "Inlong-Cluster-API")
public class InlongClusterController {

    @Autowired
    private InlongClusterService clusterService;

    @PostMapping(value = "/cluster/tag/save")
    @ApiOperation(value = "Save cluster tag")
    @OperationLog(operation = OperationType.CREATE)
    @RequiresRoles(value = UserRoleCode.ADMIN)
    public Response<Integer> saveTag(@RequestBody ClusterTagRequest request) {
        String currentUser = LoginUserUtils.getLoginUserDetail().getUsername();
        return Response.success(clusterService.saveTag(request, currentUser));
    }

    @GetMapping(value = "/cluster/tag/get/{id}")
    @ApiOperation(value = "Get cluster tag by id")
    @ApiImplicitParam(name = "id", value = "Cluster ID", dataTypeClass = Integer.class, required = true)
    public Response<ClusterTagResponse> getTag(@PathVariable Integer id) {
        return Response.success(clusterService.getTag(id));
    }

    @PostMapping(value = "/cluster/tag/list")
    @ApiOperation(value = "List cluster tags")
    public Response<PageInfo<ClusterTagResponse>> listTag(@RequestBody ClusterTagPageRequest request) {
        request.setCurrentUser(LoginUserUtils.getLoginUserDetail().getUsername());
        return Response.success(clusterService.listTag(request));
    }

    @PostMapping(value = "/cluster/tag/update")
    @OperationLog(operation = OperationType.UPDATE)
    @ApiOperation(value = "Update cluster tag")
    public Response<Boolean> updateTag(@RequestBody ClusterTagRequest request) {
        String username = LoginUserUtils.getLoginUserDetail().getUsername();
        return Response.success(clusterService.updateTag(request, username));
    }

    @DeleteMapping(value = "/cluster/tag/delete/{id}")
    @ApiOperation(value = "Delete cluster tag by id")
    @OperationLog(operation = OperationType.DELETE)
    @ApiImplicitParam(name = "id", value = "Cluster ID", dataTypeClass = Integer.class, required = true)
    @RequiresRoles(value = UserRoleCode.ADMIN)
    public Response<Boolean> deleteTag(@PathVariable Integer id) {
        return Response.success(clusterService.deleteTag(id, LoginUserUtils.getLoginUserDetail().getUsername()));
    }

    @PostMapping(value = "/cluster/save")
    @ApiOperation(value = "Save cluster")
    @OperationLog(operation = OperationType.CREATE)
    @RequiresRoles(value = UserRoleCode.ADMIN)
    public Response<Integer> save(@RequestBody InlongClusterRequest request) {
        String currentUser = LoginUserUtils.getLoginUserDetail().getUsername();
        return Response.success(clusterService.save(request, currentUser));
    }

    @GetMapping(value = "/cluster/get/{id}")
    @ApiOperation(value = "Get cluster by id")
    @ApiImplicitParam(name = "id", value = "Cluster ID", dataTypeClass = Integer.class, required = true)
    public Response<InlongClusterInfo> get(@PathVariable Integer id) {
        return Response.success(clusterService.get(id));
    }

    @PostMapping(value = "/cluster/list")
    @ApiOperation(value = "List clusters")
    public Response<PageInfo<InlongClusterInfo>> list(@RequestBody InlongClusterPageRequest request) {
        return Response.success(clusterService.list(request));
    }

    @PostMapping(value = "/cluster/update")
    @OperationLog(operation = OperationType.UPDATE)
    @ApiOperation(value = "Update cluster")
    public Response<Boolean> update(@RequestBody InlongClusterRequest request) {
        String username = LoginUserUtils.getLoginUserDetail().getUsername();
        return Response.success(clusterService.update(request, username));
    }

    @DeleteMapping(value = "/cluster/delete/{id}")
    @ApiOperation(value = "Delete cluster by id")
    @OperationLog(operation = OperationType.DELETE)
    @ApiImplicitParam(name = "id", value = "Cluster ID", dataTypeClass = Integer.class, required = true)
    @RequiresRoles(value = UserRoleCode.ADMIN)
    public Response<Boolean> delete(@PathVariable Integer id) {
        return Response.success(clusterService.delete(id, LoginUserUtils.getLoginUserDetail().getUsername()));
    }

    @PostMapping(value = "/cluster/node/save")
    @ApiOperation(value = "Save cluster node")
    @OperationLog(operation = OperationType.CREATE)
    public Response<Integer> saveNode(@RequestBody ClusterNodeRequest request) {
        String currentUser = LoginUserUtils.getLoginUserDetail().getUsername();
        return Response.success(clusterService.saveNode(request, currentUser));
    }

    @GetMapping(value = "/cluster/node/get/{id}")
    @ApiOperation(value = "Get cluster node by id")
    @ApiImplicitParam(name = "id", value = "Cluster node ID", dataTypeClass = Integer.class, required = true)
    public Response<ClusterNodeResponse> getNode(@PathVariable Integer id) {
        return Response.success(clusterService.getNode(id));
    }

    @PostMapping(value = "/cluster/node/list")
    @ApiOperation(value = "List cluster nodes")
    public Response<PageInfo<ClusterNodeResponse>> listNode(@RequestBody InlongClusterPageRequest request) {
        return Response.success(clusterService.listNode(request));
    }

    @RequestMapping(value = "/cluster/node/update", method = RequestMethod.POST)
    @OperationLog(operation = OperationType.UPDATE)
    @ApiOperation(value = "Update cluster node")
    public Response<Boolean> updateNode(@RequestBody ClusterNodeRequest request) {
        String username = LoginUserUtils.getLoginUserDetail().getUsername();
        return Response.success(clusterService.updateNode(request, username));
    }

    @RequestMapping(value = "/cluster/node/delete/{id}", method = RequestMethod.DELETE)
    @ApiOperation(value = "Delete cluster node")
    @OperationLog(operation = OperationType.DELETE)
    @ApiImplicitParam(name = "id", value = "Cluster node id", dataTypeClass = Integer.class, required = true)
    public Response<Boolean> deleteNode(@PathVariable Integer id) {
        return Response.success(clusterService.deleteNode(id, LoginUserUtils.getLoginUserDetail().getUsername()));
    }

}
