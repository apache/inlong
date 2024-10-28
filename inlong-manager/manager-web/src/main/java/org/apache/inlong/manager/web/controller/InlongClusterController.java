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
import org.apache.inlong.manager.common.validation.SaveValidation;
import org.apache.inlong.manager.common.validation.UpdateByIdValidation;
import org.apache.inlong.manager.common.validation.UpdateByKeyValidation;
import org.apache.inlong.manager.common.validation.UpdateValidation;
import org.apache.inlong.manager.pojo.cluster.BindTagRequest;
import org.apache.inlong.manager.pojo.cluster.ClusterInfo;
import org.apache.inlong.manager.pojo.cluster.ClusterNodeRequest;
import org.apache.inlong.manager.pojo.cluster.ClusterNodeResponse;
import org.apache.inlong.manager.pojo.cluster.ClusterPageRequest;
import org.apache.inlong.manager.pojo.cluster.ClusterRequest;
import org.apache.inlong.manager.pojo.cluster.ClusterTagPageRequest;
import org.apache.inlong.manager.pojo.cluster.ClusterTagRequest;
import org.apache.inlong.manager.pojo.cluster.ClusterTagResponse;
import org.apache.inlong.manager.pojo.cluster.TenantClusterTagInfo;
import org.apache.inlong.manager.pojo.cluster.TenantClusterTagPageRequest;
import org.apache.inlong.manager.pojo.cluster.TenantClusterTagRequest;
import org.apache.inlong.manager.pojo.common.PageResult;
import org.apache.inlong.manager.pojo.common.Response;
import org.apache.inlong.manager.pojo.common.UpdateResult;
import org.apache.inlong.manager.pojo.user.LoginUserUtils;
import org.apache.inlong.manager.pojo.user.UserRoleCode;
import org.apache.inlong.manager.service.cluster.InlongClusterProcessService;
import org.apache.inlong.manager.service.cluster.InlongClusterService;
import org.apache.inlong.manager.service.operationlog.OperationLog;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiImplicitParam;
import io.swagger.annotations.ApiImplicitParams;
import io.swagger.annotations.ApiOperation;
import org.apache.shiro.authz.annotation.Logical;
import org.apache.shiro.authz.annotation.RequiresRoles;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

/**
 * Inlong cluster controller
 */
@RestController
@RequestMapping("/api")
@Api(tags = "Inlong-Cluster-API")
public class InlongClusterController {

    @Autowired
    private InlongClusterService clusterService;
    @Autowired
    private InlongClusterProcessService clusterProcessService;

    @PostMapping(value = "/cluster/tag/save")
    @ApiOperation(value = "Save cluster tag")
    @OperationLog(operation = OperationType.CREATE, operationTarget = OperationTarget.CLUSTER_TAG)
    @RequiresRoles(value = UserRoleCode.INLONG_ADMIN)
    public Response<Integer> saveTag(@Validated(SaveValidation.class) @RequestBody ClusterTagRequest request) {
        String currentUser = LoginUserUtils.getLoginUser().getName();
        return Response.success(clusterService.saveTag(request, currentUser));
    }

    @GetMapping(value = "/cluster/tag/get/{id}")
    @ApiOperation(value = "Get cluster tag by id")
    @ApiImplicitParam(name = "id", value = "Cluster ID", dataTypeClass = Integer.class, required = true)
    public Response<ClusterTagResponse> getTag(@PathVariable Integer id) {
        String currentUser = LoginUserUtils.getLoginUser().getName();
        return Response.success(clusterService.getTag(id, currentUser));
    }

    @PostMapping(value = "/cluster/tag/list")
    @ApiOperation(value = "List cluster tags")
    public Response<PageResult<ClusterTagResponse>> listTag(@RequestBody ClusterTagPageRequest request) {
        request.setCurrentUser(LoginUserUtils.getLoginUser().getName());
        request.setIsAdminRole(LoginUserUtils.isInlongAdminOrTenantAdmin());
        return Response.success(clusterService.listTag(request));
    }

    @PostMapping(value = "/cluster/tag/update")
    @OperationLog(operation = OperationType.UPDATE, operationTarget = OperationTarget.CLUSTER_TAG)
    @ApiOperation(value = "Update cluster tag")
    @RequiresRoles(value = UserRoleCode.INLONG_ADMIN)
    public Response<Boolean> updateTag(@Validated(UpdateValidation.class) @RequestBody ClusterTagRequest request) {
        String username = LoginUserUtils.getLoginUser().getName();
        return Response.success(clusterService.updateTag(request, username));
    }

    @DeleteMapping(value = "/cluster/tag/delete/{id}")
    @ApiOperation(value = "Delete cluster tag by id")
    @OperationLog(operation = OperationType.DELETE, operationTarget = OperationTarget.CLUSTER_TAG)
    @ApiImplicitParam(name = "id", value = "Cluster tag ID", dataTypeClass = Integer.class, required = true)
    @RequiresRoles(value = UserRoleCode.INLONG_ADMIN)
    public Response<Boolean> deleteTag(@PathVariable Integer id) {
        return Response.success(clusterService.deleteTag(id, LoginUserUtils.getLoginUser().getName()));
    }

    @PostMapping(value = "/cluster/tenant/tag/save")
    @ApiOperation(value = "Save tenant cluster tag")
    @OperationLog(operation = OperationType.CREATE, operationTarget = OperationTarget.CLUSTER_TAG)
    @RequiresRoles(value = UserRoleCode.INLONG_ADMIN)
    public Response<Integer> saveTenantTag(
            @Validated(SaveValidation.class) @RequestBody TenantClusterTagRequest request) {
        String currentUser = LoginUserUtils.getLoginUser().getName();
        return Response.success(clusterService.saveTenantTag(request, currentUser));
    }

    @PostMapping(value = "/cluster/tenant/tag/list")
    @ApiOperation(value = "List tenant cluster tags")
    public Response<PageResult<TenantClusterTagInfo>> listTenantTag(@RequestBody TenantClusterTagPageRequest request) {
        return Response.success(clusterService.listTenantTag(request));
    }

    @PostMapping(value = "/cluster/tag/listTagByTenantRole")
    @ApiOperation(value = "List cluster tags by tenant condition")
    public Response<PageResult<ClusterTagResponse>> listTagByTenantRole(
            @RequestBody TenantClusterTagPageRequest request) {
        return Response.success(clusterService.listTagByTenantRole(request));
    }

    @PostMapping(value = "/cluster/listByTenantRole")
    @ApiOperation(value = "List cluster by tenant condition")
    public Response<PageResult<ClusterInfo>> listByTenantRole(
            @RequestBody ClusterPageRequest request) {
        return Response.success(clusterService.listByTenantRole(request));
    }

    @DeleteMapping(value = "/cluster/tenant/tag/delete/{id}")
    @ApiOperation(value = "Delete tenant cluster tag by id")
    @OperationLog(operation = OperationType.DELETE, operationTarget = OperationTarget.CLUSTER_TAG)
    @ApiImplicitParam(name = "id", value = "Cluster tag ID", dataTypeClass = Integer.class, required = true)
    @RequiresRoles(value = UserRoleCode.INLONG_ADMIN)
    public Response<Boolean> deleteTenantTag(@PathVariable Integer id) {
        return Response.success(clusterService.deleteTenantTag(id, LoginUserUtils.getLoginUser().getName()));
    }

    @PostMapping(value = "/cluster/save")
    @ApiOperation(value = "Save cluster")
    @OperationLog(operation = OperationType.CREATE, operationTarget = OperationTarget.CLUSTER)
    @RequiresRoles(logical = Logical.OR, value = {UserRoleCode.INLONG_ADMIN, UserRoleCode.TENANT_ADMIN})
    public Response<Integer> save(@Validated(SaveValidation.class) @RequestBody ClusterRequest request) {
        String currentUser = LoginUserUtils.getLoginUser().getName();
        return Response.success(clusterService.save(request, currentUser));
    }

    @GetMapping(value = "/cluster/get/{id}")
    @ApiOperation(value = "Get cluster by id")
    @ApiImplicitParam(name = "id", value = "Cluster ID", dataTypeClass = Integer.class, required = true)
    public Response<ClusterInfo> get(@PathVariable Integer id) {
        String currentUser = LoginUserUtils.getLoginUser().getName();
        return Response.success(clusterService.get(id, currentUser));
    }

    @PostMapping(value = "/cluster/list")
    @ApiOperation(value = "List clusters")
    public Response<PageResult<ClusterInfo>> list(@RequestBody ClusterPageRequest request) {
        request.setCurrentUser(LoginUserUtils.getLoginUser().getName());
        request.setIsAdminRole(
                LoginUserUtils.isInlongAdminOrTenantAdmin());
        return Response.success(clusterService.list(request));
    }

    @PostMapping(value = "/cluster/update")
    @OperationLog(operation = OperationType.UPDATE, operationTarget = OperationTarget.CLUSTER)
    @ApiOperation(value = "Update cluster")
    @RequiresRoles(value = UserRoleCode.INLONG_ADMIN)
    public Response<Boolean> update(@Validated(UpdateByIdValidation.class) @RequestBody ClusterRequest request) {
        String username = LoginUserUtils.getLoginUser().getName();
        return Response.success(clusterService.update(request, username));
    }

    @PostMapping(value = "/cluster/updateByKey")
    @OperationLog(operation = OperationType.UPDATE, operationTarget = OperationTarget.CLUSTER)
    @ApiOperation(value = "Update cluster by key")
    @RequiresRoles(value = UserRoleCode.INLONG_ADMIN)
    public Response<UpdateResult> updateByKey(
            @Validated(UpdateByKeyValidation.class) @RequestBody ClusterRequest request) {
        String username = LoginUserUtils.getLoginUser().getName();
        return Response.success(clusterService.updateByKey(request, username));
    }

    @PostMapping(value = "/cluster/bindTag")
    @OperationLog(operation = OperationType.UPDATE, operationTarget = OperationTarget.CLUSTER)
    @ApiOperation(value = "Bind or unbind cluster tag")
    @RequiresRoles(value = UserRoleCode.INLONG_ADMIN)
    public Response<Boolean> bindTag(@Validated @RequestBody BindTagRequest request) {
        String username = LoginUserUtils.getLoginUser().getName();
        return Response.success(clusterService.bindTag(request, username));
    }

    @DeleteMapping(value = "/cluster/delete/{id}")
    @ApiOperation(value = "Delete cluster by id")
    @OperationLog(operation = OperationType.DELETE, operationTarget = OperationTarget.CLUSTER)
    @ApiImplicitParam(name = "id", value = "Cluster ID", dataTypeClass = Integer.class, required = true)
    @RequiresRoles(UserRoleCode.INLONG_ADMIN)
    public Response<Boolean> delete(@PathVariable Integer id) {
        return Response.success(clusterService.delete(id, LoginUserUtils.getLoginUser().getName()));
    }

    @DeleteMapping(value = "/cluster/deleteByKey")
    @ApiOperation(value = "Delete cluster by cluster name and type")
    @OperationLog(operation = OperationType.DELETE, operationTarget = OperationTarget.CLUSTER)
    @ApiImplicitParams({
            @ApiImplicitParam(name = "name", value = "Cluster name", dataTypeClass = String.class, required = true),
            @ApiImplicitParam(name = "type", value = "Cluster type", dataTypeClass = String.class, required = true),
    })
    @RequiresRoles(value = UserRoleCode.INLONG_ADMIN)
    public Response<Boolean> deleteByKey(@RequestParam String name, @RequestParam String type) {
        return Response.success(clusterService.deleteByKey(name, type,
                LoginUserUtils.getLoginUser().getName()));
    }

    @PostMapping(value = "/cluster/node/save")
    @ApiOperation(value = "Save cluster node")
    @OperationLog(operation = OperationType.CREATE, operationTarget = OperationTarget.CLUSTER_NODE)
    public Response<Integer> saveNode(@Validated @RequestBody ClusterNodeRequest request) {
        String currentUser = LoginUserUtils.getLoginUser().getName();
        request.setCurrentUser(currentUser);
        return Response.success(clusterService.saveNode(request, currentUser));
    }

    @GetMapping(value = "/cluster/node/get/{id}")
    @ApiOperation(value = "Get cluster node by id")
    @ApiImplicitParam(name = "id", value = "Cluster node ID", dataTypeClass = Integer.class, required = true)
    public Response<ClusterNodeResponse> getNode(@PathVariable Integer id) {
        String currentUser = LoginUserUtils.getLoginUser().getName();
        return Response.success(clusterService.getNode(id, currentUser));
    }

    @PostMapping(value = "/cluster/node/list")
    @ApiOperation(value = "List cluster nodes by pagination")
    public Response<PageResult<ClusterNodeResponse>> listNode(@RequestBody ClusterPageRequest request) {
        String currentUser = LoginUserUtils.getLoginUser().getName();
        return Response.success(clusterService.listNode(request, currentUser));
    }

    @GetMapping(value = "/cluster/node/listByGroupId")
    @ApiOperation(value = "List cluster nodes by groupId, clusterType and protocolType")
    @ApiImplicitParams({
            @ApiImplicitParam(name = "inlongGroupId", dataTypeClass = String.class, required = true),
            @ApiImplicitParam(name = "clusterType", dataTypeClass = String.class, required = true),
            @ApiImplicitParam(name = "protocolType", dataTypeClass = String.class, required = false)
    })
    @OperationLog(operation = OperationType.GET, operationTarget = OperationTarget.CLUSTER)
    public Response<List<ClusterNodeResponse>> listByGroupId(@RequestParam String inlongGroupId,
            @RequestParam String clusterType, @RequestParam(required = false) String protocolType) {
        return Response.success(clusterService.listNodeByGroupId(inlongGroupId, clusterType, protocolType));
    }

    @RequestMapping(value = "/cluster/node/update", method = RequestMethod.POST)
    @OperationLog(operation = OperationType.UPDATE, operationTarget = OperationTarget.CLUSTER_NODE)
    @ApiOperation(value = "Update cluster node")
    public Response<Boolean> updateNode(@Validated(UpdateValidation.class) @RequestBody ClusterNodeRequest request) {
        String username = LoginUserUtils.getLoginUser().getName();
        request.setCurrentUser(username);
        return Response.success(clusterService.updateNode(request, username));
    }

    @RequestMapping(value = "/cluster/node/delete/{id}", method = RequestMethod.DELETE)
    @ApiOperation(value = "Delete cluster node")
    @OperationLog(operation = OperationType.DELETE, operationTarget = OperationTarget.CLUSTER_NODE)
    @ApiImplicitParam(name = "id", value = "Cluster node ID", dataTypeClass = Integer.class, required = true)
    public Response<Boolean> deleteNode(@PathVariable Integer id) {
        return Response.success(clusterService.deleteNode(id, LoginUserUtils.getLoginUser().getName()));
    }

    @RequestMapping(value = "/cluster/node/unload/{id}", method = RequestMethod.DELETE)
    @ApiOperation(value = "Delete cluster node")
    @OperationLog(operation = OperationType.DELETE, operationTarget = OperationTarget.CLUSTER_NODE)
    @ApiImplicitParam(name = "id", value = "Cluster node ID", dataTypeClass = Integer.class, required = true)
    public Response<Boolean> unloadNode(@PathVariable Integer id) {
        return Response.success(clusterService.unloadNode(id, LoginUserUtils.getLoginUser().getName()));
    }

    @RequestMapping(value = "/cluster/node/getManagerSSHPublicKey", method = RequestMethod.GET)
    @ApiOperation(value = "Obtain the SSH public key from the manager to install the agent.")
    public Response<String> getManagerSSHPublicKey() {
        return Response.success(clusterService.getManagerSSHPublicKey());
    }

    @PostMapping("/cluster/node/testSSHConnection")
    @ApiOperation(value = "Test SSH connection for inlong cluster node")
    public Response<Boolean> testSSHConnection(@RequestBody ClusterNodeRequest request) {
        return Response.success(clusterService.testSSHConnection(request));
    }

    @PostMapping("/cluster/testConnection")
    @ApiOperation(value = "Test connection for inlong cluster")
    public Response<Boolean> testConnection(@Validated @RequestBody ClusterRequest request) {
        return Response.success(clusterService.testConnection(request));
    }

    @RequestMapping(value = "/cluster/startProcess/{clusterTag}", method = RequestMethod.POST)
    @ApiOperation(value = "Start inlong cluster process")
    @OperationLog(operation = OperationType.START, operationTarget = OperationTarget.CLUSTER)
    @ApiImplicitParam(name = "clusterTag", value = "Inlong cluster tag", dataTypeClass = String.class)
    public Response<Boolean> startProcess(@PathVariable String clusterTag,
            @RequestParam(required = false, defaultValue = "false") boolean sync) {
        String operator = LoginUserUtils.getLoginUser().getName();
        return Response.success(clusterProcessService.startProcess(clusterTag, operator, sync));
    }

}
