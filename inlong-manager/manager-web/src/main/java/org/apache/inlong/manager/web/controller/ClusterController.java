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
import org.apache.inlong.manager.common.pojo.cluster.ClusterInfo;
import org.apache.inlong.manager.common.pojo.cluster.ClusterRequest;
import org.apache.inlong.manager.common.pojo.cluster.DataProxyClusterInfo;
import org.apache.inlong.manager.common.pojo.cluster.DataProxyClusterPageRequest;
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
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

/**
 * Cluster controller
 */
@RestController
@RequestMapping("/cluster")
@Api(tags = "Cluster Config")
public class ClusterController {

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

    @Deprecated
    @PostMapping(value = "/thirdparty/save")
    @ApiOperation(value = "Add a cluster info")
    @OperationLog(operation = OperationType.CREATE)
    public Response<Integer> saveClusterV1(@RequestBody ClusterInfo clusterInfo) {
        String currentUser = LoginUserUtils.getLoginUserDetail().getUserName();
        return Response.success(thirdPartyClusterService.save(clusterInfo, currentUser));
    }

    @Deprecated
    @GetMapping(value = "/thirdparty/get/{id}")
    @ApiOperation(value = "Query third party cluster information of the common")
    @ApiImplicitParam(name = "id", value = "common cluster ID", dataTypeClass = Integer.class, required = true)
    public Response<ClusterInfo> getClusterV1(@PathVariable Integer id) {
        return Response.success(thirdPartyClusterService.get(id));
    }

    @Deprecated
    @PostMapping(value = "/thirdparty/list")
    @ApiOperation(value = "Query the list of general clusters based on conditions")
    public Response<List<ClusterInfo>> listV1(@RequestBody ClusterRequest request) {
        return Response.success(thirdPartyClusterService.list(request));
    }

    @Deprecated
    @RequestMapping(value = "/thirdparty/update", method = RequestMethod.POST)
    @OperationLog(operation = OperationType.UPDATE)
    @ApiOperation(value = "Modify third party cluster information of the common")
    public Response<Boolean> updateClusterV1(@RequestBody ClusterInfo clusterInfo) {
        String username = LoginUserUtils.getLoginUserDetail().getUserName();
        return Response.success(thirdPartyClusterService.update(clusterInfo, username));
    }

    @Deprecated
    @RequestMapping(value = "/thirdparty/delete/{id}", method = RequestMethod.DELETE)
    @ApiOperation(value = "Delete third party cluster information")
    @OperationLog(operation = OperationType.DELETE)
    @ApiImplicitParam(name = "id", value = "DataProxy cluster id", dataTypeClass = Integer.class, required = true)
    public Response<Boolean> deleteV1(@PathVariable Integer id) {
        return Response.success(thirdPartyClusterService.delete(id, LoginUserUtils.getLoginUserDetail().getUserName()));
    }

    @RequestMapping(value = "/dataproxy/save", method = RequestMethod.POST)
    @OperationLog(operation = OperationType.CREATE)
    @ApiOperation(value = "Save cluster information of the DataProxy")
    public Response<Integer> saveDataProxy(@RequestBody DataProxyClusterInfo clusterInfo) {
        String currentUser = LoginUserUtils.getLoginUserDetail().getUserName();
        return Response.success(dataProxyClusterService.save(clusterInfo, currentUser));
    }

    @RequestMapping(value = "/dataproxy/get/{id}", method = RequestMethod.GET)
    @ApiOperation(value = "Query cluster information of the DataProxy")
    @ApiImplicitParam(name = "id", value = "DataProxy cluster ID", dataTypeClass = Integer.class, required = true)
    public Response<DataProxyClusterInfo> getDataProxy(@PathVariable Integer id) {
        return Response.success(dataProxyClusterService.get(id));
    }

    @RequestMapping(value = "/dataproxy/list", method = RequestMethod.GET)
    @ApiOperation(value = "Query the list of DataProxy clusters based on conditions")
    public Response<PageInfo<DataProxyClusterInfo>> listDataProxyByCondition(DataProxyClusterPageRequest request) {
        request.setCurrentUser(LoginUserUtils.getLoginUserDetail().getUserName());
        return Response.success(dataProxyClusterService.listByCondition(request));
    }

    @RequestMapping(value = "/dataproxy/update", method = RequestMethod.POST)
    @OperationLog(operation = OperationType.UPDATE)
    @ApiOperation(value = "Modify cluster information of the DataProxy")
    public Response<Boolean> updateDataProxy(@RequestBody DataProxyClusterInfo clusterInfo) {
        String username = LoginUserUtils.getLoginUserDetail().getUserName();
        return Response.success(dataProxyClusterService.update(clusterInfo, username));
    }

    @RequestMapping(value = "/dataproxy/delete/{id}", method = RequestMethod.DELETE)
    @ApiOperation(value = "Delete cluster information of the dataproxy")
    @OperationLog(operation = OperationType.DELETE)
    @ApiImplicitParam(name = "id", value = "DataProxy cluster id", dataTypeClass = Integer.class, required = true)
    public Response<Boolean> deleteDataProxy(@PathVariable Integer id) {
        return Response.success(dataProxyClusterService.delete(id, LoginUserUtils.getLoginUserDetail().getUserName()));
    }

}
