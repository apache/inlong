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
import org.apache.inlong.manager.common.util.LoginUserUtil;
import org.apache.inlong.manager.service.core.ClusterInfoService;
import org.apache.inlong.manager.service.core.DataProxyClusterService;
import org.apache.inlong.manager.service.core.operationlog.OperationLog;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

/**
 * Various cluster control layers
 */
@RestController
@RequestMapping("/cluster")
@Api(tags = "Cluster Config")
public class ClusterController {

    @Autowired
    private ClusterInfoService clusterInfoService;
    @Autowired
    private DataProxyClusterService dataProxyClusterService;

    @RequestMapping(value = "/common/list", method = RequestMethod.GET)
    @ApiOperation(value = "Query the list of general clusters based on conditions")
    public Response<List<ClusterInfo>> list(ClusterRequest request) {
        return Response.success(clusterInfoService.list(request));
    }

    @RequestMapping(value = "/common/save", method = RequestMethod.GET)
    @ApiOperation(value = "Add a cluster info")
    @OperationLog(operation = OperationType.CREATE)
    public Response<Integer> saveCluster(@RequestBody ClusterInfo clusterInfo) {
        String currentUser = LoginUserUtil.getLoginUserDetail().getUserName();
        return Response.success(clusterInfoService.save(clusterInfo, currentUser));
    }

    @RequestMapping(value = "/common/get/{id}")
    @ApiOperation(value = "Query cluster information of the common")
    @ApiImplicitParam(name = "id", value = "common cluster ID", dataTypeClass = Integer.class, required = true)
    public Response<ClusterInfo> getCluster(@PathVariable Integer id) {
        return Response.success(clusterInfoService.get(id));
    }

    @RequestMapping(value = "/common/update", method = RequestMethod.POST)
    @OperationLog(operation = OperationType.UPDATE)
    @ApiOperation(value = "Modify cluster information of the common")
    public Response<Boolean> updateCluster(@RequestBody ClusterInfo clusterInfo) {
        String username = LoginUserUtil.getLoginUserDetail().getUserName();
        return Response.success(clusterInfoService.update(clusterInfo, username));
    }

    @RequestMapping(value = "/common/delete/{id}", method = RequestMethod.DELETE)
    @ApiOperation(value = "Delete cluster information")
    @OperationLog(operation = OperationType.DELETE)
    @ApiImplicitParam(name = "id", value = "DataProxy cluster id", dataTypeClass = Integer.class, required = true)
    public Response<Boolean> delete(@PathVariable Integer id) {
        return Response.success(clusterInfoService.delete(id, LoginUserUtil.getLoginUserDetail().getUserName()));
    }

    @RequestMapping(value = "/dataproxy/save", method = RequestMethod.POST)
    @OperationLog(operation = OperationType.CREATE)
    @ApiOperation(value = "Save cluster information of the DataProxy")
    public Response<Integer> saveDataProxy(@RequestBody DataProxyClusterInfo clusterInfo) {
        String currentUser = LoginUserUtil.getLoginUserDetail().getUserName();
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
        request.setCurrentUser(LoginUserUtil.getLoginUserDetail().getUserName());
        return Response.success(dataProxyClusterService.listByCondition(request));
    }

    @RequestMapping(value = "/dataproxy/update", method = RequestMethod.POST)
    @OperationLog(operation = OperationType.UPDATE)
    @ApiOperation(value = "Modify cluster information of the DataProxy")
    public Response<Boolean> updateDataProxy(@RequestBody DataProxyClusterInfo clusterInfo) {
        String username = LoginUserUtil.getLoginUserDetail().getUserName();
        return Response.success(dataProxyClusterService.update(clusterInfo, username));
    }

    @RequestMapping(value = "/dataproxy/delete/{id}", method = RequestMethod.DELETE)
    @ApiOperation(value = "Delete cluster information of the dataproxy")
    @OperationLog(operation = OperationType.DELETE)
    @ApiImplicitParam(name = "id", value = "DataProxy cluster id", dataTypeClass = Integer.class, required = true)
    public Response<Boolean> deleteDataProxy(@PathVariable Integer id) {
        return Response.success(dataProxyClusterService.delete(id, LoginUserUtil.getLoginUserDetail().getUserName()));
    }

}
