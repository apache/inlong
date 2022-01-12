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

import java.util.List;

import org.apache.inlong.manager.common.beans.Response;
import org.apache.inlong.manager.common.pojo.dataproxy.DataProxyIpRequest;
import org.apache.inlong.manager.common.pojo.dataproxy.DataProxyIpResponse;
import org.apache.inlong.manager.dao.entity.DataProxyConfig;
import org.apache.inlong.manager.service.core.DataProxyClusterService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@RestController
@RequestMapping("/openapi/dataproxy")
@Api(tags = "DataProxy Config")
public class DataProxyController {

    @Autowired
    private DataProxyClusterService dataProxyClusterService;

    @PostMapping("/getIpList")
    @ApiOperation(value = "get data proxy ip list")
    public Response<List<DataProxyIpResponse>> postIpList(@RequestBody DataProxyIpRequest request) {
        return Response.success(dataProxyClusterService.getIpList(request));
    }

    @GetMapping("/getIpList")
    @ApiOperation(value = "get data proxy ip list")
    public Response<List<DataProxyIpResponse>> getIpList(@RequestBody DataProxyIpRequest request) {
        return Response.success(dataProxyClusterService.getIpList(request));
    }

    @GetMapping("/getConfig")
    @ApiOperation(value = "get data proxy config list")
    public Response<List<DataProxyConfig>> getConfig() {
        return Response.success(dataProxyClusterService.getConfig());
    }

    @GetMapping("/getAllConfig")
    @ApiOperation(value = "get data proxy config")
    public String getAllConfig(@RequestParam("clusterName") String clusterName, @RequestParam("setName") String setName,
            @RequestParam("md5") String md5) {
        return dataProxyClusterService.getAllConfig(clusterName, setName, md5);
    }
}
