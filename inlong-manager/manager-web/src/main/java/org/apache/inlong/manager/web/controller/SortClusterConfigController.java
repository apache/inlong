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

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import org.apache.inlong.manager.common.beans.Response;
import org.apache.inlong.manager.common.pojo.sort.SortClusterConfigResponse;
import org.apache.inlong.manager.service.core.SortClusterConfigService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/openapi/sort/standalone")
@Api(tags = "Sort Cluster Config")
public class SortClusterConfigController {

    @Autowired
    private SortClusterConfigService clusterConfigService;

    @GetMapping("/getClusterConfig")
    @ApiOperation("Get sort stand-alone cluster config")
    public Response<SortClusterConfigResponse> getClusterConfig(
            @RequestParam("clusterName") String clusterName,
            @RequestParam("md5") String md5,
            @RequestParam("apiVersion") String apiVersion) {
        return Response.success(clusterConfigService.get(clusterName, md5));
    }

    @GetMapping("/hello")
    @ApiOperation("test")
    public Response<String> getHello() {
        return Response.success("hello");
    }

}
