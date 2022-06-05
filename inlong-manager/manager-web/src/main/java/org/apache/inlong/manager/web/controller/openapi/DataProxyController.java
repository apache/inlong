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
import io.swagger.annotations.ApiImplicitParams;
import io.swagger.annotations.ApiOperation;
import org.apache.inlong.common.pojo.dataproxy.DataProxyConfig;
import org.apache.inlong.common.pojo.dataproxy.ThirdPartyClusterDTO;
import org.apache.inlong.manager.common.beans.Response;
import org.apache.inlong.manager.common.pojo.dataproxy.DataProxyResponse;
import org.apache.inlong.manager.service.core.InlongClusterService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

/**
 * Data proxy controller.
 */
@RestController
@RequestMapping("/openapi")
@Api(tags = "Open-DataProxy-Config")
public class DataProxyController {

    @Autowired
    private InlongClusterService clusterService;

    /**
     * Support GET and POST methods,
     * POST is used for DataProxy requests,
     * GET is used for quick lookup of IP lists (e.g. via browser requests).
     */
    @RequestMapping(value = "/dataproxy/getIpList", method = {RequestMethod.GET, RequestMethod.POST})
    @ApiOperation(value = "Get data proxy ip list by cluster name")
    public Response<List<DataProxyResponse>> getIpList(@RequestParam(required = false) String clusterName) {
        return Response.success(clusterService.getIpList(clusterName));
    }

    @GetMapping("/dataproxy/getConfig")
    @ApiOperation(value = "Get data proxy topic list")
    public Response<List<DataProxyConfig>> getConfig() {
        return Response.success(clusterService.getConfig());
    }

    @GetMapping("/dataproxy/getConfig_v2")
    @ApiOperation(value = "Get data proxy list - including topic")
    public Response<ThirdPartyClusterDTO> getConfigV2(@RequestParam("clusterName") String clusterName) {
        ThirdPartyClusterDTO dto = clusterService.getConfigV2(clusterName);
        if (dto.getMqSet().isEmpty() || dto.getTopicList().isEmpty()) {
            return Response.fail("failed to get mq config or topics");
        }
        return Response.success(dto);
    }

    @GetMapping("/dataproxy/getAllConfig")
    @ApiOperation(value = "Get all proxy config")
    @ApiImplicitParams({
            @ApiImplicitParam(name = "clusterName", dataTypeClass = String.class, required = true),
            @ApiImplicitParam(name = "md5", dataTypeClass = String.class, required = true)
    })
    public String getAllConfig(@RequestParam String clusterName, @RequestParam(required = false) String md5) {
        return clusterService.getAllConfig(clusterName, md5);
    }

}
