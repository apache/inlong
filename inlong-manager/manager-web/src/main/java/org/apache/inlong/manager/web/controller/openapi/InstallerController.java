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

import org.apache.inlong.common.pojo.agent.installer.ConfigRequest;
import org.apache.inlong.common.pojo.agent.installer.ConfigResult;
import org.apache.inlong.manager.pojo.common.Response;
import org.apache.inlong.manager.service.core.AgentService;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * Installer controller.
 */
@RestController
@RequestMapping("/openapi")
@Api(tags = "Open-Installer-API")
public class InstallerController {

    @Autowired
    private AgentService agentService;

    @PostMapping("/installer/getConfig")
    @ApiOperation(value = "Get config for installer")
    public Response<ConfigResult> getConfig(@RequestBody ConfigRequest request) {
        return Response.success(agentService.getConfig(request));
    }
}
