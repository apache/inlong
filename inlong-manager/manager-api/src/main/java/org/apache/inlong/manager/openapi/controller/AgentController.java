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

package org.apache.inlong.manager.openapi.controller;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import org.apache.inlong.manager.common.beans.Response;
import org.apache.inlong.manager.common.pojo.agent.AgentHeartbeatRequest;
import org.apache.inlong.manager.common.pojo.agent.AgentStatusReportRequest;
import org.apache.inlong.manager.common.pojo.agent.AgentSysConfig;
import org.apache.inlong.manager.common.pojo.agent.AgentSysconfRequest;
import org.apache.inlong.manager.common.pojo.agent.CheckAgentTaskConfRequest;
import org.apache.inlong.manager.common.pojo.agent.ConfirmAgentIpRequest;
import org.apache.inlong.manager.common.pojo.agent.FileAgentCommandInfo;
import org.apache.inlong.manager.common.pojo.agent.FileAgentTaskConfig;
import org.apache.inlong.manager.common.pojo.agent.FileAgentTaskInfo;
import org.apache.inlong.manager.service.core.AgentHeartBeatService;
import org.apache.inlong.manager.service.core.AgentSysConfigService;
import org.apache.inlong.manager.service.core.AgentTaskService;
import org.apache.inlong.manager.service.core.ClusterInfoService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
@RequestMapping("/agent")
@Api(tags = "Agent Config")
public class AgentController {

    @Autowired
    private ClusterInfoService clusterInfoService;

    @Autowired
    private AgentTaskService agentTaskService;

    @Autowired
    private AgentSysConfigService agentSysConfigService;

    @Autowired
    private AgentHeartBeatService agentHeartBeatService;

    @GetMapping("/getVirtualIp")
    @ApiOperation(value = "get inlong manager ip list")
    public Response<List<String>> getVirtualIp() {
        return Response.success(clusterInfoService.listClusterIpByType("inlong-openapi"));
    }

    @PostMapping("/file_agent/taskconf")
    @ApiOperation(value = "fetch file access task")
    public Response<FileAgentTaskInfo> getFileAgentTask(@RequestBody FileAgentCommandInfo info) {
        return Response.success(agentTaskService.getFileAgentTask(info));
    }

    @PostMapping("/file_agent/confirmAgentIp")
    @ApiOperation(value = "confirm current agent ip")
    public Response<String> confirmAgentIp(@RequestBody ConfirmAgentIpRequest info) {
        return Response.success(agentTaskService.confirmAgentIp(info));
    }

    @PostMapping("/file_agent/queryAgentSysconf")
    @ApiOperation(value = "query agent system config")
    public Response<AgentSysConfig> getAgentSysConf(@RequestBody AgentSysconfRequest info) {
        return Response.success(agentSysConfigService.getAgentSysConfig(info));
    }

    @PostMapping("/file_agent/heartbeat")
    @ApiOperation(value = "agent heartbeat report")
    public Response<String> heartbeat(@RequestBody AgentHeartbeatRequest info) {
        return Response.success(agentHeartBeatService.heartbeat(info));
    }

    @PostMapping("/file_agent/checkAgentTaskConf")
    @ApiOperation(value = "agent data source comparison")
    public Response<List<FileAgentTaskConfig>> checkAgentTaskConf(@RequestBody CheckAgentTaskConfRequest info) {
        return Response.success(agentTaskService.checkAgentTaskConf(info));
    }

    @PostMapping("/file_agent/reportAgentStatus")
    @ApiOperation(value = "agent status report")
    public Response<String> reportAgentStatus(@RequestBody AgentStatusReportRequest info) {
        return Response.success(agentTaskService.reportAgentStatus(info));
    }
}
