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
import io.swagger.annotations.ApiOperation;
import org.apache.inlong.common.pojo.agent.TaskRequest;
import org.apache.inlong.common.pojo.agent.TaskResult;
import org.apache.inlong.common.pojo.agent.TaskSnapshotRequest;
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
import org.apache.inlong.manager.service.core.AgentHeartbeatService;
import org.apache.inlong.manager.service.core.AgentService;
import org.apache.inlong.manager.service.core.AgentSysConfigService;
import org.apache.inlong.manager.service.core.ThirdPartyClusterService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
@RequestMapping("/openapi/agent")
@Api(tags = "Agent Config")
public class AgentController {

    @Autowired
    private AgentService agentService;
    @Autowired
    private AgentSysConfigService agentSysConfigService;
    @Autowired
    private AgentHeartbeatService heartbeatService;
    @Autowired
    private ThirdPartyClusterService thirdPartyClusterService;

    @PostMapping("/getManagerIpList")
    @ApiOperation(value = "Get inlong manager ip list")
    public Response<List<String>> getInLongManagerIp() {
        return Response.success(thirdPartyClusterService.listClusterIpByType("inlong-openapi"));
    }

    @PostMapping("/reportSnapshot")
    @ApiOperation(value = "Report source task snapshot")
    public Response<Boolean> reportSnapshot(@RequestBody TaskSnapshotRequest request) {
        return Response.success(agentService.reportSnapshot(request));
    }

    @PostMapping("/reportAndGetTask")
    @ApiOperation(value = "Report source task snapshot")
    public Response<TaskResult> reportAndGetTask(@RequestBody TaskRequest request) {
        return Response.success(agentService.reportAndGetTask(request));
    }

    @Deprecated
    @PostMapping("/fileAgent/getTaskConf")
    @ApiOperation(value = "Get file task")
    public Response<FileAgentTaskInfo> getFileAgentTask(@RequestBody FileAgentCommandInfo info) {
        return Response.success(agentService.getFileAgentTask(info));
    }

    @PostMapping("/fileAgent/confirmAgentIp")
    @ApiOperation(value = "Confirm current agent ip")
    public Response<String> confirmAgentIp(@RequestBody ConfirmAgentIpRequest info) {
        return Response.success(agentService.confirmAgentIp(info));
    }

    @PostMapping("/fileAgent/getAgentSysConf")
    @ApiOperation(value = "Get agent system config")
    public Response<AgentSysConfig> getAgentSysConf(@RequestBody AgentSysconfRequest info) {
        return Response.success(agentSysConfigService.getAgentSysConfig(info));
    }

    @PostMapping("/fileAgent/heartbeat")
    @ApiOperation(value = "Report agent heartbeat")
    public Response<String> heartbeat(@RequestBody AgentHeartbeatRequest info) {
        return Response.success(heartbeatService.heartbeat(info));
    }

    @PostMapping("/fileAgent/checkAgentTaskConf")
    @ApiOperation(value = "Check agent source config")
    public Response<List<FileAgentTaskConfig>> checkAgentTaskConf(@RequestBody CheckAgentTaskConfRequest info) {
        return Response.success(agentService.checkAgentTaskConf(info));
    }

    @PostMapping("/fileAgent/reportAgentStatus")
    @ApiOperation(value = "Report agent status")
    public Response<String> reportAgentStatus(@RequestBody AgentStatusReportRequest info) {
        return Response.success(agentService.reportAgentStatus(info));
    }

}
