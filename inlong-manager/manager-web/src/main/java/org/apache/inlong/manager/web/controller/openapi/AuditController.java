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

import org.apache.inlong.audit.entity.AuditProxy;
import org.apache.inlong.common.pojo.audit.AuditConfig;
import org.apache.inlong.common.pojo.audit.AuditConfigRequest;
import org.apache.inlong.manager.pojo.audit.AuditRequest;
import org.apache.inlong.manager.pojo.audit.AuditVO;
import org.apache.inlong.manager.pojo.common.Response;
import org.apache.inlong.manager.service.cluster.InlongClusterService;
import org.apache.inlong.manager.service.core.AuditService;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiImplicitParam;
import io.swagger.annotations.ApiOperation;
import org.apache.commons.collections.CollectionUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import javax.validation.Valid;

import java.util.List;

/**
 * Audit controller.
 */
@RestController("OpenAuditController")
@RequestMapping("/openapi")
@Api(tags = "Open-Audit-API")
public class AuditController {

    @Autowired
    private InlongClusterService clusterService;
    @Autowired
    private AuditService auditService;

    @PostMapping("/audit/getConfig")
    @ApiOperation(value = "Get mq config list")
    public Response<AuditConfig> getConfig(@RequestBody AuditConfigRequest request) {
        AuditConfig auditConfig = clusterService.getAuditConfig(request.getClusterTag());
        if (CollectionUtils.isEmpty(auditConfig.getMqInfoList())) {
            return Response.fail("Failed to get MQ config of cluster tag: " + request.getClusterTag());
        }
        return Response.success(auditConfig);
    }

    @PostMapping(value = "/audit/list")
    @ApiOperation(value = "Query audit list according to conditions")
    public Response<List<AuditVO>> listByCondition(@Valid @RequestBody AuditRequest request) throws Exception {
        return Response.success(auditService.listByCondition(request));
    }

    @PostMapping(value = "/audit/listAll")
    @ApiOperation(value = "Query all audit list according to conditions")
    public Response<List<AuditVO>> listAll(@Valid @RequestBody AuditRequest request) throws Exception {
        return Response.success(auditService.listAll(request));
    }

    @GetMapping(value = "/audit/getAuditProxy")
    @ApiOperation(value = "Get audit proxy url")
    @ApiImplicitParam(name = "component", dataTypeClass = String.class, required = true)
    public Response<List<AuditProxy>> getAuditProxy(@RequestParam String component) throws Exception {
        return Response.success(auditService.getAuditProxy(component));
    }
}
