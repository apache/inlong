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

import org.apache.inlong.audit.entity.AuditInformation;
import org.apache.inlong.audit.entity.AuditProxy;
import org.apache.inlong.manager.pojo.audit.AuditRequest;
import org.apache.inlong.manager.pojo.audit.AuditVO;
import org.apache.inlong.manager.pojo.common.Response;
import org.apache.inlong.manager.service.core.AuditService;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiImplicitParam;
import io.swagger.annotations.ApiOperation;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Lazy;
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
@RestController
@RequestMapping("/api")
@Api(tags = "Audit-API")
public class AuditController {

    @Lazy
    @Autowired
    private AuditService auditService;

    @PostMapping(value = "/audit/list")
    @ApiOperation(value = "Query audit list according to conditions")
    public Response<List<AuditVO>> listByCondition(@Valid @RequestBody AuditRequest request) throws Exception {
        return Response.success(auditService.listByCondition(request));
    }

    @PostMapping(value = "/audit/listAll")
    @ApiOperation(value = "Query audit list all according to conditions")
    public Response<List<AuditVO>> listAll(@Valid @RequestBody AuditRequest request) throws Exception {
        return Response.success(auditService.listAll(request));
    }

    @ApiOperation(value = "Refresh audit base item cache")
    @PostMapping("/audit/refreshCache")
    public Response<Boolean> refreshCache() {
        return Response.success(auditService.refreshBaseItemCache());
    }

    @ApiOperation(value = "Get the audit base info")
    @GetMapping("/audit/getAuditBases")
    public Response<List<AuditInformation>> getAuditBases(
            @RequestParam(required = false, defaultValue = "false") boolean isMetric) {
        return Response.success(auditService.getAuditBases(isMetric));
    }

    @GetMapping(value = "/audit/getAuditProxy")
    @ApiOperation(value = "Get audit proxy url")
    @ApiImplicitParam(name = "component", dataTypeClass = String.class, required = true)
    public Response<List<AuditProxy>> getAuditProxy(@RequestParam String component) throws Exception {
        return Response.success(auditService.getAuditProxy(component));
    }

    @PostMapping(value = "/audit/alert/rule")
    @ApiOperation(value = "Create an Audit alarm policy")
    public Response<AuditAlertRule> createAlertRule(@Valid @RequestBody AuditAlertRule rule) {
        String operator = LoginUserUtils.getLoginUser().getUsername();
        return Response.success(auditService.createAlertRule(rule, operator));
    }

    @GetMapping(value = "/audit/alert/rule/{id}")
    @ApiOperation(value = "Query the details of the alarm policy")
    public Response<AuditAlertRule> getAlertRule(@PathVariable Integer id) {
        return Response.success(auditService.getAlertRule(id));
    }

    @GetMapping(value = "/audit/alert/rule/enabled")
    @ApiOperation(value = "Query all enabled alarm policies")
    public Response<List<AuditAlertRule>> listEnabledAlertRules() {
        List<AuditAlertRule> allRules = auditService.listAlertRules(null, null);
        List<AuditAlertRule> enabledRules = allRules.stream()
                .filter(AuditAlertRule::getEnabled) 
                .collect(Collectors.toList());
        return Response.success(enabledRules);
    }

    @GetMapping(value = "/audit/alert/rule/list")
    @ApiOperation(value = "Batch query alarm policies")
    public Response<List<AuditAlertRule>> listAlertRules(
            @RequestParam(required = false) String inlongGroupId,
            @RequestParam(required = false) String inlongStreamId) {
        return Response.success(auditService.listAlertRules(inlongGroupId, inlongStreamId));
    }

    @PutMapping(value = "/audit/alert/rule")
    @ApiOperation(value = "Update the Audit alarm policy")
    public Response<AuditAlertRule> updateAlertRule(@Valid @RequestBody AuditAlertRule rule) {
        String operator = LoginUserUtils.getLoginUser().getUsername();
        return Response.success(auditService.updateAlertRule(rule, operator));
    }

    @DeleteMapping(value = "/audit/alert/rule/{id}")
    @ApiOperation(value = "Delete the Audit alarm policy")
    public Response<Boolean> deleteAlertRule(@PathVariable Integer id) {
        return Response.success(auditService.deleteAlertRule(id));
    }


}
