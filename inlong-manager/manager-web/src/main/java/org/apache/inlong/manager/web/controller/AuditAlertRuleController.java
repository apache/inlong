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

import org.apache.inlong.manager.pojo.audit.AuditAlertRule;
import org.apache.inlong.manager.pojo.audit.AuditAlertRuleRequest;
import org.apache.inlong.manager.pojo.audit.AuditAlertRuleUpdateRequest;
import org.apache.inlong.manager.pojo.common.Response;
import org.apache.inlong.manager.pojo.user.LoginUserUtils;
import org.apache.inlong.manager.service.core.AuditService;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Lazy;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import javax.validation.Valid;

import java.util.List;

/**
 * Audit alert rule controller.
 */
@RestController
@RequestMapping("/api")
@Api(tags = "Audit-Alert-Rule-API")
public class AuditAlertRuleController {

    @Lazy
    @Autowired
    private AuditService auditService;

    @PostMapping(value = "/audit/alert/rule")
    @ApiOperation(value = "Create an Audit alarm policy")
    public Response<Integer> create(@Valid @RequestBody AuditAlertRuleRequest request) {
        String operator = LoginUserUtils.getLoginUser().getName();
        Integer ruleId = auditService.create(request, operator);
        return Response.success(ruleId);
    }

    @GetMapping(value = "/audit/alert/rule/{id}")
    @ApiOperation(value = "Query the details of the alarm policy")
    public Response<AuditAlertRule> get(@PathVariable Integer id) {
        return Response.success(auditService.get(id));
    }

    @GetMapping(value = "/audit/alert/rule/enabled")
    @ApiOperation(value = "Query all enabled alarm policies")
    public Response<List<AuditAlertRule>> listEnabled() {
        return Response.success(auditService.listEnabled());
    }

    @GetMapping(value = "/audit/alert/rule/list")
    @ApiOperation(value = "Batch query alarm policies")
    public Response<List<AuditAlertRule>> listRules(
            @RequestParam(required = false) String inlongGroupId,
            @RequestParam(required = false) String inlongStreamId) {
        return Response.success(auditService.listRules(inlongGroupId, inlongStreamId));
    }

    @PutMapping(value = "/audit/alert/rule")
    @ApiOperation(value = "Update the Audit alarm policy")
    public Response<AuditAlertRule> update(@Valid @RequestBody AuditAlertRuleUpdateRequest request) {
        String operator = LoginUserUtils.getLoginUser().getName();
        AuditAlertRule rule = auditService.update(request, operator);
        return Response.success(rule);
    }

    @DeleteMapping(value = "/audit/alert/rule/{id}")
    @ApiOperation(value = "Delete the Audit alarm policy")
    public Response<Boolean> delete(@PathVariable Integer id) {
        return Response.success(auditService.delete(id));
    }
}
