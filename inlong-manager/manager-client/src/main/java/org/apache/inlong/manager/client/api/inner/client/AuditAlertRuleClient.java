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

package org.apache.inlong.manager.client.api.inner.client;

import org.apache.inlong.manager.client.api.ClientConfiguration;
import org.apache.inlong.manager.client.api.service.AuditAlertRuleApi;
import org.apache.inlong.manager.client.api.util.ClientUtils;
import org.apache.inlong.manager.common.enums.ErrorCodeEnum;
import org.apache.inlong.manager.common.util.Preconditions;
import org.apache.inlong.manager.pojo.audit.AuditAlertRule;
import org.apache.inlong.manager.pojo.audit.AuditAlertRulePageRequest;
import org.apache.inlong.manager.pojo.audit.AuditAlertRuleRequest;
import org.apache.inlong.manager.pojo.common.PageResult;
import org.apache.inlong.manager.pojo.common.Response;

/**
 * Client for {@link AuditAlertRuleApi}.
 */
public class AuditAlertRuleClient {

    private final AuditAlertRuleApi auditAlertRuleApi;

    public AuditAlertRuleClient(ClientConfiguration configuration) {
        auditAlertRuleApi = ClientUtils.createRetrofit(configuration).create(AuditAlertRuleApi.class);
    }

    /**
     * Create an audit alert rule
     *
     * @param request The audit alert rule to create
     * @return The created audit alert rule ID
     */
    public Integer create(AuditAlertRuleRequest request) {
        Preconditions.expectNotNull(request, "audit alert rule request cannot be null");
        Preconditions.expectNotBlank(request.getInlongGroupId(), ErrorCodeEnum.INVALID_PARAMETER,
                "inlong group id cannot be empty");
        Preconditions.expectNotBlank(request.getAuditId(), ErrorCodeEnum.INVALID_PARAMETER,
                "audit id cannot be empty");
        Preconditions.expectNotBlank(request.getAlertName(), ErrorCodeEnum.INVALID_PARAMETER,
                "alert name cannot be empty");
        Preconditions.expectNotNull(request.getCondition(), ErrorCodeEnum.INVALID_PARAMETER,
                "condition cannot be null");
        Response<Integer> response = ClientUtils.executeHttpCall(auditAlertRuleApi.create(request));
        ClientUtils.assertRespSuccess(response);
        return response.getData();
    }

    /**
     * Get an audit alert rule by ID
     *
     * @param id The rule ID
     * @return The audit alert rule
     */
    public AuditAlertRule get(Integer id) {
        Preconditions.expectNotNull(id, "rule id cannot be null");
        Response<AuditAlertRule> response = ClientUtils.executeHttpCall(auditAlertRuleApi.get(id));
        ClientUtils.assertRespSuccess(response);
        return response.getData();
    }

    /**
     * Select audit alert rules by condition with pagination
     *
     * @param request The condition to filter audit alert rules
     * @return Page result of audit alert rules
     */
    public PageResult<AuditAlertRule> listByCondition(AuditAlertRulePageRequest request) {
        Response<PageResult<AuditAlertRule>> response = ClientUtils.executeHttpCall(
                auditAlertRuleApi.listByCondition(request));
        ClientUtils.assertRespSuccess(response);
        return response.getData();
    }

    /**
     * Update an audit alert rule
     *
     * @param rule The audit alert rule to update
     * @return True if update is successful, false otherwise
     */
    public Boolean update(AuditAlertRuleRequest rule) {
        Preconditions.expectNotNull(rule, "audit alert rule cannot be null");
        Preconditions.expectNotNull(rule.getId(), "rule id cannot be null");
        Response<Boolean> response = ClientUtils.executeHttpCall(auditAlertRuleApi.update(rule));
        ClientUtils.assertRespSuccess(response);
        return response.getData();
    }

    /**
     * Delete an audit alert rule by ID
     *
     * @param id The rule ID
     * @return True if deletion is successful, false otherwise
     */
    public Boolean delete(Integer id) {
        Preconditions.expectNotNull(id, "rule id cannot be null");
        Response<Boolean> response = ClientUtils.executeHttpCall(auditAlertRuleApi.delete(id));
        ClientUtils.assertRespSuccess(response);
        return response.getData();
    }
}