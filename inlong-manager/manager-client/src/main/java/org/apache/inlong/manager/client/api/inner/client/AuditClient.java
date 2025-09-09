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
import org.apache.inlong.manager.client.api.service.AuditApi;
import org.apache.inlong.manager.client.api.util.ClientUtils;
import org.apache.inlong.manager.common.enums.ErrorCodeEnum;
import org.apache.inlong.manager.common.util.Preconditions;
import org.apache.inlong.manager.pojo.audit.AuditAlertRule;
import org.apache.inlong.manager.pojo.audit.AuditAlertRuleRequest;
import org.apache.inlong.manager.pojo.audit.AuditRequest;
import org.apache.inlong.manager.pojo.audit.AuditVO;
import org.apache.inlong.manager.pojo.common.Response;

import java.util.List;

/**
 * Client for {@link AuditApi}.
 */
public class AuditClient {

    private final AuditApi auditApi;

    public AuditClient(ClientConfiguration configuration) {
        auditApi = ClientUtils.createRetrofit(configuration).create(AuditApi.class);
    }

    /**
     * Query audit data for list by condition
     *
     * @param request The audit request of query condition
     * @return The result of query
     */
    public List<AuditVO> list(AuditRequest request) {
        Preconditions.expectNotNull(request, "request cannot be null");
        Preconditions.expectNotBlank(request.getInlongGroupId(), ErrorCodeEnum.INVALID_PARAMETER,
                "inlong group id cannot be empty");
        Preconditions.expectNotBlank(request.getInlongStreamId(), ErrorCodeEnum.INVALID_PARAMETER,
                "inlong stream id cannot be empty");
        Response<List<AuditVO>> response = ClientUtils.executeHttpCall(auditApi.list(request));
        ClientUtils.assertRespSuccess(response);
        return response.getData();
    }

    /**
     * Query audit data for list by condition
     *
     * @param request The audit request of query condition
     * @return The result of query
     */
    public List<AuditVO> listAll(AuditRequest request) {
        Preconditions.expectNotNull(request, "request cannot be null");
        Preconditions.expectNotBlank(request.getInlongGroupId(), ErrorCodeEnum.INVALID_PARAMETER,
                "inlong group id cannot be empty");
        Preconditions.expectNotBlank(request.getInlongStreamId(), ErrorCodeEnum.INVALID_PARAMETER,
                "inlong stream id cannot be empty");
        Response<List<AuditVO>> response = ClientUtils.executeHttpCall(auditApi.listAll(request));
        ClientUtils.assertRespSuccess(response);
        return response.getData();
    }

    /**
     * Refresh the base item of audit cache.
     *
     * @return true if not exception, or false if it has exception
     */
    public Boolean refreshCache(AuditRequest request) {
        Response<Boolean> response = ClientUtils.executeHttpCall(auditApi.refreshCache());
        ClientUtils.assertRespSuccess(response);
        return response.getData();
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
        Response<Integer> response = ClientUtils.executeHttpCall(auditApi.create(request));
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
        Response<AuditAlertRule> response = ClientUtils.executeHttpCall(auditApi.get(id));
        ClientUtils.assertRespSuccess(response);
        return response.getData();
    }

    /**
     * List all enabled audit alert rules
     *
     * @return List of enabled audit alert rules
     */
    public List<AuditAlertRule> listEnabled() {
        Response<List<AuditAlertRule>> response = ClientUtils.executeHttpCall(auditApi.listEnabled());
        ClientUtils.assertRespSuccess(response);
        return response.getData();
    }

    /**
     * List audit alert rules by conditions
     *
     * @param inlongGroupId The inlong group ID (optional)
     * @param inlongStreamId The inlong stream ID (optional)
     * @return List of audit alert rules
     */
    public List<AuditAlertRule> listRules(String inlongGroupId, String inlongStreamId) {
        Response<List<AuditAlertRule>> response = ClientUtils.executeHttpCall(
                auditApi.listRules(inlongGroupId, inlongStreamId));
        ClientUtils.assertRespSuccess(response);
        return response.getData();
    }


    /**
     * Update an audit alert rule
     *
     * @param rule The audit alert rule to update
     * @return The updated audit alert rule
     */
    public AuditAlertRule update(AuditAlertRule rule) {
        Preconditions.expectNotNull(rule, "audit alert rule cannot be null");
        Preconditions.expectNotNull(rule.getId(), "rule id cannot be null");
        Response<AuditAlertRule> response = ClientUtils.executeHttpCall(auditApi.update(rule));
        ClientUtils.assertRespSuccess(response);
        return response.getData();
    }

    /**
     * Delete an audit alert rule by ID
     *
     * @param id The rule ID
     * @return true if deletion was successful
     */
    public Boolean delete(Integer id) {
        Preconditions.expectNotNull(id, "rule id cannot be null");
        Response<Boolean> response = ClientUtils.executeHttpCall(auditApi.delete(id));
        ClientUtils.assertRespSuccess(response);
        return response.getData();
    }
}
