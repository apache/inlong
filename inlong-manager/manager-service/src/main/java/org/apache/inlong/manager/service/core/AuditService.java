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

package org.apache.inlong.manager.service.core;

import org.apache.inlong.audit.entity.AuditInformation;
import org.apache.inlong.audit.entity.AuditProxy;
import org.apache.inlong.common.enums.IndicatorType;
import org.apache.inlong.manager.pojo.audit.AuditAlertRule;
import org.apache.inlong.manager.pojo.audit.AuditAlertRuleRequest;
import org.apache.inlong.manager.pojo.audit.AuditAlertRuleUpdateRequest;
import org.apache.inlong.manager.pojo.audit.AuditRequest;
import org.apache.inlong.manager.pojo.audit.AuditVO;

import java.util.List;

/**
 * The service interface for audit.
 */
public interface AuditService {

    /**
     * Query audit data for list by condition
     *
     * @param request The audit request of query condition
     * @return The result of query
     */
    List<AuditVO> listByCondition(AuditRequest request) throws Exception;

    /**
     * Query audit data for list by condition
     *
     * @param request The audit request of query condition
     * @return The result of query
     */
    List<AuditVO> listAll(AuditRequest request) throws Exception;

    List<AuditInformation> getAuditBases(Boolean isMetric);

    /**
     * Get audit id by type and indicator type.
     *
     * @param type audit type.
     * @param indicatorType indicator type
     * @return Audit id.
     */
    String getAuditId(String type, IndicatorType indicatorType);

    /**
     * Refresh the base item of audit cache.
     *
     * @return true if not exception, or false if it has exception
     */
    Boolean refreshBaseItemCache();

    /**
     * Get audit proxy url by component
     *
     * @return audit proxy list
     */
    List<AuditProxy> getAuditProxy(String component) throws Exception;

    List<AuditInformation> getCdcAuditInfoList(String type, IndicatorType indicatorType);

    /**
     * Batch Query of Alarm Policies
     * @param inlongGroupId InLong group ID
     * @param inlongStreamId InLong group ID
     * @return Alarm policy list
     */
    List<AuditAlertRule> listRules(String inlongGroupId, String inlongStreamId);

    /**
     * Create an alarm policy
     */
    AuditAlertRule create(AuditAlertRule rule, String operator);

    /**
     * Create an alarm policy from request
     */
    Integer create(AuditAlertRuleRequest request, String operator);

    /**
     * Query a single alarm policy
     */
    AuditAlertRule get(Integer id);

    /**
     * Update the alarm policy
     */
    AuditAlertRule update(AuditAlertRule rule, String operator);

    /**
     * Update the alarm policy from request
     */
    AuditAlertRule update(AuditAlertRuleUpdateRequest request, String operator);

    /**
     * Delete the alarm policy
     */
    Boolean delete(Integer id);

    /**
     * Query all enabled alarm policies
     */
    List<AuditAlertRule> listEnabled();
}
