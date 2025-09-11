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

import org.apache.inlong.manager.pojo.audit.AuditAlertRule;
import org.apache.inlong.manager.pojo.audit.AuditAlertRuleRequest;

import java.util.List;

/**
 * The service interface for audit alert rule.
 */
public interface AuditAlertRuleService {

    /**
     * Create an audit alert rule from request
     */
    Integer create(AuditAlertRuleRequest request, String operator);

    /**
     * Get audit alert rule by id
     */
    AuditAlertRule get(Integer id);

    /**
     * Update the audit alert rule from request
     */
    AuditAlertRule update(AuditAlertRuleRequest request, String operator);

    /**
     * Delete the audit alert rule 
     */
    Boolean delete(Integer id);

    /**
     * Select audit alert rules by condition
     */
    List<AuditAlertRule> selectByCondition(AuditAlertRuleRequest request);
}
