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

package org.apache.inlong.manager.service.core.impl;

import org.apache.inlong.manager.common.enums.ErrorCodeEnum;
import org.apache.inlong.manager.common.exceptions.BusinessException;
import org.apache.inlong.manager.common.util.CommonBeanUtils;
import org.apache.inlong.manager.common.util.Preconditions;
import org.apache.inlong.manager.dao.entity.AuditAlertRuleEntity;
import org.apache.inlong.manager.dao.mapper.AuditAlertRuleEntityMapper;
import org.apache.inlong.manager.pojo.audit.AuditAlertCondition;
import org.apache.inlong.manager.pojo.audit.AuditAlertRule;
import org.apache.inlong.manager.pojo.audit.AuditAlertRuleRequest;
import org.apache.inlong.manager.service.core.AuditAlertRuleService;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Implementation of audit alert rule service.
 */
@Slf4j
@Service
public class AuditAlertRuleServiceImpl implements AuditAlertRuleService {

    private static final ObjectMapper objectMapper = new ObjectMapper();

    @Autowired
    private AuditAlertRuleEntityMapper alertRuleMapper;

    @Override
    public Integer create(AuditAlertRuleRequest request, String operator) {
        log.info("begin to create audit alert rule from request, request={}, operator={}", request, operator);

        // Validate input
        // Note: Validation is partially handled by javax.validation annotations in AuditAlertRuleRequest
        Preconditions.expectNotNull(request, ErrorCodeEnum.INVALID_PARAMETER, "request cannot be null");

        // Convert request to rule
        AuditAlertRule rule = CommonBeanUtils.copyProperties(request, AuditAlertRule::new);
        rule.setCreator(operator);
        rule.setModifier(operator);
        AuditAlertRuleEntity entity = CommonBeanUtils.copyProperties(rule, AuditAlertRuleEntity::new);
        entity.setCreator(operator);
        entity.setModifier(operator);

        // Convert Condition object to JSON string for database storage
        try {
            String conditionJson = objectMapper.writeValueAsString(rule.getCondition());
            entity.setCondition(conditionJson);
        } catch (JsonProcessingException e) {
            log.error("Failed to serialize condition to JSON: {}", rule.getCondition(), e);
            throw new BusinessException(ErrorCodeEnum.INVALID_PARAMETER, "Invalid condition format");
        }

        // Insert into database
        int result = alertRuleMapper.insert(entity);
        if (result <= 0) {
            log.error("Failed to create audit alert rule, rule={}", rule);
            throw new BusinessException(ErrorCodeEnum.GROUP_SAVE_FAILED, "Failed to create audit alert rule");
        }

        // Get the created entity from database to ensure all fields are populated
        AuditAlertRuleEntity createdEntity = alertRuleMapper.selectById(entity.getId());
        AuditAlertRule createdRule = CommonBeanUtils.copyProperties(createdEntity, AuditAlertRule::new);

        // Convert condition JSON string back to Condition object
        if (StringUtils.isNotBlank(createdEntity.getCondition())) {
            try {
                AuditAlertCondition condition =
                        objectMapper.readValue(createdEntity.getCondition(), AuditAlertCondition.class);
                createdRule.setCondition(condition);
            } catch (JsonProcessingException e) {
                log.error("Failed to parse condition JSON: {}", createdEntity.getCondition(), e);
                throw new BusinessException(ErrorCodeEnum.INVALID_PARAMETER, "Invalid condition format");
            }
        }

        log.info("success to create audit alert rule, rule={}, operator={}", rule, operator);

        log.info("success to create audit alert rule from request, request={}, operator={}", request, operator);
        return createdRule.getId();
    }

    @Override
    public AuditAlertRule get(Integer id) {
        log.info("begin to get audit alert rule by id={}", id);

        // Validate input
        Preconditions.expectNotNull(id, ErrorCodeEnum.INVALID_PARAMETER, "id cannot be null");

        // Get from database
        AuditAlertRuleEntity entity = alertRuleMapper.selectById(id);
        if (entity == null) {
            log.warn("Audit alert rule not found with id: {}", id);
            throw new BusinessException(ErrorCodeEnum.RECORD_NOT_FOUND, "Audit alert rule not found with id: " + id);
        }

        // Check if the rule is soft-deleted
        if (entity.getIsDeleted() != null && entity.getIsDeleted() != 0) {
            log.warn("Audit alert rule has been deleted with id: {}", id);
            throw new BusinessException(ErrorCodeEnum.RECORD_NOT_FOUND, "Audit alert rule not found with id: " + id);
        }

        // Convert to rule
        AuditAlertRule rule = CommonBeanUtils.copyProperties(entity, AuditAlertRule::new);
        // Convert condition JSON string to Condition object
        if (StringUtils.isNotBlank(entity.getCondition())) {
            try {
                AuditAlertCondition condition =
                        objectMapper.readValue(entity.getCondition(), AuditAlertCondition.class);
                rule.setCondition(condition);
            } catch (JsonProcessingException e) {
                log.error("Failed to parse condition JSON: {}", entity.getCondition(), e);
                throw new BusinessException(ErrorCodeEnum.INVALID_PARAMETER, "Invalid condition format");
            }
        }

        log.info("success to get audit alert rule by id={}", id);
        return rule;
    }

    @Override
    public AuditAlertRule update(AuditAlertRuleRequest request, String operator) {
        log.info("begin to update audit alert rule from request, request={}, operator={}", request, operator);

        // Validate input
        Preconditions.expectNotNull(request, ErrorCodeEnum.INVALID_PARAMETER, "request cannot be null");
        Preconditions.expectNotNull(request.getId(), ErrorCodeEnum.INVALID_PARAMETER, "rule id cannot be null");

        // Convert request to rule
        AuditAlertRule rule = CommonBeanUtils.copyProperties(request, AuditAlertRule::new);
        rule.setModifier(operator);

        // Check if exists
        AuditAlertRuleEntity existingEntity = alertRuleMapper.selectById(rule.getId());
        if (existingEntity == null) {
            log.warn("Audit alert rule not found with id: {}", rule.getId());
            throw new BusinessException(ErrorCodeEnum.RECORD_NOT_FOUND,
                    "Audit alert rule not found with id: " + rule.getId());
        }

        log.debug("Existing entity version: {}, Incoming rule version: {}", existingEntity.getVersion(),
                rule.getVersion());

        // Version check for optimistic locking
        if (rule.getVersion() == null || !rule.getVersion().equals(existingEntity.getVersion())) {
            log.warn(
                    "Audit alert rule config has been modified, please refresh and try again. Existing version: {}, Incoming version: {}",
                    existingEntity.getVersion(), rule.getVersion());
            throw new BusinessException(ErrorCodeEnum.CONFIG_EXPIRED,
                    "Audit alert rule config has been modified, please refresh and try again. Existing version: "
                            + existingEntity.getVersion() + ", Incoming version: " + rule.getVersion());
        }

        // Convert to entity and set modifier
        AuditAlertRuleEntity entity = CommonBeanUtils.copyProperties(rule, AuditAlertRuleEntity::new);
        entity.setModifier(operator);
        // Version will be automatically incremented by the mapper

        log.debug("Updating entity with current version: {}", existingEntity.getVersion());

        // Convert Condition object to JSON string for database storage
        if (rule.getCondition() != null) {
            try {
                String conditionJson = objectMapper.writeValueAsString(rule.getCondition());
                entity.setCondition(conditionJson);
            } catch (JsonProcessingException e) {
                log.error("Failed to serialize condition to JSON: {}", rule.getCondition(), e);
                throw new BusinessException(ErrorCodeEnum.INVALID_PARAMETER, "Invalid condition format");
            }
        }

        // Update in database
        int result = alertRuleMapper.updateById(entity);
        log.debug("Update result: {} rows affected", result);
        if (result <= 0) {
            log.error("Failed to update audit alert rule, rule={}", rule);
            throw new BusinessException(ErrorCodeEnum.GROUP_SAVE_FAILED, "Failed to update audit alert rule");
        }

        // Return updated entity
        AuditAlertRuleEntity updatedEntity = alertRuleMapper.selectById(rule.getId());
        AuditAlertRule resultRule = CommonBeanUtils.copyProperties(updatedEntity, AuditAlertRule::new);
        // Convert condition JSON string to Condition object
        if (StringUtils.isNotBlank(updatedEntity.getCondition())) {
            try {
                AuditAlertCondition condition =
                        objectMapper.readValue(updatedEntity.getCondition(), AuditAlertCondition.class);
                resultRule.setCondition(condition);
            } catch (JsonProcessingException e) {
                log.error("Failed to parse condition JSON: {}", updatedEntity.getCondition(), e);
                throw new BusinessException(ErrorCodeEnum.INVALID_PARAMETER, "Invalid condition format");
            }
        }
        log.info("success to update audit alert rule from request, request={}, operator={}", request, operator);
        return resultRule;
    }

    @Override
    public Boolean delete(Integer id) {
        log.info("begin to delete audit alert rule by id={}", id);

        // Validate input
        Preconditions.expectNotNull(id, ErrorCodeEnum.INVALID_PARAMETER, "id cannot be null");

        // Check if exists
        AuditAlertRuleEntity existingEntity = alertRuleMapper.selectById(id);
        if (existingEntity == null) {
            log.warn("Audit alert rule not found with id: {}", id);
            throw new BusinessException(ErrorCodeEnum.RECORD_NOT_FOUND, "Audit alert rule not found with id: " + id);
        }

        // Soft delete - set is_deleted to the record id instead of physical deletion
        existingEntity.setIsDeleted(id);
        // Version will be automatically incremented by the mapper
        int result = alertRuleMapper.updateById(existingEntity);
        if (result <= 0) {
            log.error("Failed to delete audit alert rule by id={}", id);
            throw new BusinessException(ErrorCodeEnum.GROUP_DELETE_NOT_ALLOWED, "Failed to delete audit alert rule");
        }

        log.info("success to delete audit alert rule by id={}", id);
        return true;
    }

    @Override
    public List<AuditAlertRule> listRules(String inlongGroupId, String inlongStreamId) {
        log.info("begin to list audit alert rules, inlongGroupId={}, inlongStreamId={}", inlongGroupId, inlongStreamId);

        // Get from database
        List<AuditAlertRuleEntity> entities = alertRuleMapper.selectByGroupAndStream(inlongGroupId, inlongStreamId);

        // Convert to rules and filter out soft-deleted records
        List<AuditAlertRule> rules = entities.stream()
                .filter(entity -> entity.getIsDeleted() == null || entity.getIsDeleted() == 0)
                .map(entity -> {
                    AuditAlertRule rule = CommonBeanUtils.copyProperties(entity, AuditAlertRule::new);
                    // Convert condition JSON string to Condition object
                    if (StringUtils.isNotBlank(entity.getCondition())) {
                        try {
                            AuditAlertCondition condition =
                                    objectMapper.readValue(entity.getCondition(), AuditAlertCondition.class);
                            rule.setCondition(condition);
                        } catch (JsonProcessingException e) {
                            log.error("Failed to parse condition JSON: {}", entity.getCondition(), e);
                            // Set a default condition to avoid breaking the list
                            rule.setCondition(new AuditAlertCondition());
                        }
                    }
                    return rule;
                })
                .collect(Collectors.toList());

        log.info("success to list audit alert rules, count={}", rules.size());
        return rules;
    }

    @Override
    public List<AuditAlertRule> listEnabled() {
        log.info("begin to list enabled audit alert rules");

        // Get from database
        List<AuditAlertRuleEntity> entities = alertRuleMapper.selectEnabledRules();

        // Convert to rules
        List<AuditAlertRule> rules = entities.stream()
                .map(entity -> {
                    AuditAlertRule rule = CommonBeanUtils.copyProperties(entity, AuditAlertRule::new);
                    // Convert condition JSON string to Condition object
                    if (StringUtils.isNotBlank(entity.getCondition())) {
                        try {
                            AuditAlertCondition condition =
                                    objectMapper.readValue(entity.getCondition(), AuditAlertCondition.class);
                            rule.setCondition(condition);
                        } catch (JsonProcessingException e) {
                            log.error("Failed to parse condition JSON: {}", entity.getCondition(), e);
                            // Set a default condition to avoid breaking the list
                            rule.setCondition(new AuditAlertCondition());
                        }
                    }
                    return rule;
                })
                .collect(Collectors.toList());

        log.info("success to list enabled audit alert rules, count={}", rules.size());
        return rules;
    }
}
