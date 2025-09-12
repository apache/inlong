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
import org.apache.inlong.manager.common.enums.NotifyType;
import org.apache.inlong.manager.common.exceptions.BusinessException;
import org.apache.inlong.manager.common.util.CommonBeanUtils;
import org.apache.inlong.manager.common.util.Preconditions;
import org.apache.inlong.manager.dao.entity.AuditAlertRuleEntity;
import org.apache.inlong.manager.dao.mapper.AuditAlertRuleEntityMapper;
import org.apache.inlong.manager.pojo.audit.AuditAlertCondition;
import org.apache.inlong.manager.pojo.audit.AuditAlertRule;
import org.apache.inlong.manager.pojo.audit.AuditAlertRulePageRequest;
import org.apache.inlong.manager.pojo.audit.AuditAlertRuleRequest;
import org.apache.inlong.manager.pojo.common.PageResult;
import org.apache.inlong.manager.service.core.AuditAlertRuleService;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.pagehelper.Page;
import com.github.pagehelper.PageHelper;
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
        AuditAlertRule rule = new AuditAlertRule();
        // Manually copy properties to avoid issues with CommonBeanUtils.copyProperties
        rule.setInlongGroupId(request.getInlongGroupId());
        rule.setInlongStreamId(request.getInlongStreamId());
        rule.setAuditId(request.getAuditId());
        rule.setAlertName(request.getAlertName());
        rule.setCondition(request.getCondition());
        rule.setLevel(request.getLevel());
        rule.setNotifyType(request.getNotifyType()); // Keep as enum type
        rule.setReceivers(request.getReceivers());
        rule.setEnabled(request.getEnabled());
        rule.setCreator(operator);
        rule.setModifier(operator);
        rule.setIsDeleted(0); // Initialize isDeleted
        rule.setVersion(1); // Initialize version

        AuditAlertRuleEntity entity = new AuditAlertRuleEntity();
        // Manually copy properties to avoid issues with CommonBeanUtils.copyProperties
        entity.setInlongGroupId(rule.getInlongGroupId());
        entity.setInlongStreamId(rule.getInlongStreamId());
        entity.setAuditId(rule.getAuditId());
        entity.setAlertName(rule.getAlertName());
        entity.setLevel(rule.getLevel());
        entity.setReceivers(rule.getReceivers());
        entity.setEnabled(rule.getEnabled());
        entity.setCreator(rule.getCreator());
        entity.setModifier(rule.getModifier());
        entity.setIsDeleted(rule.getIsDeleted());
        entity.setVersion(rule.getVersion());

        // Handle notifyType conversion from enum to string
        if (request.getNotifyType() != null) {
            entity.setNotifyType(request.getNotifyType().name());
        } else {
            entity.setNotifyType(null); // Explicitly set to null if request notifyType is null
        }

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
        AuditAlertRule createdRule = new AuditAlertRule();
        // Manually copy properties to avoid issues with CommonBeanUtils.copyProperties
        createdRule.setId(createdEntity.getId());
        createdRule.setInlongGroupId(createdEntity.getInlongGroupId());
        createdRule.setInlongStreamId(createdEntity.getInlongStreamId());
        createdRule.setAuditId(createdEntity.getAuditId());
        createdRule.setAlertName(createdEntity.getAlertName());
        createdRule.setLevel(createdEntity.getLevel());
        createdRule.setReceivers(createdEntity.getReceivers());
        createdRule.setEnabled(createdEntity.getEnabled());
        createdRule.setCreator(createdEntity.getCreator());
        createdRule.setModifier(createdEntity.getModifier());
        createdRule.setCreateTime(createdEntity.getCreateTime());
        createdRule.setModifyTime(createdEntity.getModifyTime());
        createdRule.setIsDeleted(createdEntity.getIsDeleted());
        createdRule.setVersion(createdEntity.getVersion());

        // Handle notifyType conversion from string to enum
        if (createdEntity.getNotifyType() != null) {
            try {
                createdRule.setNotifyType(NotifyType.valueOf(createdEntity.getNotifyType()));
            } catch (IllegalArgumentException e) {
                log.warn("Invalid notifyType value in database: {}", createdEntity.getNotifyType());
            }
        }

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

        // Check if exists
        AuditAlertRuleEntity existingEntity = alertRuleMapper.selectById(request.getId());
        if (existingEntity == null) {
            log.warn("Audit alert rule not found with id: {}", request.getId());
            throw new BusinessException(ErrorCodeEnum.RECORD_NOT_FOUND,
                    "Audit alert rule not found with id: " + request.getId());
        }

        log.debug("Existing entity version: {}, Incoming rule version: {}", existingEntity.getVersion(),
                request.getVersion());

        // Version check for optimistic locking
        if (request.getVersion() == null || !request.getVersion().equals(existingEntity.getVersion())) {
            log.warn(
                    "Audit alert rule config has been modified, please refresh and try again. Existing version: {}, Incoming version: {}",
                    existingEntity.getVersion(), request.getVersion());
            throw new BusinessException(ErrorCodeEnum.CONFIG_EXPIRED,
                    "Audit alert rule config has been modified, please refresh and try again. Existing version: "
                            + existingEntity.getVersion() + ", Incoming version: " + request.getVersion());
        }

        // Create rule and entity objects manually to avoid issues with CommonBeanUtils.copyProperties
        AuditAlertRule rule = new AuditAlertRule();
        rule.setId(request.getId());
        rule.setInlongGroupId(request.getInlongGroupId());
        rule.setInlongStreamId(request.getInlongStreamId());
        rule.setAuditId(request.getAuditId());
        rule.setAlertName(request.getAlertName());
        rule.setCondition(request.getCondition());
        rule.setLevel(request.getLevel());
        rule.setNotifyType(request.getNotifyType()); // Keep as enum type
        rule.setReceivers(request.getReceivers());
        rule.setEnabled(request.getEnabled());
        rule.setModifier(operator);
        rule.setVersion(request.getVersion());

        AuditAlertRuleEntity entity = new AuditAlertRuleEntity();
        entity.setId(rule.getId());
        entity.setInlongGroupId(rule.getInlongGroupId());
        entity.setInlongStreamId(rule.getInlongStreamId());
        entity.setAuditId(rule.getAuditId());
        entity.setAlertName(rule.getAlertName());
        entity.setLevel(rule.getLevel());
        entity.setReceivers(rule.getReceivers());
        entity.setEnabled(rule.getEnabled());
        entity.setModifier(rule.getModifier());
        entity.setVersion(rule.getVersion());

        // Handle notifyType conversion from enum to string
        if (request.getNotifyType() != null) {
            entity.setNotifyType(request.getNotifyType().name());
        } else {
            entity.setNotifyType(existingEntity.getNotifyType()); // Keep existing value if request notifyType is null
        }

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
        } else {
            // Keep existing condition if not provided in request
            entity.setCondition(existingEntity.getCondition());
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
        AuditAlertRule resultRule = new AuditAlertRule();
        // Manually copy properties to avoid issues with CommonBeanUtils.copyProperties
        resultRule.setId(updatedEntity.getId());
        resultRule.setInlongGroupId(updatedEntity.getInlongGroupId());
        resultRule.setInlongStreamId(updatedEntity.getInlongStreamId());
        resultRule.setAuditId(updatedEntity.getAuditId());
        resultRule.setAlertName(updatedEntity.getAlertName());
        resultRule.setLevel(updatedEntity.getLevel());
        resultRule.setReceivers(updatedEntity.getReceivers());
        resultRule.setEnabled(updatedEntity.getEnabled());
        resultRule.setCreator(updatedEntity.getCreator());
        resultRule.setModifier(updatedEntity.getModifier());
        resultRule.setCreateTime(updatedEntity.getCreateTime());
        resultRule.setModifyTime(updatedEntity.getModifyTime());
        resultRule.setIsDeleted(updatedEntity.getIsDeleted());
        resultRule.setVersion(updatedEntity.getVersion());

        // Handle notifyType conversion from string to enum
        if (updatedEntity.getNotifyType() != null) {
            try {
                resultRule.setNotifyType(NotifyType.valueOf(updatedEntity.getNotifyType()));
            } catch (IllegalArgumentException e) {
                log.warn("Invalid notifyType value in database: {}", updatedEntity.getNotifyType());
            }
        }

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
    public PageResult<AuditAlertRule> selectByCondition(AuditAlertRulePageRequest request) {
        log.info("begin to select audit alert rules by condition with pagination, request={}", request);

        // Start pagination
        PageHelper.startPage(request.getPageNum(), request.getPageSize(),
                String.format("%s %s", request.getOrderField(), request.getOrderType()));

        // Convert request to entity for database query
        AuditAlertRuleEntity entity = CommonBeanUtils.copyProperties(request, AuditAlertRuleEntity::new);

        // Get from database
        Page<AuditAlertRuleEntity> entityPage = (Page<AuditAlertRuleEntity>) alertRuleMapper.selectByCondition(entity);

        // Convert to rules and filter out soft-deleted records
        List<AuditAlertRule> rules = entityPage.stream()
                .filter(e -> e.getIsDeleted() == null || e.getIsDeleted() == 0)
                .map(e -> {
                    AuditAlertRule rule = CommonBeanUtils.copyProperties(e, AuditAlertRule::new);
                    // Convert condition JSON string to Condition object
                    if (StringUtils.isNotBlank(e.getCondition())) {
                        try {
                            AuditAlertCondition condition =
                                    objectMapper.readValue(e.getCondition(), AuditAlertCondition.class);
                            rule.setCondition(condition);
                        } catch (JsonProcessingException ex) {
                            log.error("Failed to parse condition JSON: {}", e.getCondition(), ex);
                            // Set a default condition to avoid breaking the list
                            rule.setCondition(new AuditAlertCondition());
                        }
                    }

                    // Handle notifyType conversion from string to enum
                    if (StringUtils.isNotBlank(e.getNotifyType())) {
                        try {
                            rule.setNotifyType(NotifyType.valueOf(e.getNotifyType()));
                        } catch (IllegalArgumentException ex) {
                            log.warn("Invalid notifyType value in database: {}", e.getNotifyType());
                        }
                    }

                    return rule;
                })
                .collect(Collectors.toList());

        // Create page result
        PageResult<AuditAlertRule> pageResult = new PageResult<>(rules, entityPage.getTotal(),
                entityPage.getPageNum(), entityPage.getPageSize());

        log.info("success to select audit alert rules by condition with pagination, count={}",
                pageResult.getList().size());
        return pageResult;
    }
}