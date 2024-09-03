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

package org.apache.inlong.manager.service.stream;

import org.apache.inlong.manager.common.consts.InlongConstants;
import org.apache.inlong.manager.common.enums.ErrorCodeEnum;
import org.apache.inlong.manager.common.enums.TemplateVisibleRange;
import org.apache.inlong.manager.common.exceptions.BusinessException;
import org.apache.inlong.manager.common.util.CommonBeanUtils;
import org.apache.inlong.manager.common.util.Preconditions;
import org.apache.inlong.manager.dao.entity.TemplateEntity;
import org.apache.inlong.manager.dao.entity.TemplateFieldEntity;
import org.apache.inlong.manager.dao.entity.TenantTemplateEntity;
import org.apache.inlong.manager.dao.mapper.TemplateEntityMapper;
import org.apache.inlong.manager.dao.mapper.TemplateFieldEntityMapper;
import org.apache.inlong.manager.dao.mapper.TenantTemplateEntityMapper;
import org.apache.inlong.manager.pojo.common.PageResult;
import org.apache.inlong.manager.pojo.stream.TemplateField;
import org.apache.inlong.manager.pojo.stream.TemplateInfo;
import org.apache.inlong.manager.pojo.stream.TemplatePageRequest;
import org.apache.inlong.manager.pojo.stream.TemplateRequest;
import org.apache.inlong.manager.pojo.stream.TenantTemplateRequest;
import org.apache.inlong.manager.pojo.user.LoginUserUtils;

import com.github.pagehelper.Page;
import com.github.pagehelper.PageHelper;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Inlong template service layer implementation
 */
@Service
public class TemplateServiceImpl implements TemplateService {

    private static final Logger LOGGER = LoggerFactory.getLogger(TemplateServiceImpl.class);

    @Autowired
    private TemplateEntityMapper templateEntityMapper;
    @Autowired
    private TemplateFieldEntityMapper templateFieldEntityMapper;
    @Autowired
    private TenantTemplateEntityMapper tenantTemplateEntityMapper;

    @Transactional(rollbackFor = Throwable.class)
    @Override
    public Integer save(TemplateRequest request, String operator) {
        LOGGER.debug("begin to save template info={}", request);
        Preconditions.expectNotNull(request, "inlong template info is empty");
        String templateName = request.getName();
        TemplateEntity existEntity = templateEntityMapper.selectByName(templateName);
        if (existEntity != null) {
            LOGGER.error("inlong template name [{}] has already exists", templateName);
            throw new BusinessException(ErrorCodeEnum.TEMPLATE_NAME_DUPLICATE);
        }
        TemplateEntity templateEntity = CommonBeanUtils.copyProperties(request, TemplateEntity::new);
        templateEntity.setCreator(operator);
        templateEntity.setModifier(operator);

        templateEntityMapper.insert(templateEntity);
        request.setId(templateEntity.getId());
        saveField(request);

        if (CollectionUtils.isNotEmpty(request.getTenantList())) {
            TenantTemplateRequest tagRequest = new TenantTemplateRequest();
            tagRequest.setTemplateName(templateName);
            request.getTenantList().forEach(tenant -> {
                tagRequest.setTenant(tenant);
                this.saveTenantTemplate(tagRequest, operator);
            });
        }
        LOGGER.info("success to save inlong stream info for template name={}", templateName);
        return templateEntity.getId();
    }

    @Override
    public Boolean exist(String templateName) {
        Preconditions.expectNotBlank(templateName, ErrorCodeEnum.TEMPLATE_INFO_INCORRECT);
        TemplateEntity templateEntity = templateEntityMapper.selectByName(templateName);
        return templateEntity != null;
    }

    @Override
    public TemplateInfo get(String templateName, String operator) {
        LOGGER.debug("begin to get inlong template by template name={}", templateName);
        Preconditions.expectNotBlank(templateName, ErrorCodeEnum.TEMPLATE_INFO_INCORRECT);

        TemplateEntity templateEntity = templateEntityMapper.selectByName(templateName);
        if (templateEntity == null) {
            LOGGER.error("inlong template not found by template name={}", templateName);
            throw new BusinessException(ErrorCodeEnum.TEMPLATE_NOT_FOUND);
        }

        TemplateInfo templateInfo = CommonBeanUtils.copyProperties(templateEntity, TemplateInfo::new);
        List<TemplateField> templateFields = getTemplateFields(templateEntity.getId());
        templateInfo.setFieldList(templateFields);
        List<TenantTemplateEntity> tenantTemplateEntities = tenantTemplateEntityMapper.selectByTemplateName(
                templateName);
        if (Objects.equals(templateEntity.getVisibleRange(), TemplateVisibleRange.TENANT.name())
                && CollectionUtils.isNotEmpty(tenantTemplateEntities)) {
            List<String> tenantList = tenantTemplateEntities.stream()
                    .map(TenantTemplateEntity::getTenant)
                    .collect(Collectors.toList());
            checkVis(templateEntity, tenantList, operator);
            templateInfo.setTenantList(tenantList);
        }
        return templateInfo;
    }

    private void checkVis(TemplateEntity templateEntity, List<String> tenantList, String operator) {
        if (Objects.equals(templateEntity.getVisibleRange(), TemplateVisibleRange.IN_CHARGE.name())
                && !templateEntity.getInCharges().contains(operator)) {
            throw new BusinessException("current user is not in charge");
        }
        if (Objects.equals(templateEntity.getVisibleRange(), TemplateVisibleRange.TENANT.name())
                && !tenantList.contains(LoginUserUtils.getLoginUser().getTenant())) {
            throw new BusinessException("current user is not in tenant");
        }
    }

    @Override
    public PageResult<TemplateInfo> list(TemplatePageRequest request) {
        PageHelper.startPage(request.getPageNum(), request.getPageSize());
        Page<TemplateEntity> entityPage = (Page<TemplateEntity>) templateEntityMapper.selectByCondition(request);
        PageResult<TemplateInfo> pageResult = PageResult.fromPage(entityPage)
                .map(entity -> {
                    TemplateInfo response = CommonBeanUtils.copyProperties(entity, TemplateInfo::new);

                    List<String> tenantList = tenantTemplateEntityMapper
                            .selectByTemplateName(entity.getName()).stream()
                            .map(TenantTemplateEntity::getTenant)
                            .collect(Collectors.toList());
                    response.setTenantList(tenantList);
                    return response;
                });
        LOGGER.debug("success to list template page, result size {}", pageResult.getList().size());
        return pageResult;
    }

    private List<TemplateField> getTemplateFields(Integer templateId) {
        List<TemplateFieldEntity> fieldEntityList = templateFieldEntityMapper.selectByTemplateId(templateId);
        if (CollectionUtils.isEmpty(fieldEntityList)) {
            return Collections.emptyList();
        }
        return CommonBeanUtils.copyListProperties(fieldEntityList, TemplateField::new);
    }

    @Override
    @Transactional(rollbackFor = Throwable.class)
    public Boolean update(TemplateRequest request, String operator) {
        LOGGER.debug("begin to update inlong template, request={}", request);

        String templateName = request.getName();
        TemplateEntity templateEntity = templateEntityMapper.selectByName(templateName);
        if (templateEntity == null) {
            LOGGER.error("inlong template not found by template name={}", templateName);
            throw new BusinessException(ErrorCodeEnum.TEMPLATE_NOT_FOUND);
        }

        if (!templateEntity.getInCharges().contains(operator)) {
            throw new BusinessException(ErrorCodeEnum.TEMPLATE_PERMISSION_DENIED,
                    String.format("user [%s] has no update privilege for the inlong temlate", operator));
        }

        String errMsg = String.format("template has already updated with template name=%s, curVersion=%s",
                templateEntity.getName(), request.getVersion());
        if (!Objects.equals(templateEntity.getVersion(), request.getVersion())) {
            LOGGER.error(errMsg);
            throw new BusinessException(ErrorCodeEnum.CONFIG_EXPIRED);
        }

        CommonBeanUtils.copyProperties(request, templateEntity, true);
        templateEntity.setModifier(operator);
        int rowCount = templateEntityMapper.updateByIdSelective(templateEntity);
        if (rowCount != InlongConstants.AFFECTED_ONE_ROW) {
            LOGGER.error(errMsg);
            throw new BusinessException(ErrorCodeEnum.CONFIG_EXPIRED);
        }
        // update template fields
        updateField(request);
        if (CollectionUtils.isNotEmpty(request.getTenantList())) {
            Set<String> updatedTenants = new HashSet<>(request.getTenantList());
            List<TenantTemplateEntity> tenantList = tenantTemplateEntityMapper.selectByTemplateName(templateName);
            // remove
            tenantList.stream()
                    .filter(entity -> !updatedTenants.contains(entity.getTenant()))
                    .forEach(entity -> {
                        try {
                            this.deleteTenantTemplate(entity.getId(), operator);
                        } catch (Exception e) {
                            LOGGER.error(e.getMessage());
                        }
                    });
            // add
            Set<String> currentTenants = tenantList.stream()
                    .map(TenantTemplateEntity::getTenant)
                    .collect(Collectors.toSet());
            TenantTemplateRequest tagRequest = new TenantTemplateRequest();
            tagRequest.setTemplateName(templateName);
            updatedTenants.stream()
                    .filter(tenant -> !currentTenants.contains(tenant))
                    .forEach(tenant -> {
                        try {
                            tagRequest.setTenant(tenant);
                            this.saveTenantTemplate(tagRequest, operator);
                        } catch (Exception e) {
                            LOGGER.error(e.getMessage());
                        }
                    });
        }
        LOGGER.info("success to update inlong template for template name={}", templateName);
        return true;

    }

    @Override
    @Transactional(rollbackFor = Throwable.class)
    public Boolean delete(Integer id, String operator) {
        LOGGER.debug("begin to delete inlong template, Id={}", id);

        TemplateEntity entity = templateEntityMapper.selectByPrimaryKey(id);
        if (entity == null) {
            LOGGER.error("inlong template not found by template id={}", id);
            throw new BusinessException(ErrorCodeEnum.TEMPLATE_NOT_FOUND);
        }

        if (!entity.getInCharges().contains(operator)) {
            throw new BusinessException(ErrorCodeEnum.TEMPLATE_PERMISSION_DENIED,
                    String.format("user [%s] has no delete privilege for the inlong template", operator));
        }

        entity.setIsDeleted(entity.getId());
        entity.setModifier(operator);
        int rowCount = templateEntityMapper.updateByIdSelective(entity);
        if (rowCount != InlongConstants.AFFECTED_ONE_ROW) {
            LOGGER.error("template has already updated with template id={}, curVersion={}",
                    id, entity.getVersion());
            throw new BusinessException(ErrorCodeEnum.CONFIG_EXPIRED);
        }
        // Logically delete the associated field table
        LOGGER.debug("begin to delete inlong template field, templateId={}", id);
        templateFieldEntityMapper.logicDeleteAllByTemplateId(id);

        LOGGER.info("success to delete inlong template, fields for templateId={}", id);
        return true;
    }

    @Override
    @Transactional(rollbackFor = Throwable.class)
    public Boolean delete(String templateName, String operator) {
        LOGGER.debug("begin to delete inlong template, template name={}", templateName);

        TemplateEntity entity = templateEntityMapper.selectByName(templateName);
        if (entity == null) {
            LOGGER.error("inlong template not found by template name={}", templateName);
            throw new BusinessException(ErrorCodeEnum.TEMPLATE_NOT_FOUND);
        }

        if (!entity.getInCharges().contains(operator)) {
            throw new BusinessException(ErrorCodeEnum.TEMPLATE_PERMISSION_DENIED,
                    String.format("user [%s] has no delete privilege for the inlong template", operator));
        }

        entity.setIsDeleted(entity.getId());
        entity.setModifier(operator);
        int rowCount = templateEntityMapper.updateByIdSelective(entity);
        if (rowCount != InlongConstants.AFFECTED_ONE_ROW) {
            LOGGER.error("template has already updated with template name={}, curVersion={}",
                    templateName, entity.getVersion());
            throw new BusinessException(ErrorCodeEnum.CONFIG_EXPIRED);
        }
        // Logically delete the associated field table
        LOGGER.debug("begin to delete inlong template field, templateId={}", templateName);
        templateFieldEntityMapper.logicDeleteAllByTemplateId(entity.getId());

        LOGGER.info("success to delete inlong template, fields for templateName={}", templateName);
        return true;
    }

    @Transactional(rollbackFor = Throwable.class)
    public void saveField(TemplateRequest request) {
        List<TemplateField> fieldList = request.getFieldList();
        LOGGER.debug("begin to save template fields={}", fieldList);
        if (CollectionUtils.isEmpty(fieldList)) {
            return;
        }

        int size = fieldList.size();
        List<TemplateFieldEntity> entityList = new ArrayList<>(size);
        Integer templateId = request.getId();
        for (TemplateField fieldInfo : fieldList) {
            TemplateFieldEntity fieldEntity = CommonBeanUtils.copyProperties(fieldInfo, TemplateFieldEntity::new);
            if (StringUtils.isEmpty(fieldEntity.getFieldComment())) {
                fieldEntity.setFieldComment(fieldEntity.getFieldName());
            }
            fieldEntity.setTemplateId(templateId);
            fieldEntity.setIsDeleted(InlongConstants.UN_DELETED);
            entityList.add(fieldEntity);
        }

        templateFieldEntityMapper.insertAll(entityList);
        LOGGER.debug("success to save sink fields");
    }

    @Transactional(rollbackFor = Throwable.class)
    public void updateField(TemplateRequest request) {
        Integer templateId = request.getId();
        List<TemplateField> fieldRequestList = request.getFieldList();
        if (CollectionUtils.isEmpty(fieldRequestList)) {
            return;
        }

        // First physically delete the existing fields
        templateFieldEntityMapper.deleteAllByTemplateId(templateId);
        // Then batch save the sink fields
        this.saveField(request);
        LOGGER.info("success to update template field");
    }

    public Integer saveTenantTemplate(TenantTemplateRequest request, String operator) {
        LOGGER.debug("begin to save tenant template {}", request);
        Preconditions.expectNotNull(request, "tenant cluster request cannot be empty");
        Preconditions.expectNotBlank(request.getTemplateName(), ErrorCodeEnum.INVALID_PARAMETER,
                "template name cannot be empty");
        Preconditions.expectNotBlank(request.getTenant(), ErrorCodeEnum.INVALID_PARAMETER,
                "tenant cannot be empty");

        TenantTemplateEntity entity = CommonBeanUtils.copyProperties(request, TenantTemplateEntity::new);
        entity.setCreator(operator);
        entity.setModifier(operator);
        tenantTemplateEntityMapper.insert(entity);
        LOGGER.info("success to save tenant tag, tenant={}, template name={}", request.getTenant(),
                request.getTemplateName());
        return entity.getId();
    }

    public Boolean deleteTenantTemplate(Integer id, String operator) {
        LOGGER.debug("start to delete tenant template with id={}", id);
        TenantTemplateEntity entity = tenantTemplateEntityMapper.selectByPrimaryKey(id);
        Preconditions.expectNotNull(entity, ErrorCodeEnum.RECORD_NOT_FOUND.getMessage());

        entity.setModifier(operator);
        entity.setIsDeleted(id);

        int rowCount = tenantTemplateEntityMapper.updateByIdSelective(entity);
        if (rowCount != InlongConstants.AFFECTED_ONE_ROW) {
            LOGGER.error("tenant template has already deleted for tenant={} tag={}",
                    entity.getTenant(), entity.getTemplateName());
            throw new BusinessException(ErrorCodeEnum.CONFIG_EXPIRED);
        }
        LOGGER.info("success to delete tenant template of tenant={} tag={}, user={}", entity.getTenant(),
                entity.getTemplateName(), operator);
        return true;
    }

}
