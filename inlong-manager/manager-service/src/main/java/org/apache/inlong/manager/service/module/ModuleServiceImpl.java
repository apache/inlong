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

package org.apache.inlong.manager.service.module;

import org.apache.inlong.manager.common.enums.ErrorCodeEnum;
import org.apache.inlong.manager.common.exceptions.BusinessException;
import org.apache.inlong.manager.common.util.CommonBeanUtils;
import org.apache.inlong.manager.common.util.Preconditions;
import org.apache.inlong.manager.dao.entity.ModuleConfigEntity;
import org.apache.inlong.manager.dao.mapper.ModuleConfigEntityMapper;
import org.apache.inlong.manager.pojo.common.OrderFieldEnum;
import org.apache.inlong.manager.pojo.common.OrderTypeEnum;
import org.apache.inlong.manager.pojo.common.PageResult;
import org.apache.inlong.manager.pojo.module.ModuleDTO;
import org.apache.inlong.manager.pojo.module.ModulePageRequest;
import org.apache.inlong.manager.pojo.module.ModuleRequest;
import org.apache.inlong.manager.pojo.module.ModuleResponse;
import org.apache.inlong.manager.pojo.user.UserInfo;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.pagehelper.Page;
import com.github.pagehelper.PageHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

/**
 * Module service layer implementation
 */
@Service
public class ModuleServiceImpl implements ModuleService {

    private static final Logger LOGGER = LoggerFactory.getLogger(ModuleServiceImpl.class);

    @Autowired
    private ModuleConfigEntityMapper moduleConfigEntityMapper;
    @Autowired
    private ObjectMapper objectMapper;

    @Override
    public Integer save(ModuleRequest request, String operator) {
        LOGGER.info("begin to save module info: {}", request);
        ModuleConfigEntity moduleConfigEntity = CommonBeanUtils.copyProperties(request, ModuleConfigEntity::new);
        try {
            ModuleDTO dto = ModuleDTO.getFromRequest(request, moduleConfigEntity.getExtParams(),
                    moduleConfigEntity.getPackageId());
            String extParams = objectMapper.writeValueAsString(dto);
            moduleConfigEntity.setExtParams(extParams);
        } catch (Exception e) {
            throw new BusinessException(ErrorCodeEnum.MODULE_INFO_INCORRECT,
                    String.format("serialize extParams of module failure: %s", e.getMessage()));
        }
        moduleConfigEntity.setCreator(operator);
        moduleConfigEntity.setModifier(operator);
        int id = moduleConfigEntityMapper.insert(moduleConfigEntity);

        LOGGER.info("success to save module info: {}", request);
        return id;
    }

    @Override
    public Boolean update(ModuleRequest request, String operator) {
        LOGGER.info("begin to update module info: {}", request);
        ModuleConfigEntity moduleConfigEntity = moduleConfigEntityMapper.selectByPrimaryKey(request.getId());
        if (moduleConfigEntity == null) {
            throw new BusinessException(ErrorCodeEnum.MODULE_NOT_FOUND,
                    String.format("Module does not exist with id=%s", request.getId()));
        }
        CommonBeanUtils.copyProperties(request, moduleConfigEntity, true);
        try {
            ModuleDTO dto = ModuleDTO.getFromRequest(request, moduleConfigEntity.getExtParams(),
                    moduleConfigEntity.getPackageId());
            String extParams = objectMapper.writeValueAsString(dto);
            request.setExtParams(extParams);
        } catch (Exception e) {
            throw new BusinessException(ErrorCodeEnum.MODULE_INFO_INCORRECT,
                    String.format("serialize extParams of module failure: %s", e.getMessage()));
        }
        CommonBeanUtils.copyProperties(request, moduleConfigEntity, true);
        moduleConfigEntity.setModifier(operator);
        moduleConfigEntityMapper.updateByIdSelective(moduleConfigEntity);
        LOGGER.info("success to update module info: {}", request);
        return true;
    }

    @Override
    public ModuleResponse get(Integer id, UserInfo opInfo) {
        LOGGER.info("begin to get module info for id = {}", id);
        ModuleConfigEntity entity = moduleConfigEntityMapper.selectByPrimaryKey(id);
        if (entity == null) {
            throw new BusinessException(ErrorCodeEnum.MODULE_NOT_FOUND,
                    String.format("Module config does not exist with id=%s", id));
        }
        ModuleResponse response = CommonBeanUtils.copyProperties(entity, ModuleResponse::new);
        CommonBeanUtils.copyProperties(ModuleDTO.getFromJson(entity.getExtParams()), response, true);
        LOGGER.info("begin to get module info for id = {}", id);
        return response;
    }

    @Override
    public PageResult<ModuleResponse> listByCondition(ModulePageRequest request) {
        LOGGER.debug("begin to list source page, request = {}", request);
        PageHelper.startPage(request.getPageNum(), request.getPageSize());
        OrderFieldEnum.checkOrderField(request);
        OrderTypeEnum.checkOrderType(request);
        Page<ModuleConfigEntity> entityPage =
                (Page<ModuleConfigEntity>) moduleConfigEntityMapper.selectByCondition(request);

        PageResult<ModuleResponse> pageResult = PageResult.fromPage(entityPage)
                .map(entity -> {
                    ModuleResponse response = CommonBeanUtils.copyProperties(entity, ModuleResponse::new);
                    CommonBeanUtils.copyProperties(ModuleDTO.getFromJson(entity.getExtParams()), response, true);
                    return response;
                });
        LOGGER.debug("success to list source page, result size {}", pageResult.getList().size());
        return pageResult;
    }

    @Override
    public Boolean delete(Integer id, String operator) {
        LOGGER.info("begin to delete module config by id={}", id);
        Preconditions.expectNotNull(id, ErrorCodeEnum.ID_IS_EMPTY.getMessage());
        ModuleConfigEntity entity = moduleConfigEntityMapper.selectByPrimaryKey(id);
        Preconditions.expectNotNull(entity, ErrorCodeEnum.MODULE_NOT_FOUND.getMessage());
        entity.setModifier(operator);
        entity.setIsDeleted(entity.getId());
        moduleConfigEntityMapper.updateByIdSelective(entity);
        LOGGER.info("success to delete module config by id: {}", entity);
        return true;
    }

}
