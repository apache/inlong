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
import org.apache.inlong.manager.dao.entity.PackageConfigEntity;
import org.apache.inlong.manager.dao.mapper.PackageConfigEntityMapper;
import org.apache.inlong.manager.pojo.common.OrderFieldEnum;
import org.apache.inlong.manager.pojo.common.OrderTypeEnum;
import org.apache.inlong.manager.pojo.common.PageResult;
import org.apache.inlong.manager.pojo.module.PackagePageRequest;
import org.apache.inlong.manager.pojo.module.PackageRequest;
import org.apache.inlong.manager.pojo.module.PackageResponse;
import org.apache.inlong.manager.pojo.user.UserInfo;

import com.github.pagehelper.Page;
import com.github.pagehelper.PageHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;

/**
 * Package service layer implementation
 */
@Service
public class PackageServiceImpl implements PackageService {

    private static final Logger LOGGER = LoggerFactory.getLogger(PackageServiceImpl.class);

    @Autowired
    private PackageConfigEntityMapper packageConfigEntityMapper;

    @Override
    public Integer save(PackageRequest request, String operator) {
        LOGGER.info("begin to save package info: {}", request);
        PackageConfigEntity packageConfigEntity = CommonBeanUtils.copyProperties(request, PackageConfigEntity::new);
        packageConfigEntity.setCreator(operator);
        packageConfigEntity.setModifier(operator);
        int id = packageConfigEntityMapper.insert(packageConfigEntity);
        LOGGER.info("success to save package info: {}", request);
        return id;
    }

    @Override
    public Boolean update(PackageRequest request, String operator) {
        LOGGER.info("begin to update package info: {}", request);
        PackageConfigEntity packageConfigEntity = packageConfigEntityMapper.selectByPrimaryKey(request.getId());
        if (packageConfigEntity == null) {
            throw new BusinessException(ErrorCodeEnum.PACKAGE_NOT_FOUND,
                    String.format("Package does not exist with id=%s", request.getId()));
        }
        CommonBeanUtils.copyProperties(request, packageConfigEntity, true);
        packageConfigEntity.setModifier(operator);
        packageConfigEntityMapper.updateByIdSelective(packageConfigEntity);
        LOGGER.info("success to update package info: {}", request);
        return true;
    }

    @Override
    public PackageResponse get(Integer id, UserInfo opInfo) {
        LOGGER.info("begin to get package info for id = {}", id);
        PackageConfigEntity packageConfigEntity = packageConfigEntityMapper.selectByPrimaryKey(id);
        if (packageConfigEntity == null) {
            throw new BusinessException(ErrorCodeEnum.PACKAGE_NOT_FOUND,
                    String.format("Package does not exist with id=%s", id));
        }
        LOGGER.info("success to get package info for id = {}", id);
        return CommonBeanUtils.copyProperties(packageConfigEntity, PackageResponse::new);
    }

    @Override
    public PageResult<PackageResponse> listByCondition(PackagePageRequest request) {
        LOGGER.debug("begin to list package page, request = {}", request);
        PageHelper.startPage(request.getPageNum(), request.getPageSize());
        OrderFieldEnum.checkOrderField(request);
        OrderTypeEnum.checkOrderType(request);
        Page<PackageConfigEntity> entityPage =
                (Page<PackageConfigEntity>) packageConfigEntityMapper.selectByCondition(request);
        List<PackageResponse> packageResponseList =
                CommonBeanUtils.copyListProperties(entityPage, PackageResponse::new);
        PageResult<PackageResponse> pageResult = new PageResult<>(packageResponseList, entityPage.getTotal(),
                entityPage.getPageNum(), entityPage.getPageSize());
        LOGGER.debug("success to list package page, result size {}", pageResult.getList().size());
        return pageResult;
    }

    @Override
    public Boolean delete(Integer id, String operator) {
        LOGGER.info("begin to delete packeage by id={}", id);
        Preconditions.expectNotNull(id, ErrorCodeEnum.ID_IS_EMPTY.getMessage());
        PackageConfigEntity entity = packageConfigEntityMapper.selectByPrimaryKey(id);
        Preconditions.expectNotNull(entity, ErrorCodeEnum.PACKAGE_NOT_FOUND.getMessage());
        entity.setModifier(operator);
        entity.setIsDeleted(entity.getId());
        packageConfigEntityMapper.updateByIdSelective(entity);
        LOGGER.info("success to delete package by id: {}", entity);
        return true;
    }
}
