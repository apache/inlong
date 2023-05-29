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

package org.apache.inlong.manager.service.user;

import com.github.pagehelper.Page;
import com.github.pagehelper.PageHelper;
import com.github.pagehelper.PageInfo;
import lombok.extern.slf4j.Slf4j;
import org.apache.inlong.manager.common.consts.InlongConstants;
import org.apache.inlong.manager.common.enums.ErrorCodeEnum;
import org.apache.inlong.manager.common.exceptions.BusinessException;
import org.apache.inlong.manager.common.util.CommonBeanUtils;
import org.apache.inlong.manager.common.util.Preconditions;
import org.apache.inlong.manager.dao.entity.InlongTenantEntity;
import org.apache.inlong.manager.dao.entity.TenantRoleEntity;
import org.apache.inlong.manager.dao.mapper.InlongTenantEntityMapper;
import org.apache.inlong.manager.dao.mapper.TenantRoleEntityMapper;
import org.apache.inlong.manager.pojo.user.TenantRoleInfo;
import org.apache.inlong.manager.pojo.user.TenantRolePageRequest;
import org.apache.inlong.manager.pojo.user.TenantRoleRequest;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import static org.apache.inlong.manager.common.enums.ErrorCodeEnum.TENANT_NOT_EXIST;

/**
 * Role operation
 */
@Slf4j
@Service
public class TenantRoleServiceImpl implements TenantRoleService {

    @Autowired
    private TenantRoleEntityMapper tenantRoleEntityMapper;

    @Autowired
    private InlongTenantEntityMapper tenantMapper;

    @Override
    public PageInfo<TenantRoleInfo> listByCondition(TenantRolePageRequest request) {
        PageHelper.startPage(request.getPageNum(), request.getPageSize());
        Page<TenantRoleEntity> entityPage = tenantRoleEntityMapper.listByCondition(request);
        return entityPage.toPageInfo(entity -> CommonBeanUtils.copyProperties(entity, TenantRoleInfo::new));
    }

    @Override
    public int save(TenantRoleRequest request) {
        String tenantName = request.getTenant();
        String username = request.getUsername();
        Preconditions.expectNotBlank(tenantName, "Failed to save tenant user role, tenant should not be blank");
        Preconditions.expectNotBlank(username, "Failed to save tenant user role, user should not be blank");
        Preconditions.expectNotBlank(request.getRoleCode(),
                "Failed to save tenant user role, role code should not be blank");

        InlongTenantEntity tenant = tenantMapper.selectByName(tenantName);
        Preconditions.expectNotNull(tenant, TENANT_NOT_EXIST, String.format(TENANT_NOT_EXIST.getMessage(), tenantName));

        TenantRoleEntity entity = CommonBeanUtils.copyProperties(request, TenantRoleEntity::new);
        String operator = LoginUserUtils.getLoginUser().getName();
        entity.setCreator(operator);
        entity.setModifier(operator);
        tenantRoleEntityMapper.insert(entity);
        return entity.getId();
    }

    @Override
    public boolean update(TenantRoleRequest request) {
        TenantRoleEntity exist = tenantRoleEntityMapper.selectById(request.getId());
        Preconditions.expectNotNull(exist, ErrorCodeEnum.RECORD_NOT_FOUND,
                String.format("tenant user role record not found by id=%s", request.getId()));

        TenantRoleEntity entity = CommonBeanUtils.copyProperties(request, TenantRoleEntity::new);
        String operator = LoginUserUtils.getLoginUser().getName();
        entity.setModifier(operator);
        int rowCount = tenantRoleEntityMapper.updateById(entity);
        if (rowCount != InlongConstants.AFFECTED_ONE_ROW) {
            throw new BusinessException(ErrorCodeEnum.CONFIG_EXPIRED,
                    String.format(
                            "failure to update tenant user role with id=%d, request version=%d, updated row=%d",
                            request.getId(), request.getVersion(), rowCount));
        }
        return true;
    }

    @Override
    public TenantRoleInfo get(int id) {
        TenantRoleEntity entity = tenantRoleEntityMapper.selectById(id);
        return CommonBeanUtils.copyProperties(entity, TenantRoleInfo::new);
    }

}
