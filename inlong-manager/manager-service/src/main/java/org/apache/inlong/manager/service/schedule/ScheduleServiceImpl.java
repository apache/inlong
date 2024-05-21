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

package org.apache.inlong.manager.service.schedule;

import org.apache.inlong.manager.common.enums.ErrorCodeEnum;
import org.apache.inlong.manager.common.enums.ScheduleStatus;
import org.apache.inlong.manager.common.exceptions.BusinessException;
import org.apache.inlong.manager.common.util.CommonBeanUtils;
import org.apache.inlong.manager.common.util.Preconditions;
import org.apache.inlong.manager.dao.entity.InlongGroupEntity;
import org.apache.inlong.manager.dao.entity.ScheduleEntity;
import org.apache.inlong.manager.dao.mapper.InlongGroupEntityMapper;
import org.apache.inlong.manager.dao.mapper.ScheduleEntityMapper;
import org.apache.inlong.manager.pojo.schedule.ScheduleInfo;
import org.apache.inlong.manager.pojo.schedule.ScheduleInfoRequest;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class ScheduleServiceImpl implements ScheduleService {

    private static Logger LOGGER = LoggerFactory.getLogger(ScheduleServiceImpl.class);

    @Autowired
    private InlongGroupEntityMapper groupMapper;
    @Autowired
    private ScheduleEntityMapper scheduleEntityMapper;

    @Override
    public int save(ScheduleInfoRequest scheduleInfo, String operator) {
        LOGGER.debug("begin to save schedule info, scheduleInfo: {}, operator: {}", scheduleInfo, operator);
        Preconditions.expectNotNull(scheduleInfo, "schedule info request can't be null");

        String groupId = scheduleInfo.getGroupId();
        checkGroupExist(groupId);
        if (scheduleEntityMapper.selectByGroupId(groupId) != null) {
            LOGGER.error("schedule info for group : {} already exists", groupId);
            throw new BusinessException(ErrorCodeEnum.SCHEDULE_DUPLICATE);
        }

        ScheduleEntity scheduleEntity = CommonBeanUtils.copyProperties(scheduleInfo, ScheduleEntity::new);
        scheduleEntity.setStatus(ScheduleStatus.NEW.getCode());
        scheduleEntity.setCreator(operator);
        scheduleEntity.setModifier(operator);
        return scheduleEntityMapper.insert(scheduleEntity);
    }

    @Override
    public Boolean exist(String groupId) {
        checkGroupExist(groupId);
        return scheduleEntityMapper.selectByGroupId(groupId) != null;
    }

    @Override
    public ScheduleInfo get(String groupId) {
        LOGGER.debug("begin to get schedule info by groupId={}", groupId);
        ScheduleEntity entity = getScheduleEntity(groupId);
        return CommonBeanUtils.copyProperties(entity, ScheduleInfo::new);
    }

    @Override
    public Boolean update(ScheduleInfoRequest request, String operator) {
        LOGGER.debug("begin to update schedule info={}", request);
        String groupId = request.getGroupId();
        ScheduleEntity entity = getScheduleEntity(groupId);
        CommonBeanUtils.copyProperties(request, entity, true);
        entity.setModifier(operator);
        scheduleEntityMapper.updateByIdSelective(entity);
        LOGGER.info("success to update schedule info for groupId={}", groupId);
        return true;
    }

    @Override
    public Boolean deleteByGroupId(String groupId, String operator) {
        LOGGER.debug("begin to delete schedule info for groupId={}", groupId);
        checkGroupExist(groupId);
        ScheduleEntity entity = scheduleEntityMapper.selectByGroupId(groupId);
        if (entity == null) {
            LOGGER.error("schedule info for groupId {} does not exist", groupId);
            return false;
        }
        int res = scheduleEntityMapper.deleteByGroupId(groupId);
        LOGGER.info("success to delete schedule info for groupId={}", groupId);
        return res > 0;
    }

    /**
     * Check whether InLongGroup exists, throw BusinessException with ErrorCodeEnum.GROUP_NOT_FOUND if check failed.
     * */
    private void checkGroupExist(String groupId) {
        Preconditions.expectNotBlank(groupId, ErrorCodeEnum.GROUP_ID_IS_EMPTY);
        InlongGroupEntity entity = groupMapper.selectByGroupId(groupId);
        if (entity == null) {
            LOGGER.error("inlong group not found by groupId={}", groupId);
            throw new BusinessException(ErrorCodeEnum.GROUP_NOT_FOUND);
        }
    }

    private ScheduleEntity getScheduleEntity(String groupId) {
        checkGroupExist(groupId);
        ScheduleEntity entity = scheduleEntityMapper.selectByGroupId(groupId);
        if (entity == null) {
            LOGGER.error("schedule info for group : {} not found", groupId);
            throw new BusinessException(ErrorCodeEnum.SCHEDULE_NOT_FOUND);
        }
        return entity;
    }
}
