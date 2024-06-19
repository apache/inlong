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

import org.apache.inlong.manager.common.consts.InlongConstants;
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

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import static org.apache.inlong.manager.common.enums.ScheduleStatus.APPROVED;
import static org.apache.inlong.manager.common.enums.ScheduleStatus.DELETED;
import static org.apache.inlong.manager.common.enums.ScheduleStatus.NEW;
import static org.apache.inlong.manager.common.enums.ScheduleStatus.REGISTERED;
import static org.apache.inlong.manager.common.enums.ScheduleStatus.UPDATED;

@Service
public class ScheduleServiceImpl implements ScheduleService {

    private static Logger LOGGER = LoggerFactory.getLogger(ScheduleServiceImpl.class);

    @Autowired
    private InlongGroupEntityMapper groupMapper;
    @Autowired
    private ScheduleEntityMapper scheduleEntityMapper;

    // finite state machine
    private Map<ScheduleStatus, Set<ScheduleStatus>> fsm;

    @Override
    public int save(ScheduleInfoRequest request, String operator) {
        LOGGER.debug("begin to save schedule info, scheduleInfo={}, operator={}", request, operator);

        String groupId = request.getInlongGroupId();
        checkGroupExist(groupId);
        if (scheduleEntityMapper.selectByGroupId(groupId) != null) {
            LOGGER.error("schedule info for group={} already exists", groupId);
            throw new BusinessException(ErrorCodeEnum.SCHEDULE_DUPLICATE);
        }

        ScheduleEntity scheduleEntity = CommonBeanUtils.copyProperties(request, ScheduleEntity::new);
        scheduleEntity.setStatus(ScheduleStatus.NEW.getCode());
        scheduleEntity.setCreator(operator);
        scheduleEntity.setModifier(operator);
        scheduleEntityMapper.insert(scheduleEntity);
        return scheduleEntity.getId();
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
        String groupId = request.getInlongGroupId();
        ScheduleEntity entity = getScheduleEntity(groupId);
        String errMsg =
                String.format("schedule info has already been updated with groupId=%s, curVersion=%s, expectVersion=%s",
                        entity.getInlongGroupId(), request.getVersion(), entity.getVersion());
        if (!Objects.equals(entity.getVersion(), request.getVersion())) {
            LOGGER.error(errMsg);
            throw new BusinessException(ErrorCodeEnum.CONFIG_EXPIRED);
        }
        CommonBeanUtils.copyProperties(request, entity, true);
        entity.setModifier(operator);
        updateScheduleInfo(entity, errMsg);
        LOGGER.info("success to update schedule info for groupId={}", groupId);
        return true;
    }

    @Override
    public Boolean updateStatus(String groupId, ScheduleStatus newStatus, String operator) {
        LOGGER.debug("begin to update schedule status for groupId={}", groupId);
        ScheduleEntity entity = getScheduleEntity(groupId);
        ScheduleStatus preStatus = ScheduleStatus.forCode(entity.getStatus());
        if (!isAllowedTransition(preStatus, newStatus)) {
            String errorMsg = String.format("Schedule status transition is not allowed from %s to %s for group %s",
                    preStatus, newStatus, groupId);
            LOGGER.error(errorMsg);
            throw new BusinessException(ErrorCodeEnum.SCHEDULE_STATUS_TRANSITION_NOT_ALLOWED);
        }
        entity.setStatus(newStatus.getCode());
        entity.setModifier(operator);
        updateScheduleInfo(entity,
                String.format("update schedule status from %s to %s failed for groupId=%s",
                        preStatus.getCode(), newStatus.getCode(), entity.getInlongGroupId()));
        LOGGER.info("success to update schedule status from {} to {} for groupId={}",
                preStatus.getCode(), newStatus.getCode(), groupId);
        return true;
    }

    private void initFSMIfNeed() {
        if (fsm != null) {
            return;
        }
        fsm = new HashMap<>();
        fsm.put(NEW, new HashSet<>(Arrays.asList(APPROVED, DELETED)));
        fsm.put(APPROVED, new HashSet<>(Arrays.asList(REGISTERED, DELETED)));
        fsm.put(REGISTERED, new HashSet<>(Arrays.asList(UPDATED, DELETED)));
        fsm.put(UPDATED, new HashSet<>(Arrays.asList(REGISTERED, DELETED)));
    }

    private boolean isAllowedTransition(ScheduleStatus preStatus, ScheduleStatus newStatus) {
        initFSMIfNeed();
        return fsm.get(preStatus).contains(newStatus);
    }

    @Override
    public Boolean deleteByGroupId(String groupId, String operator) {
        LOGGER.debug("begin to delete schedule info for groupId={}", groupId);
        checkGroupExist(groupId);
        ScheduleEntity entity = scheduleEntityMapper.selectByGroupId(groupId);
        if (entity == null) {
            LOGGER.error("schedule info for groupId={} does not exist", groupId);
            return false;
        }
        entity.setPreviousStatus(entity.getStatus());
        entity.setStatus(ScheduleStatus.DELETED.getCode());
        entity.setModifier(operator);
        entity.setIsDeleted(entity.getId());
        updateScheduleInfo(entity,
                String.format("schedule info has already been updated with groupId=%s, curVersion=%s",
                        entity.getInlongGroupId(), entity.getVersion()));
        LOGGER.info("success to delete schedule info for groupId={}", groupId);
        return true;
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
            LOGGER.error("schedule info for group={} not found", groupId);
            throw new BusinessException(ErrorCodeEnum.SCHEDULE_NOT_FOUND);
        }
        return entity;
    }

    /**
     * Update schedule entity and throw exception if update failed.
     * @param entity to update
     * @param errorMsg when update failed.
     * @return
     *
     * */
    private void updateScheduleInfo(ScheduleEntity entity, String errorMsg) {
        if (scheduleEntityMapper.updateByIdSelective(entity) != InlongConstants.AFFECTED_ONE_ROW) {
            LOGGER.error(errorMsg);
            throw new BusinessException(ErrorCodeEnum.CONFIG_EXPIRED);
        }
    }
}
