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

import org.apache.inlong.common.bounded.Boundaries;
import org.apache.inlong.manager.common.consts.InlongConstants;
import org.apache.inlong.manager.common.exceptions.BusinessException;
import org.apache.inlong.manager.common.util.CommonBeanUtils;
import org.apache.inlong.manager.dao.entity.InlongGroupExtEntity;
import org.apache.inlong.manager.dao.mapper.InlongGroupExtEntityMapper;
import org.apache.inlong.manager.pojo.schedule.ScheduleInfo;
import org.apache.inlong.manager.pojo.schedule.ScheduleInfoRequest;
import org.apache.inlong.manager.pojo.stream.InlongStreamInfo;
import org.apache.inlong.manager.schedule.ScheduleClientFactory;
import org.apache.inlong.manager.schedule.ScheduleEngineClient;
import org.apache.inlong.manager.workflow.processor.OfflineJobOperator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.util.stream.Collectors;

import static org.apache.inlong.manager.common.enums.ScheduleStatus.APPROVED;
import static org.apache.inlong.manager.common.enums.ScheduleStatus.REGISTERED;
import static org.apache.inlong.manager.common.enums.ScheduleStatus.UPDATED;

@Service
public class ScheduleOperatorImpl implements ScheduleOperator {

    private static final Logger LOGGER = LoggerFactory.getLogger(ScheduleOperatorImpl.class);

    @Autowired
    private ScheduleService scheduleService;

    @Autowired
    private InlongGroupExtEntityMapper groupExtMapper;

    @Autowired
    private ScheduleClientFactory scheduleClientFactory;

    private OfflineJobOperator offlineJobOperator;

    @Override
    @Transactional(rollbackFor = Throwable.class)
    public int saveOpt(ScheduleInfoRequest request, String operator) {
        // save schedule info first
        int scheduleInfoId = scheduleService.save(request, operator);
        LOGGER.info("Save schedule info success for group {}", request.getInlongGroupId());
        // process new schedule info for approved inlong group
        registerScheduleInfoForApprovedGroup(CommonBeanUtils.copyProperties(request, ScheduleInfo::new), operator);
        return scheduleInfoId;
    }

    /**
     * If an inlong group in DATASYNC_OFFLINE_MODE created first without schedule info and has been approved, it should
     * be registered to schedule engine once the schedule info for this group is added.
     * */
    private void registerScheduleInfoForApprovedGroup(ScheduleInfo scheduleInfo, String operator) {
        String groupId = scheduleInfo.getInlongGroupId();
        InlongGroupExtEntity scheduleStatusExt =
                groupExtMapper.selectByUniqueKey(groupId, InlongConstants.REGISTER_SCHEDULE_STATUS);
        if (scheduleStatusExt != null && InlongConstants.REGISTERED.equalsIgnoreCase(scheduleStatusExt.getKeyValue())) {
            // change schedule state to approved
            scheduleService.updateStatus(scheduleInfo.getInlongGroupId(), APPROVED, operator);
            registerToScheduleEngine(scheduleInfo, operator, false);
            LOGGER.info("Register schedule info success for group {}", groupId);
        }
    }

    private ScheduleEngineClient getScheduleEngineClient(String scheduleEngine) {
        return scheduleClientFactory.getInstance(scheduleEngine);
    }

    @Override
    public Boolean scheduleInfoExist(String groupId) {
        return scheduleService.exist(groupId);
    }

    @Override
    public ScheduleInfo getScheduleInfo(String groupId) {
        return scheduleService.get(groupId);
    }

    @Override
    @Transactional(rollbackFor = Throwable.class)
    public Boolean updateOpt(ScheduleInfoRequest request, String operator) {
        // if the inlong group exist without schedule info
        // then, save the new schedule info when updating inlong group
        if (!scheduleInfoExist(request.getInlongGroupId())) {
            saveOpt(request, operator);
            return true;
        }
        ScheduleInfo scheduleInfo = CommonBeanUtils.copyProperties(request, ScheduleInfo::new);
        if (!needUpdate(scheduleInfo)) {
            LOGGER.info("schedule info not changed for group {}", request.getInlongGroupId());
            return false;
        }
        // update schedule info
        boolean res = scheduleService.update(request, operator);
        // update status
        scheduleService.updateStatus(request.getInlongGroupId(), UPDATED, operator);
        return res;
    }

    @Override
    @Transactional(rollbackFor = Throwable.class)
    public Boolean updateAndRegister(ScheduleInfoRequest request, String operator) {
        if (updateOpt(request, operator)) {
            return registerToScheduleEngine(CommonBeanUtils.copyProperties(request, ScheduleInfo::new), operator, true);
        }
        return false;
    }

    /**
     * There are three places may trigger resister schedule info to schedule engine:
     * - 1. new group approved with schedule info
     * - 2. new schedule info for an exist approved inlong group added
     * - 3. group's schedule info updated
     * */
    private Boolean registerToScheduleEngine(ScheduleInfo scheduleInfo, String operator, boolean isUpdate) {
        // update(un-register and then register) or register
        boolean res = isUpdate ? getScheduleEngineClient(scheduleInfo.getScheduleEngine()).update(scheduleInfo)
                : getScheduleEngineClient(scheduleInfo.getScheduleEngine()).register(scheduleInfo);
        // update status to REGISTERED
        scheduleService.updateStatus(scheduleInfo.getInlongGroupId(), REGISTERED, operator);
        LOGGER.info("{} schedule info success for group {}",
                isUpdate ? "Update" : "Register", scheduleInfo.getInlongGroupId());
        return res;
    }

    private boolean needUpdate(ScheduleInfo scheduleInfo) {
        if (scheduleInfo == null) {
            return false;
        }
        ScheduleInfo existedSchedule = getScheduleInfo(scheduleInfo.getInlongGroupId());
        return !scheduleInfo.equals(existedSchedule);
    }

    @Override
    public Boolean deleteByGroupIdOpt(String groupId, String operator) {
        return scheduleService.deleteByGroupId(groupId, operator);
    }

    @Override
    public Boolean handleGroupApprove(String groupId) {
        // if the inlong group exist without schedule info
        // then, save the new schedule info when updating inlong group
        if (!scheduleInfoExist(groupId)) {
            LOGGER.warn("schedule info not exist for group {}", groupId);
            return false;
        }
        // change schedule state to approved
        scheduleService.updateStatus(groupId, APPROVED, null);
        return registerToScheduleEngine(getScheduleInfo(groupId), null, false);
    }

    @Override
    public Boolean submitOfflineJob(String groupId, List<InlongStreamInfo> streamInfoList, Boundaries boundaries) {
        if (offlineJobOperator == null) {
            offlineJobOperator = OfflineJobOperatorFactory.getOfflineJobOperator();
        }
        try {
            offlineJobOperator.submitOfflineJob(groupId, streamInfoList, boundaries);
            LOGGER.info("Submit offline job for group {} and stream list {} success.", groupId,
                    streamInfoList.stream().map(InlongStreamInfo::getName).collect(Collectors.toList()));
        } catch (Exception e) {
            String errorMsg = String.format("Submit offline job failed for groupId=%s", groupId);
            LOGGER.error(errorMsg, e);
            throw new BusinessException(errorMsg);
        }
        return true;
    }

}
