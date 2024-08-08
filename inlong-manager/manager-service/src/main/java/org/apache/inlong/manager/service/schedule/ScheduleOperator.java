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
import org.apache.inlong.manager.pojo.schedule.ScheduleInfo;
import org.apache.inlong.manager.pojo.schedule.ScheduleInfoRequest;
import org.apache.inlong.manager.pojo.stream.InlongStreamInfo;

import java.util.List;

/**
 * Operator for schedule. Including:
 * 1. schedule info management
 * 2. schedule operations like resister, un-register etc.
 * */
public interface ScheduleOperator {

    /**
     * Save schedule info.
     * There are two places may save schedule info:
     * - 1. create new inlong group with schedule info
     * - 2. create new schedule info directly(inlong group has been already exist), in this situation, we should
     *      register schedule info to schedule engine if group has been approved.
     * @param request schedule request need to save
     * @param operator name of operator
     * @return schedule info id in backend storage
     */
    int saveOpt(ScheduleInfoRequest request, String operator);

    /**
     * Check whether schedule info exists for specified inlong group
     *
     * @param groupId the group id to be queried
     * @return does it exist
     */
    Boolean scheduleInfoExist(String groupId);

    /**
     * Get schedule info based on inlong group id
     *
     * @param groupId inlong group id
     * @return detail of inlong group
     */
    ScheduleInfo getScheduleInfo(String groupId);

    /**
     * Modify schedule information
     * There are two places may update schedule info:
     * - 1. update inlong group with new schedule info
     * - 2. update schedule info directly(inlong group has been already exist)
     * @param request schedule request that needs to be modified
     * @param operator name of operator
     * @return whether succeed
     */
    Boolean updateOpt(ScheduleInfoRequest request, String operator);

    /**
     * Register schedule information
     * @param request schedule request that needs to be modified
     * @param operator name of operator
     * @return whether succeed
     */
    Boolean updateAndRegister(ScheduleInfoRequest request, String operator);

    /**
     * Delete schedule info for groupId.
     * There are two places may delete schedule info:
     * - 1. delete an inlong group
     * - 2. delete schedule info directly, left inlong group alone without schedule info, which means the group of
     * offline sync job will never be triggered
     * @param groupId groupId to find a schedule info to delete
     * @param operator  name of operator
     * @Return whether succeed
     * */
    Boolean deleteByGroupIdOpt(String groupId, String operator);

    /**
     * Handle inlong group approve, check schedule info and try to register it to schedule engine.
     * @param groupId groupId to find a schedule info to delete
     * @Return whether succeed
     * */
    Boolean handleGroupApprove(String groupId);

    /**
     * Start offline sync job when the schedule instance callback.
     * @param groupId groupId to start offline job
     * @param streamInfoList stream list to start offline job
     * @param boundaries source boundaries for bounded source
     * @Return whether succeed
     * */
    Boolean submitOfflineJob(String groupId, List<InlongStreamInfo> streamInfoList, Boundaries boundaries);
}
