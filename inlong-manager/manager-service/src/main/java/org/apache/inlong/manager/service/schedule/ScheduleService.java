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

import org.apache.inlong.manager.common.enums.ScheduleStatus;
import org.apache.inlong.manager.pojo.schedule.ScheduleInfo;
import org.apache.inlong.manager.pojo.schedule.ScheduleInfoRequest;

import javax.validation.Valid;
import javax.validation.constraints.NotNull;

public interface ScheduleService {

    /**
     * Save schedule info.
     *
     * @param request schedule request need to save
     * @param operator name of operator
     * @return schedule info id in backend storage
     */
    int save(@Valid @NotNull(message = "schedule request cannot be null") ScheduleInfoRequest request,
            String operator);

    /**
     * Query whether schedule info exists for specified inlong group
     *
     * @param groupId the group id to be queried
     * @return does it exist
     */
    Boolean exist(String groupId);

    /**
     * Get schedule info based on inlong group id
     *
     * @param groupId inlong group id
     * @return detail of inlong group
     */
    ScheduleInfo get(String groupId);

    /**
     * Modify schedule information
     *
     * @param request schedule request that needs to be modified
     * @param operator name of operator
     * @return whether succeed
     */
    Boolean update(@Valid @NotNull(message = "schedule request cannot be null") ScheduleInfoRequest request,
            String operator);

    /**
     * Update status of schedule info.
     *
     * @param groupId group to update schedule status
     * @param newStatus status to update
     * @param operator name of operator
     * @return whether succeed
     */
    Boolean updateStatus(String groupId, ScheduleStatus newStatus, String operator);

    /**
     * Delete schedule info for gropuId.
     * @param groupId groupId to find a schedule info to delete
     * @param operator  name of operator
     * @Return whether succeed
     * */
    Boolean deleteByGroupId(String groupId, String operator);
}
