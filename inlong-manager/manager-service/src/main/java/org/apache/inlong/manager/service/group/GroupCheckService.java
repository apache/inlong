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

package org.apache.inlong.manager.service.group;

import org.apache.inlong.manager.common.enums.ErrorCodeEnum;
import org.apache.inlong.manager.common.enums.GroupStatus;
import org.apache.inlong.manager.common.enums.StreamStatus;
import org.apache.inlong.manager.common.exceptions.BusinessException;
import org.apache.inlong.manager.dao.entity.InlongGroupEntity;
import org.apache.inlong.manager.dao.entity.InlongStreamEntity;
import org.apache.inlong.manager.dao.mapper.InlongGroupEntityMapper;
import org.apache.inlong.manager.dao.mapper.InlongStreamEntityMapper;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

/**
 * Service of inlong group check.
 */
@Service
public class GroupCheckService {

    @Autowired
    private InlongGroupEntityMapper groupMapper;
    @Autowired
    private InlongStreamEntityMapper streamMapper;

    /**
     * Check whether the inlong group status is temporary
     *
     * @param groupId inlong group id
     * @return inlong group entity, for caller reuse
     */
    public InlongGroupEntity checkGroupStatus(String groupId, String operator) {
        InlongGroupEntity inlongGroupEntity = groupMapper.selectByGroupId(groupId);
        if (inlongGroupEntity == null) {
            throw new BusinessException(String.format("InlongGroup does not exist with InlongGroupId=%s", groupId));
        }

        GroupStatus status = GroupStatus.forCode(inlongGroupEntity.getStatus());
        if (GroupStatus.notAllowedUpdate(status)) {
            throw new BusinessException(String.format(ErrorCodeEnum.OPT_NOT_ALLOWED_BY_STATUS.getMessage(), status));
        }

        return inlongGroupEntity;
    }

    public InlongStreamEntity checkStreamStatus(String groupId, String streamId, String operator) {
        InlongStreamEntity inlongStreamEntity = streamMapper.selectByIdentifier(groupId, streamId);
        if (inlongStreamEntity == null) {
            throw new BusinessException(
                    String.format("InlongStream does not exist with groupId=%s, streamId=%s", groupId, streamId));
        }

        StreamStatus status = StreamStatus.forCode(inlongStreamEntity.getStatus());
        if (StreamStatus.notAllowedUpdate(status)) {
            throw new BusinessException(String.format(ErrorCodeEnum.OPT_NOT_ALLOWED_BY_STATUS.getMessage(), status));
        }

        return inlongStreamEntity;
    }
}
