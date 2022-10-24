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

package org.apache.inlong.manager.service.task;

import lombok.extern.slf4j.Slf4j;
import org.apache.inlong.manager.common.enums.GroupStatus;
import org.apache.inlong.manager.dao.entity.InlongGroupEntity;
import org.apache.inlong.manager.dao.mapper.InlongGroupEntityMapper;
import org.apache.inlong.manager.pojo.group.InlongGroupPageRequest;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.util.List;

/**
 * Cleanup group intermediate status such as GroupStatus.CONFIG_ING after startup,
 * presumably restarted from abnormal termination, to prevent from process being blocked indefinitely
 */
@Slf4j
@Service
public class GroupStatusCleanupTask implements InitializingBean {

    /**
     * The execution starts after this delay in seconds.
     */
    private static final String SystemInitModifier = "system-startup";

    @Value("${group.status.cleanup.enabled:false}")
    private Boolean enabled;

    @Autowired
    private InlongGroupEntityMapper groupMapper;

    @Override
    public void afterPropertiesSet() {
        if (!enabled) {
            return;
        }

        try {
            InlongGroupPageRequest request = InlongGroupPageRequest.builder()
                    .status(GroupStatus.CONFIG_ING.getCode()).build();
            List<InlongGroupEntity> groupEntities = groupMapper.selectByCondition(request);
            
            for (InlongGroupEntity entity : groupEntities) {
                groupMapper.updateStatus(entity.getInlongGroupId(), GroupStatus.CONFIG_FAILED.getCode(),
                        SystemInitModifier);
            }
            log.info("success to cleanup group status, group entities size: {}", groupEntities.size());
        } catch (Exception e) {
            log.error("exception while cleanup group status on startup: ", e);
        }
    }
}
