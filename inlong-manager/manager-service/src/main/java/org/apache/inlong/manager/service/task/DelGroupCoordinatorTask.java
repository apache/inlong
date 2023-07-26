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

import org.apache.inlong.manager.common.enums.SourceStatus;
import org.apache.inlong.manager.dao.entity.StreamSourceEntity;
import org.apache.inlong.manager.dao.mapper.InlongGroupEntityMapper;
import org.apache.inlong.manager.dao.mapper.StreamSourceEntityMapper;
import org.apache.inlong.manager.service.group.coordinator.Coordinator;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.temporal.ChronoUnit;
import java.util.Date;
import java.util.List;
import java.util.TimerTask;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Stop all stream source which is running when group is deleted
 */
@Slf4j
@Service
public class DelGroupCoordinatorTask extends TimerTask implements Coordinator, InitializingBean {

    private static final int INITIAL_DELAY = 300;
    private static final int INTERVAL = 1800;

    @Value("${group.compromise.batchSize:100}")
    private Integer batchSize;
    @Autowired
    private InlongGroupEntityMapper groupMapper;
    @Autowired
    private StreamSourceEntityMapper sourceMapper;

    @Override
    public void afterPropertiesSet() throws Exception {
        log.info("start delete group compromise task");
        ScheduledExecutorService executor =
                Executors.newSingleThreadScheduledExecutor(new ThreadFactoryBuilder().setNameFormat("group-compromise"
                        + "-%s").build());
        executor.scheduleWithFixedDelay(this, INITIAL_DELAY, INTERVAL, TimeUnit.SECONDS);
    }

    @Override
    public void run() {
        LocalDateTime now = LocalDateTime.now();
        LocalDateTime twoHoursAgo = now.minusHours(2).truncatedTo(ChronoUnit.HOURS);
        Date modifyTime = Date.from(twoHoursAgo.atZone(ZoneId.systemDefault()).toInstant());
        List<String> groupIds = groupMapper.selectDeletedGroupIdsWithTimeAfter(modifyTime, batchSize);
        if (CollectionUtils.isEmpty(groupIds)) {
            return;
        }
        for (String groupId : groupIds) {
            coordinate(groupId);
        }
    }

    @Override
    public void coordinate(String inlongGroupId) {
        List<StreamSourceEntity> sourceList = sourceMapper.selectByRelatedId(inlongGroupId, null, null);
        if (CollectionUtils.isEmpty(sourceList)) {
            return;
        }
        for (StreamSourceEntity source : sourceList) {
            if (SourceStatus.SOURCE_NORMAL.getCode().equals(source.getStatus()) && StringUtils.isNotBlank(
                    source.getInlongClusterNodeGroup())) {
                source.setPreviousStatus(source.getStatus());
                source.setStatus(SourceStatus.TO_BE_ISSUED_DELETE.getCode());
                source.setIsDeleted(source.getId());
                sourceMapper.updateByPrimaryKey(source);
            }
        }
    }
}