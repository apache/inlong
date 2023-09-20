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
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.TimerTask;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor.AbortPolicy;
import java.util.concurrent.TimeUnit;

/**
 * Delete all stream source which is running but its inlong group was deleted.
 */
@Slf4j
@Service
public class DeleteStreamSourceTask extends TimerTask implements InitializingBean {

    private static final int INITIAL_DELAY_MINUTES = 5;
    private static final int INTERVAL_MINUTES = 60;

    @Value("${group.deleted.enabled:false}")
    private Boolean enabled;
    @Value("${group.deleted.batchSize:100}")
    private Integer batchSize;
    @Value("${group.deleted.latest.hours:10}")
    private Integer latestHours;

    @Autowired
    private InlongGroupEntityMapper groupMapper;
    @Autowired
    private StreamSourceEntityMapper sourceMapper;

    @Override
    public void afterPropertiesSet() {
        if (enabled) {
            ThreadFactory threadFactory = new ThreadFactoryBuilder().setNameFormat("inlong-group-delete-%s").build();
            ScheduledExecutorService executor = new ScheduledThreadPoolExecutor(1, threadFactory, new AbortPolicy());
            executor.scheduleWithFixedDelay(this, INITIAL_DELAY_MINUTES, INTERVAL_MINUTES, TimeUnit.MINUTES);

            log.info("success to start the delete stream source task");
        }
    }

    @Override
    public void run() {
        LocalDateTime currentTime = LocalDateTime.now();
        LocalDateTime latestTime = currentTime.minusHours(latestHours).truncatedTo(ChronoUnit.HOURS);
        Date modifyTime = Date.from(latestTime.atZone(ZoneId.systemDefault()).toInstant());

        List<String> groupIds = groupMapper.selectDeletedGroupIdsWithTimeAfter(modifyTime, batchSize);
        if (CollectionUtils.isEmpty(groupIds)) {
            return;
        }

        deleteSources(groupIds);
    }

    private void deleteSources(List<String> inlongGroupIds) {
        List<StreamSourceEntity> sourceList = sourceMapper.selectByGroupIds(inlongGroupIds);
        if (CollectionUtils.isEmpty(sourceList)) {
            return;
        }

        List<Integer> idList = new ArrayList<>();
        for (StreamSourceEntity source : sourceList) {
            if (SourceStatus.SOURCE_NORMAL.getCode().equals(source.getStatus())
                    && StringUtils.isNotBlank(source.getInlongClusterNodeGroup())) {
                idList.add(source.getId());
            }
        }

        if (CollectionUtils.isNotEmpty(idList)) {
            sourceMapper.logicalDeleteByIds(idList, SourceStatus.TO_BE_ISSUED_DELETE.getCode());
            log.info("success to delete stream source with id in {}", idList);
        }
    }
}
