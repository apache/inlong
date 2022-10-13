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

package org.apache.inlong.manager.service.timertask;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import org.apache.inlong.manager.dao.mapper.InlongGroupEntityMapper;
import org.apache.inlong.manager.dao.mapper.InlongGroupExtEntityMapper;
import org.apache.inlong.manager.dao.mapper.InlongStreamEntityMapper;
import org.apache.inlong.manager.dao.mapper.InlongStreamExtEntityMapper;
import org.apache.inlong.manager.dao.mapper.InlongStreamFieldEntityMapper;
import org.apache.inlong.manager.dao.mapper.StreamSinkEntityMapper;
import org.apache.inlong.manager.dao.mapper.StreamSinkFieldEntityMapper;
import org.apache.inlong.manager.dao.mapper.StreamSourceEntityMapper;
import org.apache.inlong.manager.dao.mapper.StreamSourceFieldEntityMapper;
import org.apache.inlong.manager.dao.mapper.StreamTransformEntityMapper;
import org.apache.inlong.manager.dao.mapper.StreamTransformFieldEntityMapper;
import org.apache.inlong.manager.dao.mapper.WorkflowEventLogEntityMapper;
import org.apache.inlong.manager.dao.mapper.WorkflowProcessEntityMapper;
import org.apache.inlong.manager.dao.mapper.WorkflowTaskEntityMapper;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.util.Date;
import java.util.List;
import java.util.TimerTask;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

@Slf4j
@Service
public class GroupPurgeTask extends TimerTask implements InitializingBean {

    @Value("${group.purge.enabled:false}")
    private Boolean enabled;
    @Value("${group.purge.delay.in.seconds:10}")
    private Integer delay;
    @Value("${group.purge.period.in.seconds:300}")
    private Integer period;
    @Value("${group.purge.last.modify.gap.in.seconds:10}")
    private Integer lastModifyGap;
    @Value("${group.purge.limit:2000}")
    private Integer limit;

    @Autowired
    private InlongGroupEntityMapper groupEntityMapper;
    @Autowired
    private InlongGroupExtEntityMapper groupExtEntityMapper;
    @Autowired
    private InlongStreamEntityMapper streamEntityMapper;
    @Autowired
    private InlongStreamExtEntityMapper streamExtEntityMapper;
    @Autowired
    private InlongStreamFieldEntityMapper streamFieldEntityMapper;
    @Autowired
    private StreamSinkEntityMapper streamSinkEntityMapper;
    @Autowired
    private StreamSinkFieldEntityMapper streamSinkFieldEntityMapper;
    @Autowired
    private StreamSourceEntityMapper streamSourceEntityMapper;
    @Autowired
    private StreamSourceFieldEntityMapper streamSourceFieldEntityMapper;
    @Autowired
    private StreamTransformEntityMapper streamTransformEntityMapper;
    @Autowired
    private StreamTransformFieldEntityMapper streamTransformFieldEntityMapper;
    @Autowired
    private WorkflowEventLogEntityMapper workflowEventLogEntityMapper;
    @Autowired
    private WorkflowProcessEntityMapper workflowProcessEntityMapper;
    @Autowired
    private WorkflowTaskEntityMapper workflowTaskEntityMapper;

    @Override
    public void afterPropertiesSet() throws Exception {
        if (enabled) {
            log.info("start group purge timer task");
            ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();
            executor.scheduleWithFixedDelay(this, delay, period, TimeUnit.SECONDS);
        }
    }

    @Override
    public void run() {
        try {
            Date lastModifyTime = new Date(System.currentTimeMillis() - lastModifyGap * 1000L);
            List<String> groupIdsToPurge = groupEntityMapper.selectLogicalDeletedGroupIdsBefore(lastModifyTime, limit);
            log.info("group ids to purge: {}", groupIdsToPurge);
            if (CollectionUtils.isEmpty(groupIdsToPurge)) {
                return;
            }

            // cleanse configuration data
            groupEntityMapper.deleteByInlongGroupIds(groupIdsToPurge);
            groupExtEntityMapper.deleteByInlongGroupIds(groupIdsToPurge);
            streamEntityMapper.deleteByInlongGroupIds(groupIdsToPurge);
            streamExtEntityMapper.deleteByInlongGroupIds(groupIdsToPurge);
            streamFieldEntityMapper.deleteByInlongGroupIds(groupIdsToPurge);
            streamSinkEntityMapper.deleteByInlongGroupIds(groupIdsToPurge);
            streamSinkFieldEntityMapper.deleteByInlongGroupIds(groupIdsToPurge);
            streamSourceEntityMapper.deleteByInlongGroupIds(groupIdsToPurge);
            streamSourceFieldEntityMapper.deleteByInlongGroupIds(groupIdsToPurge);
            streamTransformEntityMapper.deleteByInlongGroupIds(groupIdsToPurge);
            streamTransformFieldEntityMapper.deleteByInlongGroupIds(groupIdsToPurge);

            // cleanse workflow data
            List<Integer> processIds = workflowProcessEntityMapper.selectProcessIdsByInlongGroupIds(groupIdsToPurge);
            workflowEventLogEntityMapper.deleteByInlongGroupIds(groupIdsToPurge);
            workflowTaskEntityMapper.deleteByProcessIds(processIds);
            workflowProcessEntityMapper.deleteByInlongGroupIds(groupIdsToPurge);
            log.info("deleted {} groups", groupIdsToPurge.size());
        } catch (Exception e) {
            log.error("exception while cleansing database ", e);
        }
    }
}
