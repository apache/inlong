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

package org.apache.inlong.manager.service.timer;

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
public class DataCleansingTask extends TimerTask implements InitializingBean {

    @Value("${data.cleansing.enabled:false}")
    private Boolean enabled;
    @Value("${data.cleansing.delay.in.seconds:10}")
    private Integer delay;
    @Value("${data.cleansing.period.in.seconds:300}")
    private Integer period;
    @Value("${data.cleansing.last.modify.gap.in.seconds:10}")
    private Integer lastModifyGap;
    @Value("${data.cleansing.limit:2000}")
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
            log.info("start data cleansing timer task");
            ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();
            executor.scheduleWithFixedDelay(this, delay, period, TimeUnit.SECONDS);
        }
    }

    @Override
    public void run() {
        try {
            Date lastModifyTime = new Date(System.currentTimeMillis() - lastModifyGap * 1000L);
            List<String> groupIds = groupEntityMapper.selectDeletedGroupIds(lastModifyTime, limit);
            log.info("group ids to delete: {}", groupIds);
            if (CollectionUtils.isEmpty(groupIds)) {
                return;
            }

            // cleanse configuration data
            groupEntityMapper.deleteByInlongGroupIds(groupIds);
            groupExtEntityMapper.deleteByInlongGroupIds(groupIds);
            streamEntityMapper.deleteByInlongGroupIds(groupIds);
            streamExtEntityMapper.deleteByInlongGroupIds(groupIds);
            streamFieldEntityMapper.deleteByInlongGroupIds(groupIds);
            streamSinkEntityMapper.deleteByInlongGroupIds(groupIds);
            streamSinkFieldEntityMapper.deleteByInlongGroupIds(groupIds);
            streamSourceEntityMapper.deleteByInlongGroupIds(groupIds);
            streamSourceFieldEntityMapper.deleteByInlongGroupIds(groupIds);
            streamTransformEntityMapper.deleteByInlongGroupIds(groupIds);
            streamTransformFieldEntityMapper.deleteByInlongGroupIds(groupIds);

            // cleanse workflow data
            List<Integer> processIds = workflowProcessEntityMapper.selectProcessIdsByInlongGroupIds(groupIds);
            workflowEventLogEntityMapper.deleteByInlongGroupIds(groupIds);
            workflowTaskEntityMapper.deleteByProcessIds(processIds);
            workflowProcessEntityMapper.deleteByInlongGroupIds(groupIds);
            log.info("deleted {} groups", groupIds.size());
        } catch (Exception e) {
            log.error("exception while cleansing database ", e);
        }
    }
}
