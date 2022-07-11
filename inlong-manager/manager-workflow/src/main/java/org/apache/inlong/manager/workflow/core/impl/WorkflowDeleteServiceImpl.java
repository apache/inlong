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

package org.apache.inlong.manager.workflow.core.impl;

import lombok.extern.slf4j.Slf4j;
import org.apache.inlong.manager.common.pojo.workflow.ProcessQuery;
import org.apache.inlong.manager.dao.entity.WorkflowProcessEntity;
import org.apache.inlong.manager.dao.mapper.WorkflowEventLogEntityMapper;
import org.apache.inlong.manager.dao.mapper.WorkflowProcessEntityMapper;
import org.apache.inlong.manager.dao.mapper.WorkflowTaskEntityMapper;
import org.apache.inlong.manager.workflow.core.WorkflowDeleteService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Workflow Delete service
 */
@Slf4j
@Service
public class WorkflowDeleteServiceImpl implements WorkflowDeleteService {

    @Autowired
    private WorkflowProcessEntityMapper processEntityMapper;
    @Autowired
    private WorkflowTaskEntityMapper taskEntityMapper;
    @Autowired
    private WorkflowEventLogEntityMapper eventLogMapper;

    @Override
    public void deleteProcessAndTask(String groupId) {
        log.info("Delete all process and tasks by groupId:{}", groupId);
        ProcessQuery processQuery = new ProcessQuery();
        processQuery.setInlongGroupId(groupId);
        List<WorkflowProcessEntity> processEntities = processEntityMapper.selectByCondition(processQuery);
        List<Integer> processIds = processEntities.stream().map(entity -> entity.getId()).collect(Collectors.toList());
        processEntityMapper.deleteAll(processIds);
        taskEntityMapper.deleteAllByProcessIds(processIds);
        eventLogMapper.deleteAllByProcessIds(processIds);
        log.info("Finish delete all process and tasks by groupId:{}", groupId);
    }
}
