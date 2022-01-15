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

package org.apache.inlong.manager.service.core.impl;

import lombok.extern.slf4j.Slf4j;
import org.apache.inlong.manager.dao.entity.TaskSinkParamsPulsarEntity;
import org.apache.inlong.manager.dao.mapper.TaskSinkParamsPulsarEntityMapper;
import org.apache.inlong.manager.service.core.TaskSinkParamsPulsarService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

/**
 * Implementation of task config sink params pulsar service layer interface.
 */
@Service
@Slf4j
public class TaskSinkParamsPulsarServiceImpl implements TaskSinkParamsPulsarService {

    private static final Logger LOGGER = LoggerFactory.getLogger(TaskSinkParamsPulsarServiceImpl.class);

    @Autowired
    private TaskSinkParamsPulsarEntityMapper sinkParamsPulsarEntityMapper;

    @Override
    public TaskSinkParamsPulsarEntity selectByTaskName(String taskName) {
        LOGGER.info("Get sink params pulsar config by task: {}", taskName);
        return sinkParamsPulsarEntityMapper.selectByTaskName(taskName);
    }
}
