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
import org.apache.inlong.manager.dao.entity.TaskIdParamsKafkaEntity;
import org.apache.inlong.manager.dao.mapper.TaskIdParamsKafkaEntityMapper;
import org.apache.inlong.manager.service.core.TaskIdParamsKafkaService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.Map;

/**
 * Implementation of task config id params kafka service layer interface.
 */
@Service
@Slf4j
public class TaskIdParamsKafkaServiceImpl implements TaskIdParamsKafkaService {

    private static final Logger LOGGER = LoggerFactory.getLogger(TaskIdParamsKafkaServiceImpl.class);

    @Autowired
    private TaskIdParamsKafkaEntityMapper idParamsKafkaEntityMapper;

    @Override
    public Map<String, String> selectByTaskName(String taskName) {
        LOGGER.info("Get id params kafka config by task: {}", taskName);
        Map<String, String> idParams = idParamsKafkaEntityMapper.selectByTaskName(taskName);
        if (idParams == null || idParams.isEmpty()) {
            return null;
        }
        idParams.put("type", "kafka");
        return idParams;
    }
}
