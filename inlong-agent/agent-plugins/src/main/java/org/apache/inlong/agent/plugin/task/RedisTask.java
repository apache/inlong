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

package org.apache.inlong.agent.plugin.task;

import org.apache.inlong.agent.conf.InstanceProfile;
import org.apache.inlong.agent.conf.TaskProfile;
import org.apache.inlong.agent.constant.CycleUnitType;
import org.apache.inlong.agent.constant.TaskConstants;
import org.apache.inlong.agent.utils.AgentUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;

public class RedisTask extends AbstractTask {

    private static final Logger LOGGER = LoggerFactory.getLogger(RedisTask.class);
    private boolean isAdded = false;
    private String taskId;

    private final DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("yyyyMMddHH");

    @Override
    public boolean isProfileValid(TaskProfile profile) {
        if (!profile.allRequiredKeyExist()) {
            LOGGER.error("task profile needs all required key");
            return false;
        }
        return true;
    }

    @Override
    protected int getInstanceLimit() {
        return DEFAULT_INSTANCE_LIMIT;
    }

    @Override
    protected void initTask() {
        LOGGER.info("Redis commonInit: {}", taskProfile.toJsonStr());
        this.taskId = taskProfile.get(TaskConstants.TASK_REDIS_REPLID)
                + "-" + taskProfile.get(TaskConstants.TASK_REDIS_IS_SUBSCRIBE);
    }

    @Override
    protected List<InstanceProfile> getNewInstanceList() {
        List<InstanceProfile> list = new ArrayList<>();
        if (isAdded) {
            return list;
        }
        String dataTime = LocalDateTime.now().format(dateTimeFormatter);
        InstanceProfile instanceProfile = taskProfile.createInstanceProfile(taskId, CycleUnitType.HOUR, dataTime,
                AgentUtils.getCurrentTime());
        LOGGER.info("taskProfile.createInstanceProfile: {}", instanceProfile.toJsonStr());
        list.add(instanceProfile);
        this.isAdded = true;
        return list;
    }
}
