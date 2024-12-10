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
import org.apache.inlong.agent.utils.AgentUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;

import static org.apache.inlong.agent.constant.TaskConstants.TASK_PULSAR_NAMESPACE;
import static org.apache.inlong.agent.constant.TaskConstants.TASK_PULSAR_TENANT;
import static org.apache.inlong.agent.constant.TaskConstants.TASK_PULSAR_TOPIC;

public class PulsarTask extends AbstractTask {

    private static final Logger LOGGER = LoggerFactory.getLogger(PulsarTask.class);
    private boolean isAdded = false;
    private String tenant;
    private String namespace;
    private String topic;
    private String instanceId;
    private final DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("yyyyMMddHH");

    @Override
    protected int getInstanceLimit() {
        return DEFAULT_INSTANCE_LIMIT;
    }

    @Override
    protected void initTask() {
        LOGGER.info("pulsar commonInit: {}", taskProfile.toJsonStr());
        this.tenant = taskProfile.get(TASK_PULSAR_TENANT);
        this.namespace = taskProfile.get(TASK_PULSAR_NAMESPACE);
        this.topic = taskProfile.get(TASK_PULSAR_TOPIC);
        this.instanceId = tenant + "/" + namespace + "/" + topic;
    }

    @Override
    public boolean isProfileValid(TaskProfile profile) {
        if (!profile.allRequiredKeyExist()) {
            LOGGER.error("task profile needs all required key");
            return false;
        }
        return true;
    }

    @Override
    protected List<InstanceProfile> getNewInstanceList() {
        List<InstanceProfile> list = new ArrayList<>();
        if (isAdded) {
            return list;
        }
        String dataTime = LocalDateTime.now().format(dateTimeFormatter);
        InstanceProfile instanceProfile = taskProfile.createInstanceProfile(instanceId, CycleUnitType.HOUR, dataTime,
                AgentUtils.getCurrentTime());
        LOGGER.info("taskProfile.createInstanceProfile: {}", instanceProfile.toJsonStr());
        list.add(instanceProfile);
        this.isAdded = true;
        return list;
    }
}