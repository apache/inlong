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

import static org.apache.inlong.agent.constant.TaskConstants.TASK_POSTGRES_DBNAME;
import static org.apache.inlong.agent.constant.TaskConstants.TASK_POSTGRES_HOSTNAME;
import static org.apache.inlong.agent.constant.TaskConstants.TASK_POSTGRES_PASSWORD;
import static org.apache.inlong.agent.constant.TaskConstants.TASK_POSTGRES_PLUGIN_NAME;
import static org.apache.inlong.agent.constant.TaskConstants.TASK_POSTGRES_PORT;
import static org.apache.inlong.agent.constant.TaskConstants.TASK_POSTGRES_TABLE_INCLUDE_LIST;
import static org.apache.inlong.agent.constant.TaskConstants.TASK_POSTGRES_USER;

public class PostgreSQLTask extends AbstractTask {

    private static final Logger LOGGER = LoggerFactory.getLogger(PostgreSQLTask.class);
    private final DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("yyyyMMddHH");
    private boolean isAdded = false;
    public static final int DEFAULT_INSTANCE_LIMIT = 1;

    private String dbName;
    private String tableName;
    private String instanceId;

    @Override
    protected void initTask() {
        LOGGER.info("postgres commonInit: {}", taskProfile.toJsonStr());
        dbName = taskProfile.get(TASK_POSTGRES_DBNAME);
        tableName = taskProfile.get(TASK_POSTGRES_TABLE_INCLUDE_LIST);
        instanceId = dbName + "-" + tableName;
    }

    @Override
    public boolean isProfileValid(TaskProfile profile) {
        if (!profile.allRequiredKeyExist()) {
            LOGGER.error("task profile needs all required key");
            return false;
        }
        if (!profile.hasKey(profile.get(TASK_POSTGRES_HOSTNAME))) {
            LOGGER.error("task profile needs hostname");
            return false;
        }
        if (!profile.hasKey(profile.get(TASK_POSTGRES_PORT))) {
            LOGGER.error("task profile needs port");
            return false;
        }
        if (!profile.hasKey(profile.get(TASK_POSTGRES_USER))) {
            LOGGER.error("task profile needs username");
            return false;
        }
        if (!profile.hasKey(profile.get(TASK_POSTGRES_PASSWORD))) {
            LOGGER.error("task profile needs password");
            return false;
        }
        if (!profile.hasKey(profile.get(TASK_POSTGRES_DBNAME))) {
            LOGGER.error("task profile needs DB name");
            return false;
        }
        if (!profile.hasKey(profile.get(TASK_POSTGRES_PLUGIN_NAME))) {
            LOGGER.error("task profile needs plugin name");
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
        list.add(instanceProfile);
        this.isAdded = true;
        return list;
    }

    @Override
    protected int getInstanceLimit() {
        return DEFAULT_INSTANCE_LIMIT;
    }
}
