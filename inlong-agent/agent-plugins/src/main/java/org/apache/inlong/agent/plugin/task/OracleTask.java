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
import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.inlong.agent.constant.TaskConstants.*;

public class OracleTask extends AbstractTask {

    private static final Logger LOGGER = LoggerFactory.getLogger(OracleTask.class);

    public static final String DEFAULT_ORACLE_INSTANCE = "org.apache.inlong.agent.plugin.instance.OracleInstance";
    private AtomicBoolean isAdded = new AtomicBoolean(false);
    private final DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("yyyyMMddHH");

    private String dbName;
    private String tableName;
    private String instanceId;

    @Override
    protected int getInstanceLimit() {
        return DEFAULT_INSTANCE_LIMIT;
    }

    @Override
    public boolean isProfileValid(TaskProfile profile) {
        if (!profile.allRequiredKeyExist()) {
            LOGGER.error("task profile needs all required key");
            return false;
        }
        if (!profile.hasKey(TaskConstants.TASK_ORACLE_HOSTNAME)) {
            LOGGER.error("task profile needs hostname");
            return false;
        }
        if (!profile.hasKey(TaskConstants.TASK_ORACLE_PORT)) {
            LOGGER.error("task profile needs port");
            return false;
        }
        if (!profile.hasKey(TaskConstants.TASK_ORACLE_USER)) {
            LOGGER.error("task profile needs username");
            return false;
        }
        if (!profile.hasKey(TaskConstants.TASK_ORACLE_PASSWORD)) {
            LOGGER.error("task profile needs password");
            return false;
        }
        if (!profile.hasKey(TaskConstants.TASK_ORACLE_DBNAME)) {
            LOGGER.error("task profile needs DB name");
            return false;
        }
        if (!profile.hasKey(TaskConstants.TASK_ORACLE_SCHEMA_INCLUDE_LIST)) {
            LOGGER.error("task profile needs schema name");
            return false;
        }
        if (!profile.hasKey(TaskConstants.TASK_ORACLE_TABLE_INCLUDE_LIST)) {
            LOGGER.error("task profile needs table list");
            return false;
        }
        return true;
    }

    @Override
    protected void initTask() {
        LOGGER.info("oracle commonInit: {}", taskProfile.toJsonStr());
        dbName = taskProfile.get(TASK_ORACLE_DBNAME);
        tableName = taskProfile.get(TASK_ORACLE_TABLE_INCLUDE_LIST);
        instanceId = dbName + "-" + tableName;
    }

    @Override
    protected List<InstanceProfile> getNewInstanceList() {
        List<InstanceProfile> list = new ArrayList<>();
        if (isAdded.get()) {
            return list;
        }
        String dataTime = LocalDateTime.now().format(dateTimeFormatter);
        InstanceProfile instanceProfile =
                taskProfile.createInstanceProfile(DEFAULT_ORACLE_INSTANCE, instanceId,
                        CycleUnitType.HOUR, dataTime, AgentUtils.getCurrentTime());
        LOGGER.info("taskProfile.createInstanceProfile: {}", instanceProfile.toJsonStr());
        list.add(instanceProfile);
        this.isAdded.set(true);
        return list;
    }
}
