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
import org.apache.inlong.agent.core.instance.ActionType;
import org.apache.inlong.agent.core.instance.InstanceAction;
import org.apache.inlong.agent.core.instance.InstanceManager;
import org.apache.inlong.agent.core.task.TaskManager;
import org.apache.inlong.agent.db.Db;
import org.apache.inlong.agent.metrics.audit.AuditUtils;
import org.apache.inlong.agent.plugin.file.Task;
import org.apache.inlong.agent.state.State;
import org.apache.inlong.agent.utils.AgentUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

import static org.apache.inlong.agent.constant.TaskConstants.RESTORE_FROM_DB;

public class MongoDBTask extends Task {

    private static final Logger LOGGER = LoggerFactory.getLogger(MongoDBTask.class);
    public static final String DEFAULT_MONGODB_INSTANCE = "org.apache.inlong.agent.plugin.instance.MongoDBInstance";
    public static final int CORE_THREAD_SLEEP_TIME = 5000;
    public static final int CORE_THREAD_PRINT_TIME = 10000;

    private TaskProfile taskProfile;
    private Db basicDb;
    private TaskManager taskManager;
    private InstanceManager instanceManager;
    private long lastPrintTime = 0;
    private boolean initOK = false;
    private volatile boolean running = false;
    private boolean isAdded = false;
    private boolean isRestoreFromDB = false;

    private String database;
    private String collection;

    @Override
    public void init(Object srcManager, TaskProfile taskProfile, Db basicDb) throws IOException {
        taskManager = (TaskManager) srcManager;
        commonInit(taskProfile, basicDb);
        initOK = true;
    }

    private void commonInit(TaskProfile taskProfile, Db basicDb) {
        LOGGER.info("mongoDB commonInit: {}", taskProfile.toJsonStr());
        this.taskProfile = taskProfile;
        this.basicDb = basicDb;
        this.database = taskProfile.get(TaskConstants.TASK_MONGO_DATABASE_INCLUDE_LIST);
        this.collection = taskProfile.get(TaskConstants.TASK_MONGO_COLLECTION_INCLUDE_LIST);
        this.isRestoreFromDB = taskProfile.getBoolean(RESTORE_FROM_DB, false);
        instanceManager = new InstanceManager(taskProfile.getTaskId(), 1,
                basicDb, taskManager.getTaskDb());
        try {
            instanceManager.start();
        } catch (Exception e) {
            LOGGER.error("start instance manager error: ", e);
        }
    }

    @Override
    public void destroy() {
        doChangeState(State.SUCCEEDED);
        if (instanceManager != null) {
            instanceManager.stop();
        }
    }

    @Override
    public TaskProfile getProfile() {
        return taskProfile;
    }

    @Override
    public String getTaskId() {
        if (taskProfile == null) {
            return "";
        }
        return taskProfile.getTaskId();
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
    public void addCallbacks() {

    }

    @Override
    public void run() {
        Thread.currentThread().setName("mongoDB-task-core-" + getTaskId());
        running = true;
        try {
            doRun();
        } catch (Throwable e) {
            LOGGER.error("do run error: ", e);
        }
        running = false;
    }

    private void doRun() {
        while (!isFinished()) {
            if (AgentUtils.getCurrentTime() - lastPrintTime > CORE_THREAD_PRINT_TIME) {
                LOGGER.info("mongoDB task running! taskId {}", getTaskId());
                lastPrintTime = AgentUtils.getCurrentTime();
            }
            AgentUtils.silenceSleepInMs(CORE_THREAD_SLEEP_TIME);
            if (!initOK) {
                continue;
            }

            // Add instance profile to instance manager
            addInstanceProfile();

            String inlongGroupId = taskProfile.getInlongGroupId();
            String inlongStreamId = taskProfile.getInlongStreamId();
            AuditUtils.add(AuditUtils.AUDIT_ID_AGENT_TASK_HEARTBEAT, inlongGroupId, inlongStreamId,
                    AgentUtils.getCurrentTime(), 1, 1);
        }
    }

    private void addInstanceProfile() {
        if (isAdded) {
            return;
        }
        String dataTime = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMddHH"));
        InstanceProfile instanceProfile = taskProfile.createInstanceProfile(DEFAULT_MONGODB_INSTANCE, collection,
                CycleUnitType.HOUR, dataTime, AgentUtils.getCurrentTime());
        LOGGER.info("taskProfile.createInstanceProfile: {}", instanceProfile.toJsonStr());
        InstanceAction action = new InstanceAction(ActionType.ADD, instanceProfile);
        while (!isFinished() && !instanceManager.submitAction(action)) {
            LOGGER.error("instance manager action queue is full: taskId {}", instanceManager.getTaskId());
            AgentUtils.silenceSleepInMs(CORE_THREAD_SLEEP_TIME);
        }
        this.isAdded = true;
    }
}
