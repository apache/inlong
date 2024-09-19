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
import org.apache.inlong.agent.core.instance.ActionType;
import org.apache.inlong.agent.core.instance.InstanceAction;
import org.apache.inlong.agent.core.instance.InstanceManager;
import org.apache.inlong.agent.core.task.TaskManager;
import org.apache.inlong.agent.metrics.audit.AuditUtils;
import org.apache.inlong.agent.plugin.file.Task;
import org.apache.inlong.agent.state.State;
import org.apache.inlong.agent.store.Store;
import org.apache.inlong.agent.utils.AgentUtils;
import org.apache.inlong.agent.utils.ThreadUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

import static org.apache.inlong.agent.constant.TaskConstants.TASK_AUDIT_VERSION;

public abstract class AbstractTask extends Task {

    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractTask.class);
    public static final int CORE_THREAD_SLEEP_TIME = 1000;
    public static final int CORE_THREAD_PRINT_TIME = 10000;
    protected static final int DEFAULT_INSTANCE_LIMIT = 1;
    protected TaskProfile taskProfile;
    protected Store basicStore;
    protected TaskManager taskManager;
    private InstanceManager instanceManager;
    protected volatile boolean running = false;
    protected boolean initOK = false;
    protected long lastPrintTime = 0;
    protected long auditVersion;
    protected long instanceCount = 0;

    @Override
    public void init(Object srcManager, TaskProfile taskProfile, Store basicStore) throws IOException {
        taskManager = (TaskManager) srcManager;
        this.taskProfile = taskProfile;
        this.basicStore = basicStore;
        auditVersion = Long.parseLong(taskProfile.get(TASK_AUDIT_VERSION));
        instanceManager = new InstanceManager(taskProfile.getTaskId(), getInstanceLimit(),
                basicStore, taskManager.getTaskStore());
        try {
            instanceManager.start();
        } catch (Exception e) {
            LOGGER.error("start instance manager error: ", e);
        }
        initTask();
        initOK = true;
    }

    protected abstract int getInstanceLimit();

    protected abstract void initTask();

    protected void releaseTask() {
    }

    @Override
    public void destroy() {
        doChangeState(State.SUCCEEDED);
        if (instanceManager != null) {
            instanceManager.stop();
        }
        releaseTask();
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
    public void addCallbacks() {

    }

    @Override
    public void run() {
        Thread.currentThread().setName("task-core-" + getTaskId());
        running = true;
        try {
            doRun();
        } catch (Throwable e) {
            LOGGER.error("do run error: ", e);
            ThreadUtils.threadThrowableHandler(Thread.currentThread(), e);
        }
        running = false;
    }

    protected void doRun() {
        while (!isFinished()) {
            taskPrint();
            AgentUtils.silenceSleepInMs(CORE_THREAD_SLEEP_TIME);
            if (!initOK) {
                continue;
            }
            List<InstanceProfile> profileList = getNewInstanceList();
            for (InstanceProfile profile : profileList) {
                InstanceAction action = new InstanceAction(ActionType.ADD, profile);
                while (!isFinished() && !instanceManager.submitAction(action)) {
                    LOGGER.error("instance manager action queue is full: taskId {}", getTaskId());
                    AgentUtils.silenceSleepInMs(CORE_THREAD_SLEEP_TIME);
                }
            }
            taskHeartbeat();
        }
    }

    protected abstract List<InstanceProfile> getNewInstanceList();

    protected void taskHeartbeat() {
        AuditUtils.add(AuditUtils.AUDIT_ID_AGENT_TASK_HEARTBEAT, taskProfile.getInlongGroupId(),
                taskProfile.getInlongStreamId(), AgentUtils.getCurrentTime(), 1, 1, auditVersion);

    }

    protected void taskPrint() {
        if (AgentUtils.getCurrentTime() - lastPrintTime > CORE_THREAD_PRINT_TIME) {
            LOGGER.info("task running! taskId {}", getTaskId());
            lastPrintTime = AgentUtils.getCurrentTime();
        }
    }

    protected boolean allInstanceFinished() {
        return instanceCount == instanceManager.getFinishedInstanceCount();
    }

    protected boolean shouldAddAgain(String fileName, long lastModifyTime) {
        return instanceManager.shouldAddAgain(fileName, lastModifyTime);
    }

    protected boolean isFull() {
        return instanceManager.isFull();
    }
}
