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

package org.apache.inlong.agent.plugin.instance;

import org.apache.inlong.agent.conf.InstanceProfile;
import org.apache.inlong.agent.constant.TaskConstants;
import org.apache.inlong.agent.core.instance.ActionType;
import org.apache.inlong.agent.core.instance.InstanceAction;
import org.apache.inlong.agent.core.instance.InstanceManager;
import org.apache.inlong.agent.core.task.OffsetManager;
import org.apache.inlong.agent.plugin.Instance;
import org.apache.inlong.agent.plugin.Message;
import org.apache.inlong.agent.plugin.file.Sink;
import org.apache.inlong.agent.plugin.file.Source;
import org.apache.inlong.agent.plugin.utils.file.FileDataUtils;
import org.apache.inlong.agent.state.State;
import org.apache.inlong.agent.utils.AgentUtils;
import org.apache.inlong.agent.utils.ThreadUtils;
import org.apache.inlong.common.enums.InstanceStateEnum;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * file instance contains source and sink.
 * main job is to read from source and write to sink
 */
public class FileInstance extends Instance {

    private static final Logger LOGGER = LoggerFactory.getLogger(FileInstance.class);
    private Source source;
    private Sink sink;
    private InstanceProfile profile;
    public static final int CORE_THREAD_SLEEP_TIME = 1;
    private static final int DESTROY_LOOP_WAIT_TIME_MS = 10;
    private InstanceManager instanceManager;
    private volatile boolean running = false;
    private volatile boolean inited = false;

    @Override
    public void init(Object srcManager, InstanceProfile srcProfile) {
        try {
            instanceManager = (InstanceManager) srcManager;
            profile = srcProfile;
            profile.set(TaskConstants.INODE_INFO, FileDataUtils.getInodeInfo(profile.getInstanceId()));
            LOGGER.info("task id: {} submit new instance {} profile detail {}.", profile.getTaskId(),
                    profile.getInstanceId(), profile.toJsonStr());
            source = (Source) Class.forName(profile.getSourceClass()).newInstance();
            source.init(profile);
            sink = (Sink) Class.forName(profile.getSinkClass()).newInstance();
            sink.init(profile);
            inited = true;
        } catch (Throwable ex) {
            doChangeState(State.FATAL);
            LOGGER.error("init instance {} for task {} failed", profile.getInstanceId(), profile.getInstanceId(),
                    ex);
            ThreadUtils.threadThrowableHandler(Thread.currentThread(), ex);
        }
    }

    @Override
    public void destroy() {
        if (!inited) {
            return;
        }
        doChangeState(State.SUCCEEDED);
        while (running) {
            AgentUtils.silenceSleepInMs(DESTROY_LOOP_WAIT_TIME_MS);
        }
        this.source.destroy();
        this.sink.destroy();
    }

    @Override
    public void run() {
        Thread.currentThread().setName("file-instance-core-" + getTaskId() + "-" + getInstanceId());
        running = true;
        while (!isFinished()) {
            if (!source.sourceExist()) {
                if (profile.isRetry()) {
                    handleReadEnd();
                } else {
                    handleSourceDeleted();
                }
                handleSourceDeleted();
                break;
            }
            Message msg = source.read();
            if (msg == null) {
                if (source.sourceFinish() && sink.sinkFinish()) {
                    handleReadEnd();
                    break;
                } else {
                    AgentUtils.silenceSleepInSeconds(CORE_THREAD_SLEEP_TIME);
                }
            } else {
                sink.write(msg);
            }
        }
        running = false;
    }

    private void handleReadEnd() {
        InstanceAction action = new InstanceAction(ActionType.FINISH, profile);
        while (!instanceManager.submitAction(action)) {
            LOGGER.error("instance manager action queue is full: taskId {}",
                    instanceManager.getTaskId());
            AgentUtils.silenceSleepInSeconds(CORE_THREAD_SLEEP_TIME);
        }
    }

    private void handleSourceDeleted() {
        OffsetManager.init().deleteOffset(getTaskId(), getInstanceId());
        profile.setState(InstanceStateEnum.DELETE);
        profile.setModifyTime(AgentUtils.getCurrentTime());
        InstanceAction action = new InstanceAction(ActionType.DELETE, profile);
        while (!instanceManager.submitAction(action)) {
            LOGGER.error("instance manager action queue is full: taskId {}",
                    instanceManager.getTaskId());
            AgentUtils.silenceSleepInSeconds(CORE_THREAD_SLEEP_TIME);
        }
    }

    @Override
    public void addCallbacks() {

    }

    @Override
    public String getTaskId() {
        return profile.getTaskId();
    }

    @Override
    public String getInstanceId() {
        return profile.getInstanceId();
    }

    public Sink getSink() {
        return sink;
    }

    public InstanceProfile getProfile() {
        return profile;
    }
}
