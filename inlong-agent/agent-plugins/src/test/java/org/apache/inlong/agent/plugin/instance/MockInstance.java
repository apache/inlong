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
import org.apache.inlong.agent.core.instance.ActionType;
import org.apache.inlong.agent.core.instance.InstanceAction;
import org.apache.inlong.agent.core.instance.InstanceManager;
import org.apache.inlong.agent.plugin.Instance;
import org.apache.inlong.agent.utils.AgentUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicLong;

public class MockInstance extends Instance {

    private static final Logger LOGGER = LoggerFactory.getLogger(MockInstance.class);
    public static final int INIT_TIME = 100;
    private InstanceProfile profile;
    private AtomicLong index = new AtomicLong(INIT_TIME);
    public long initTime = 0;
    public long destroyTime = 0;
    public long runtime = 0;
    private InstanceManager instanceManager;

    @Override
    public boolean init(Object instanceManager, InstanceProfile profile) {
        this.instanceManager = (InstanceManager) instanceManager;
        this.profile = profile;
        LOGGER.info("init called " + index);
        initTime = index.getAndAdd(1);
        return true;
    }

    @Override
    public void destroy() {
        LOGGER.info("destroy called " + index);
        destroyTime = index.getAndAdd(1);
    }

    @Override
    public void notifyDestroy() {

    }

    @Override
    public InstanceProfile getProfile() {
        return profile;
    }

    @Override
    public String getTaskId() {
        return profile.getTaskId();
    }

    @Override
    public String getInstanceId() {
        return profile.getInstanceId();
    }

    @Override
    public long getLastHeartbeatTime() {
        return AgentUtils.getCurrentTime();
    }

    @Override
    public void addCallbacks() {

    }

    @Override
    public void run() {
        LOGGER.info("run called " + index);
        runtime = index.getAndAdd(1);
    }

    public void sendFinishAction() {
        InstanceAction action = new InstanceAction();
        action.setActionType(ActionType.FINISH);
        action.setProfile(profile);
        instanceManager.submitAction(action);
    }
}