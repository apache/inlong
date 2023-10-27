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

package org.apache.inlong.agent.core.instance;

import org.apache.inlong.agent.conf.InstanceProfile;
import org.apache.inlong.agent.plugin.Instance;

public class MockInstance extends Instance {

    public static final int INIT_TIME = 100;
    public static final int RUN_TIME = 101;
    public static final int DESTROY_TIME = 102;
    private InstanceProfile profile;
    private long index = INIT_TIME;
    public long initTime = 0;
    public long destroyTime = 0;
    public long runtime = 0;
    private InstanceManager instanceManager;

    @Override
    public void init(Object instanceManager, InstanceProfile profile) {
        this.instanceManager = (InstanceManager) instanceManager;
        this.profile = profile;
        initTime = index++;
    }

    @Override
    public void destroy() {
        destroyTime = index++;
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
    public void addCallbacks() {

    }

    @Override
    public void run() {
        runtime = index++;
    }

    public void sendFinishAction() {
        InstanceAction action = new InstanceAction();
        action.setActionType(ActionType.FINISH);
        action.setProfile(profile);
        instanceManager.submitAction(action);
    }
}