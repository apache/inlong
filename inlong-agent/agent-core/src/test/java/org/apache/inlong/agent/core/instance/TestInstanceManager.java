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
import org.apache.inlong.agent.conf.TaskProfile;
import org.apache.inlong.agent.core.AgentBaseTestsHelper;
import org.apache.inlong.agent.core.task.file.TaskManager;
import org.apache.inlong.agent.db.Db;
import org.apache.inlong.agent.utils.AgentUtils;
import org.apache.inlong.common.enums.InstanceStateEnum;
import org.apache.inlong.common.enums.TaskStateEnum;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

import static org.awaitility.Awaitility.await;

public class TestInstanceManager {

    private static final Logger LOGGER = LoggerFactory.getLogger(TestInstanceManager.class);
    private static InstanceManager manager;
    private static AgentBaseTestsHelper helper;
    private static TaskProfile taskProfile;

    @BeforeClass
    public static void setup() {
        helper = new AgentBaseTestsHelper(TestInstanceManager.class.getName()).setupAgentHome();
        String pattern = helper.getTestRootDir() + "/YYYYMMDD_[0-9]+.txt";
        Db basicDb = TaskManager.initDb("/localdb");
        taskProfile = helper.getTaskProfile(1, pattern, false, 0L, 0L, TaskStateEnum.RUNNING);
        manager = new InstanceManager("1", basicDb);
        manager.start();
    }

    @AfterClass
    public static void teardown() {
        manager.stop();
        helper.teardownAgentHome();
    }

    @Test
    public void testInstanceManager() {
        long timeBefore = AgentUtils.getCurrentTime();
        InstanceProfile profile = taskProfile.createInstanceProfile(MockInstance.class.getCanonicalName(),
                helper.getTestRootDir() + "/20230927_1.txt", "20230927");
        String instanceId = profile.getInstanceId();
        InstanceAction action = new InstanceAction();
        action.setActionType(ActionType.ADD);
        action.setProfile(profile);
        // test add action
        manager.submitAction(action);
        await().atMost(1, TimeUnit.SECONDS).until(() -> manager.getInstance(instanceId) != null);
        Assert.assertTrue(manager.getInstanceProfile(instanceId).getState() == InstanceStateEnum.DEFAULT);

        // test finish action
        MockInstance instance = (MockInstance) manager.getInstance(profile.getInstanceId());
        instance.sendFinishAction();
        await().atMost(1, TimeUnit.SECONDS).until(() -> manager.getInstance(instanceId) == null);
        Assert.assertTrue(manager.getInstanceProfile(instanceId).getState() == InstanceStateEnum.FINISHED);
        // test modify before finish
        Assert.assertFalse(manager.shouldAddAgain(profile.getInstanceId(), timeBefore));
        // test modify after finish
        Assert.assertTrue(manager.shouldAddAgain(profile.getInstanceId(), AgentUtils.getCurrentTime()));

        // test continue
        action.setActionType(ActionType.ADD);
        profile.setState(InstanceStateEnum.DEFAULT);
        manager.submitAction(action);
        await().atMost(1, TimeUnit.SECONDS).until(() -> manager.getInstance(instanceId) != null);
        Assert.assertTrue(manager.getInstanceProfile(instanceId).getState() == InstanceStateEnum.DEFAULT);

        // test delete action
        action.setActionType(ActionType.DELETE);
        manager.submitAction(action);
        await().atMost(1, TimeUnit.SECONDS).until(() -> manager.getInstanceProfile(instanceId) == null);
        Assert.assertTrue(String.valueOf(instance.initTime), instance.initTime == MockInstance.INIT_TIME);
        Assert.assertTrue(String.valueOf(instance.runtime), instance.runtime == MockInstance.RUN_TIME);
        Assert.assertTrue(String.valueOf(instance.destroyTime), instance.destroyTime == MockInstance.DESTROY_TIME);
    }
}
