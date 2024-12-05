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
import org.apache.inlong.agent.conf.TaskProfile;
import org.apache.inlong.agent.constant.AgentConstants;
import org.apache.inlong.agent.constant.CycleUnitType;
import org.apache.inlong.agent.core.instance.ActionType;
import org.apache.inlong.agent.core.instance.InstanceAction;
import org.apache.inlong.agent.core.instance.InstanceManager;
import org.apache.inlong.agent.core.task.TaskManager;
import org.apache.inlong.agent.plugin.AgentBaseTestsHelper;
import org.apache.inlong.agent.store.InstanceStore;
import org.apache.inlong.agent.store.Store;
import org.apache.inlong.agent.store.TaskStore;
import org.apache.inlong.agent.utils.AgentUtils;
import org.apache.inlong.agent.utils.DateTransUtils;
import org.apache.inlong.common.enums.InstanceStateEnum;
import org.apache.inlong.common.enums.TaskStateEnum;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.ParseException;
import java.util.TimeZone;
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
        String pattern = helper.getTestRootDir() + "/YYYYMMDDhh_[0-9]+.txt";
        Store basicInstanceStore = TaskManager.initStore(AgentConstants.AGENT_STORE_PATH_INSTANCE);
        taskProfile =
                helper.getFileTaskProfile(1, pattern, "csv", false, "", "", TaskStateEnum.RUNNING, CycleUnitType.HOUR,
                        "GMT+6:00", null);
        Store taskBasicStore = TaskManager.initStore(AgentConstants.AGENT_STORE_PATH_TASK);
        TaskStore taskStore = new TaskStore(taskBasicStore);
        taskStore.storeTask(taskProfile);
        manager = new InstanceManager("1", 20, basicInstanceStore, taskStore);
        manager.CORE_THREAD_SLEEP_TIME_MS = 100;
    }

    @AfterClass
    public static void teardown() {
        manager.stop();
        helper.teardownAgentHome();
    }

    @Test
    public void testInstanceManager() {
        InstanceStore instanceStore = manager.getInstanceStore();
        for (int i = 1; i <= 10; i++) {
            InstanceProfile profile = taskProfile.createInstanceProfile(MockInstance.class.getCanonicalName(),
                    String.valueOf(i), taskProfile.getCycleUnit(), "2023092710",
                    AgentUtils.getCurrentTime());
            instanceStore.storeInstance(profile);
        }
        manager.start();
        for (int i = 1; i <= 10; i++) {
            String instanceId = String.valueOf(i);
            await().atMost(1, TimeUnit.SECONDS).until(() -> manager.getInstance(instanceId) != null);
            Assert.assertTrue(manager.getInstanceProfile(instanceId).getState() == InstanceStateEnum.DEFAULT);
        }
        long timeBefore = AgentUtils.getCurrentTime();
        InstanceProfile profile = taskProfile.createInstanceProfile(MockInstance.class.getCanonicalName(),
                helper.getTestRootDir() + "/2023092710_1.txt", taskProfile.getCycleUnit(), "2023092710",
                AgentUtils.getCurrentTime());
        String sinkDataTime = String.valueOf(profile.getSinkDataTime());
        try {
            String add2TimeZone = String.valueOf(
                    DateTransUtils.timeStrConvertToMillSec("2023092712", "h", TimeZone.getTimeZone("GMT+8:00")));
            Assert.assertTrue(sinkDataTime, sinkDataTime.equals(add2TimeZone));
        } catch (ParseException e) {
            LOGGER.error("testInstanceManager error: ", e);
        }
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
        profile = taskProfile.createInstanceProfile(MockInstance.class.getCanonicalName(),
                helper.getTestRootDir() + "/2023092710_1.txt", taskProfile.getCycleUnit(), "2023092710",
                AgentUtils.getCurrentTime());
        action = new InstanceAction();
        action.setActionType(ActionType.ADD);
        action.setProfile(profile);
        manager.submitAction(action);
        await().atMost(1, TimeUnit.SECONDS).until(() -> manager.getInstance(instanceId) != null);
        Assert.assertTrue(manager.getInstanceProfile(instanceId).getState() == InstanceStateEnum.DEFAULT);

        // test delete action
        action.setActionType(ActionType.DELETE);
        manager.submitAction(action);
        await().atMost(1, TimeUnit.SECONDS).until(() -> manager.getInstanceProfile(instanceId) == null);
        Assert.assertTrue(String.valueOf(instance.initTime), instance.initTime == MockInstance.INIT_TIME);
        Assert.assertTrue(instance.runtime > instance.initTime);
    }
}
