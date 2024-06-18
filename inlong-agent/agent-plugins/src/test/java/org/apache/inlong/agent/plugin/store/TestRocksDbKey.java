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

package org.apache.inlong.agent.plugin.store;

import org.apache.inlong.agent.constant.AgentConstants;
import org.apache.inlong.agent.core.task.TaskManager;
import org.apache.inlong.agent.plugin.AgentBaseTestsHelper;
import org.apache.inlong.agent.store.InstanceStore;
import org.apache.inlong.agent.store.OffsetStore;
import org.apache.inlong.agent.store.TaskStore;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class TestRocksDbKey {

    private static final Logger LOGGER = LoggerFactory.getLogger(TestRocksDbKey.class);
    private static TaskStore taskStore;
    private static InstanceStore instanceStore;
    private static OffsetStore offsetStore;

    private static AgentBaseTestsHelper helper;

    @BeforeClass
    public static void setup() throws Exception {
        helper = new AgentBaseTestsHelper(TestRocksDbKey.class.getName()).setupAgentHome();
        taskStore = new TaskStore(TaskManager.initStore(AgentConstants.AGENT_STORE_PATH_TASK));
        instanceStore = new InstanceStore(TaskManager.initStore(AgentConstants.AGENT_STORE_PATH_INSTANCE));
        offsetStore = new OffsetStore(TaskManager.initStore(AgentConstants.AGENT_STORE_PATH_OFFSET));

    }

    @AfterClass
    public static void teardown() throws IOException {
        helper.teardownAgentHome();
    }

    @Test
    public void testStore() {
        Assert.assertEquals(0, taskStore.getKey().compareTo("task"));
        Assert.assertEquals(0, taskStore.getKeyByTaskId("1").compareTo("task_1"));
        Assert.assertEquals(0, instanceStore.getKey().compareTo("ins_"));
        Assert.assertEquals(0, instanceStore.getKeyByTaskId("1").compareTo("ins_1"));
        Assert.assertEquals(0, instanceStore.getKeyByTaskAndInstanceId("1", "/data/log/123.log")
                .compareTo("ins_1_/data/log/123.log"));
        Assert.assertEquals(0, offsetStore.getKey("1", "/data/log/123.log").compareTo("offset_1_/data/log/123.log"));
    }
}
