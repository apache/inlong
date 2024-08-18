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

package org.apache.inlong.agent.plugin.sources;

import org.apache.inlong.agent.conf.InstanceProfile;
import org.apache.inlong.agent.conf.TaskProfile;
import org.apache.inlong.agent.constant.AgentConstants;
import org.apache.inlong.agent.constant.CommonConstants;
import org.apache.inlong.agent.constant.TaskConstants;
import org.apache.inlong.agent.core.task.OffsetManager;
import org.apache.inlong.agent.core.task.TaskManager;
import org.apache.inlong.agent.plugin.AgentBaseTestsHelper;
import org.apache.inlong.agent.store.Store;
import org.apache.inlong.agent.utils.AgentUtils;
import org.apache.inlong.common.enums.TaskStateEnum;

import org.junit.Before;
import org.junit.Test;

import java.lang.reflect.Method;
import java.util.List;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Test cases for {@link RedisSource}.
 */
public class TestRedisSource {

    private RedisSource redisSource;

    private static AgentBaseTestsHelper helper;

    // task basic store
    private static Store taskBasicStore;
    // instance basic store
    private static Store instanceBasicStore;
    // offset basic store
    private static Store offsetBasicStore;

    InstanceProfile instanceProfile;

    private final String instanceId = "s4bc475560b4444dbd4e9812ab1fd64d";

    @Before
    public void setUp() throws Exception {
        helper = new AgentBaseTestsHelper(TestRedisSource.class.getName()).setupAgentHome();
        taskBasicStore = TaskManager.initStore(AgentConstants.AGENT_STORE_PATH_TASK);
        instanceBasicStore = TaskManager.initStore(AgentConstants.AGENT_STORE_PATH_INSTANCE);
        offsetBasicStore =
                TaskManager.initStore(AgentConstants.AGENT_STORE_PATH_OFFSET);
        OffsetManager.init(taskBasicStore, instanceBasicStore, offsetBasicStore);
        initSource();
    }
    // for local test
    public RedisSource initSource() {
        final String username = "";
        final String password = "123456";
        final String hostname = "127.0.0.1";
        final String port = "6379";
        final String groupId = "group01";
        final String streamId = "stream01";

        TaskProfile taskProfile = helper.getTaskProfile(1, "", false, 0L, 0L, TaskStateEnum.RUNNING, "D",
                "GMT+8:00");
        instanceProfile = taskProfile.createInstanceProfile("",
                "", taskProfile.getCycleUnit(), "20240725", AgentUtils.getCurrentTime());
        instanceProfile.set(CommonConstants.PROXY_INLONG_GROUP_ID, groupId);
        instanceProfile.set(CommonConstants.PROXY_INLONG_STREAM_ID, streamId);
        instanceProfile.set(TaskConstants.TASK_REDIS_AUTHUSER, username);
        instanceProfile.set(TaskConstants.TASK_REDIS_AUTHPASSWORD, password);
        instanceProfile.set(TaskConstants.TASK_REDIS_HOSTNAME, hostname);
        instanceProfile.set(TaskConstants.TASK_REDIS_PORT, port);
        instanceProfile.set(TaskConstants.TASK_AUDIT_VERSION, "0");
        instanceProfile.setInstanceId(instanceId);

        (redisSource = new RedisSource()).init(instanceProfile);
        return redisSource;
    }

    @Test
    public void testRedisSource() throws Exception {
        testReadDataFromSourceSuccess();
        TestReadEmptyFromSource();
    }

    // test read
    public void testReadDataFromSourceSuccess() throws Exception {
        Method handleConsumerEvent = RedisSource.class.getDeclaredMethod("readFromSource");
        handleConsumerEvent.setAccessible(true);

        List result = (List) handleConsumerEvent.invoke(redisSource);
        assertFalse(result.isEmpty());
    }

    // test read
    private void TestReadEmptyFromSource() throws Exception {
        Method handleConsumerEvent = RedisSource.class.getDeclaredMethod("readFromSource");
        handleConsumerEvent.setAccessible(true);
        List result = (List) handleConsumerEvent.invoke(redisSource);
        assertTrue(result.isEmpty());
    }
}
