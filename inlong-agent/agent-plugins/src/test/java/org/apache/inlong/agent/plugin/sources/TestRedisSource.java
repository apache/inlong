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
import org.apache.inlong.common.metric.MetricRegister;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import redis.clients.jedis.exceptions.JedisConnectionException;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.junit.Assert.*;
import static org.powermock.api.mockito.PowerMockito.mockStatic;
import static org.powermock.api.mockito.PowerMockito.when;

/**
 * Test cases for {@link RedisSource}.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({Executors.class, RedisSource.class, MetricRegister.class})
@PowerMockIgnore({"javax.management.*"})
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
    private BlockingQueue queue;

    @Mock
    private ExecutorService executorService;

    private final String instanceId = "s4bc475560b4444dbd4e9812ab1fd64d";

    @Before
    public void setUp() throws Exception {
        helper = new AgentBaseTestsHelper(TestRedisSource.class.getName()).setupAgentHome();
        taskBasicStore = TaskManager.initStore(AgentConstants.AGENT_STORE_PATH_TASK);
        instanceBasicStore = TaskManager.initStore(AgentConstants.AGENT_STORE_PATH_INSTANCE);
        offsetBasicStore =
                TaskManager.initStore(AgentConstants.AGENT_STORE_PATH_OFFSET);
        OffsetManager.init(taskBasicStore, instanceBasicStore, offsetBasicStore);
        mockStatic(Executors.class);
        when(Executors.newSingleThreadExecutor()).thenReturn(executorService);
        initSource();
        Field field = RedisSource.class.getDeclaredField("redisQueue");
        field.setAccessible(true);
        queue = (BlockingQueue) field.get(redisSource);
    }
    // init source
    public RedisSource initSource() {
        final String username = "";
        final String password = "123456";
        final String hostname = "127.0.0.1";
        final String port = "6379";
        final String groupId = "group01";
        final String streamId = "stream01";
        final String keys = "age,name,sex";
        final String command = "zscore";

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
        instanceProfile.set(TaskConstants.TASK_REDIS_COMMAND, command);
        instanceProfile.set(TaskConstants.TASK_REDIS_KEYS, keys);
        instanceProfile.set(TaskConstants.TASK_AUDIT_VERSION, "0");
        instanceProfile.setInstanceId(instanceId);
        redisSource = new RedisSource();
        try {
            redisSource.init(instanceProfile);
        } catch (JedisConnectionException ignored) {
        }
        return redisSource;
    }

    // test read
    @Test
    public void testReadDataFromSource() throws Exception {
        Method handleConsumerEvent = RedisSource.class.getDeclaredMethod("readFromSource");
        handleConsumerEvent.setAccessible(true);

        List result = (List) handleConsumerEvent.invoke(redisSource);
        if (queue.isEmpty()) {
            assertTrue(result.isEmpty());
        } else {
            assertFalse(result.isEmpty());
        }
        queue.clear();
        assertTrue(queue.isEmpty());
    }

}
