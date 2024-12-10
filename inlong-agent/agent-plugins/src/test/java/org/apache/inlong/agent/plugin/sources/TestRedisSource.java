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
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.powermock.api.mockito.PowerMockito.mockStatic;
import static org.powermock.api.mockito.PowerMockito.when;

/**
 * Test cases for {@link RedisSource}.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({Executors.class, RedisSource.class, MetricRegister.class})
@PowerMockIgnore({"javax.management.*"})
public class TestRedisSource {

    private static final Logger LOGGER = LoggerFactory.getLogger(TestRedisSource.class);

    private static AgentBaseTestsHelper helper;

    private final String instanceId = "s4bc475560b4444dbd4e9812ab1fd64d";

    private static Store taskBasicStore;
    private static Store instanceBasicStore;
    private static Store offsetBasicStore;

    @Mock
    private InstanceProfile profile;

    @Mock
    private Jedis jedis;

    @Mock
    private Pipeline pipeline;

    @Mock
    private ScheduledExecutorService executor;

    @InjectMocks
    private RedisSource redisSource;

    @Before
    public void setUp() {
        helper = new AgentBaseTestsHelper(UUID.randomUUID().toString()).setupAgentHome();
        taskBasicStore = TaskManager.initStore(AgentConstants.AGENT_STORE_PATH_TASK);
        instanceBasicStore = TaskManager.initStore(AgentConstants.AGENT_STORE_PATH_INSTANCE);
        offsetBasicStore =
                TaskManager.initStore(AgentConstants.AGENT_STORE_PATH_OFFSET);
        OffsetManager.init(taskBasicStore, instanceBasicStore, offsetBasicStore);
        mockStatic(Executors.class);
        when(Executors.newSingleThreadExecutor()).thenReturn(executor);
        when(Executors.newScheduledThreadPool(1)).thenReturn(executor);
        initProfile();
    }

    private void initProfile() {
        final String username = "";
        final String password = "123456";
        final String hostname = "127.0.0.1";
        final String port = "6379";
        final String groupId = "group01";
        final String streamId = "stream01";
        final String keys = "age,name,sex";
        final String command = "zscore";
        final String subOperation = "set,del";

        TaskProfile taskProfile = helper.getFileTaskProfile(1, "", "csv", false, "", "", TaskStateEnum.RUNNING, "D",
                "GMT+8:00", null);
        profile = taskProfile.createInstanceProfile("", taskProfile.getCycleUnit(), "20240725",
                AgentUtils.getCurrentTime());
        profile.set(CommonConstants.PROXY_INLONG_GROUP_ID, groupId);
        profile.set(CommonConstants.PROXY_INLONG_STREAM_ID, streamId);
        profile.set(TaskConstants.TASK_REDIS_AUTHUSER, username);
        profile.set(TaskConstants.TASK_REDIS_AUTHPASSWORD, password);
        profile.set(TaskConstants.TASK_REDIS_HOSTNAME, hostname);
        profile.set(TaskConstants.TASK_REDIS_PORT, port);
        profile.set(TaskConstants.TASK_REDIS_COMMAND, command);
        profile.set(TaskConstants.TASK_REDIS_KEYS, keys);
        profile.set(TaskConstants.TASK_AUDIT_VERSION, "0");
        profile.set(TaskConstants.TASK_REDIS_SUBSCRIPTION_OPERATION, subOperation);
        profile.setInstanceId(instanceId);
    }

    @Test
    public void testJedisStartup() {
        try {
            profile.setBoolean(TaskConstants.TASK_REDIS_IS_SUBSCRIBE, false);
            redisSource.initSource(profile);
            redisSource.releaseSource();
        } catch (Exception e) {
        }
    }

    @Test
    public void testReplicatorStartup() {
        try {
            profile.setBoolean(TaskConstants.TASK_REDIS_IS_SUBSCRIBE, true);
            redisSource.initSource(profile);
            redisSource.releaseSource();
        } catch (Exception e) {
            LOGGER.error(e.getMessage());
        }
    }

    @Test
    public void testScheduledExecutorStartup() {
        try {
            profile.setBoolean(TaskConstants.TASK_REDIS_IS_SUBSCRIBE, false);
            redisSource.initSource(profile);
            verify(executor, times(1)).scheduleWithFixedDelay(any(Runnable.class), eq(0L), eq(60 * 1000L),
                    eq(TimeUnit.MILLISECONDS));
            redisSource.releaseSource();
        } catch (Exception e) {
        }
    }

    @Test
    public void testFetchDataByJedis_Get()
            throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        when(jedis.pipelined()).thenReturn(pipeline);
        when(pipeline.syncAndReturnAll()).thenReturn(Arrays.asList("value1", "value2", "value3"));

        List<String> keys = Arrays.asList("key1", "key2", "key3");

        Map<String, Object> expectedData = new HashMap<>();
        expectedData.put("key1", "value1");
        expectedData.put("key2", "value2");
        expectedData.put("key3", "value3");
        Method method = RedisSource.class.getDeclaredMethod("fetchDataByJedis", Jedis.class, String.class, List.class,
                String.class);
        method.setAccessible(true);
        Map<String, Object> result = (Map<String, Object>) method.invoke(redisSource, jedis, "GET", keys, null);
        assertEquals(expectedData, result);
        verify(jedis).pipelined();
        verify(pipeline, times(3)).get(anyString());
        verify(pipeline).syncAndReturnAll();
        executor.shutdown();
    }

    @Test
    public void testFetchDataByJedis_Mget()
            throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        when(jedis.mget(eq("key1"), eq("key2"), eq("key3"))).thenReturn(Arrays.asList("value1", "value2", "value3"));
        List<String> keys = Arrays.asList("key1", "key2", "key3");
        Map<String, Object> expectedData = new HashMap<>();
        expectedData.put("key1", "value1");
        expectedData.put("key2", "value2");
        expectedData.put("key3", "value3");
        Method method = RedisSource.class.getDeclaredMethod("fetchDataByJedis", Jedis.class, String.class, List.class,
                String.class);
        method.setAccessible(true);
        Map<String, Object> result = (Map<String, Object>) method.invoke(redisSource, jedis, "MGET", keys, null);
        assertEquals(expectedData, result);
        verify(jedis).mget(eq("key1"), eq("key2"), eq("key3"));
        executor.shutdown();
    }

    @Test
    public void testFetchDataByJedis_Hget()
            throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        when(jedis.hget("key1", "field1")).thenReturn("hash_value1");
        when(jedis.hget("key2", "field1")).thenReturn("hash_value2");
        List<String> keys = Arrays.asList("key1", "key2");

        Map<String, Object> expectedData = new HashMap<>();
        expectedData.put("key1", "hash_value1");
        expectedData.put("key2", "hash_value2");

        Method method = RedisSource.class.getDeclaredMethod("fetchDataByJedis", Jedis.class, String.class, List.class,
                String.class);
        method.setAccessible(true);
        Map<String, Object> result = (Map<String, Object>) method.invoke(redisSource, jedis, "HGET", keys, "field1");
        assertEquals(expectedData, result);

        verify(jedis, times(1)).hget("key1", "field1");
        verify(jedis, times(1)).hget("key2", "field1");
        executor.shutdown();
    }

    @Test
    public void testFetchDataByJedis_Exists()
            throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        when(jedis.exists("key1")).thenReturn(true);
        when(jedis.exists("key2")).thenReturn(false);

        List<String> keys = Arrays.asList("key1", "key2");

        Map<String, Object> expectedData = new HashMap<>();
        expectedData.put("key1", true);
        expectedData.put("key2", false);

        Method method = RedisSource.class.getDeclaredMethod("fetchDataByJedis", Jedis.class, String.class, List.class,
                String.class);
        method.setAccessible(true);
        Map<String, Object> result = (Map<String, Object>) method.invoke(redisSource, jedis, "EXISTS", keys, null);
        assertEquals(expectedData, result);

        verify(jedis, times(1)).exists("key1");
        verify(jedis, times(1)).exists("key2");
        executor.shutdown();
    }
}
