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

import io.debezium.engine.ChangeEvent;
import io.debezium.engine.DebeziumEngine;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doNothing;
import static org.powermock.api.mockito.PowerMockito.mockStatic;
import static org.powermock.api.mockito.PowerMockito.when;

/**
 * Test cases for {@link SQLServerSource}.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({DebeziumEngine.class, Executors.class, SQLServerSource.class, MetricRegister.class})
@PowerMockIgnore({"javax.management.*"})
public class TestSQLServerSource {

    private SQLServerSource source;

    private static AgentBaseTestsHelper helper;
    // task basic store
    private static Store taskBasicStore;
    // instance basic store
    private static Store instanceBasicStore;
    // offset basic store
    private static Store offsetBasicStore;

    InstanceProfile instanceProfile;

    @Mock
    private DebeziumEngine.Builder builder;

    @Mock
    private ExecutorService executorService;

    @Mock
    DebeziumEngine.RecordCommitter<ChangeEvent<String, String>> committer;

    @Mock
    private DebeziumEngine<ChangeEvent<String, String>> engine;

    private BlockingQueue queue;

    private final String instanceId = "s4bc475560b4444dbd4e9812ab1fd64d";

    @Before
    public void setup() throws Exception {

        helper = new AgentBaseTestsHelper(TestSQLServerSource.class.getName()).setupAgentHome();
        taskBasicStore = TaskManager.initStore(AgentConstants.AGENT_STORE_PATH_TASK);
        instanceBasicStore = TaskManager.initStore(AgentConstants.AGENT_STORE_PATH_INSTANCE);
        offsetBasicStore =
                TaskManager.initStore(AgentConstants.AGENT_STORE_PATH_OFFSET);
        OffsetManager.init(taskBasicStore, instanceBasicStore, offsetBasicStore);
        // mock DebeziumEngine
        mockStatic(DebeziumEngine.class);
        when(DebeziumEngine.create(io.debezium.engine.format.Json.class)).thenReturn(builder);
        when(builder.using(any(Properties.class))).thenReturn(builder);
        when(builder.notifying(any(DebeziumEngine.ChangeConsumer.class))).thenReturn(builder);
        when(builder.using(any(DebeziumEngine.CompletionCallback.class))).thenReturn(builder);
        when(builder.build()).thenReturn(engine);

        doNothing().when(committer).markProcessed(any(ChangeEvent.class));
        doNothing().when(committer).markBatchFinished();

        // mock executorService
        mockStatic(Executors.class);
        when(Executors.newSingleThreadExecutor()).thenReturn(executorService);

        getSource();
        // init source debeziumQueue
        Field field = SQLServerSource.class.getDeclaredField("debeziumQueue");
        field.setAccessible(true);
        queue = (BlockingQueue) field.get(source);
    }

    private SQLServerSource getSource() {
        final String username = "SA";
        final String password = "123456";
        final String hostname = "127.0.0.1";
        final String port = "1434";
        final String groupId = "group01";
        final String streamId = "stream01";
        final String dbName = "inlong";
        final String schemaName = "dbo";
        final String tableName = "test_source";
        final String serverName = "server-01";

        TaskProfile taskProfile = helper.getFileTaskProfile(1, "", "csv", false, "", "", TaskStateEnum.RUNNING, "D",
                "GMT+8:00", null);
        instanceProfile = taskProfile.createInstanceProfile("",
                "", taskProfile.getCycleUnit(), "20240725", AgentUtils.getCurrentTime());
        instanceProfile.set(CommonConstants.PROXY_INLONG_GROUP_ID, groupId);
        instanceProfile.set(CommonConstants.PROXY_INLONG_STREAM_ID, streamId);
        instanceProfile.set(TaskConstants.TASK_SQLSERVER_USER, username);
        instanceProfile.set(TaskConstants.TASK_SQLSERVER_PASSWORD, password);
        instanceProfile.set(TaskConstants.TASK_SQLSERVER_HOSTNAME, hostname);
        instanceProfile.set(TaskConstants.TASK_SQLSERVER_PORT, port);
        instanceProfile.set(TaskConstants.TASK_SQLSERVER_DB_NAME, dbName);
        instanceProfile.set(TaskConstants.TASK_SQLSERVER_SCHEMA_NAME, schemaName);
        instanceProfile.set(TaskConstants.TASK_SQLSERVER_TABLE_NAME, tableName);
        instanceProfile.set(TaskConstants.TASK_SQLSERVER_SERVER_NAME, serverName);
        instanceProfile.set(TaskConstants.TASK_AUDIT_VERSION, "0");
        instanceProfile.setInstanceId(instanceId);

        (source = new SQLServerSource()).init(instanceProfile);
        return source;
    }

    @Test
    public void testSQLServerSource() throws Exception {
        testHandleConsumerEvent();
        TestReadDataFromSource();
        TestReadEmptyFromSource();
    }

    // test DebeziumEngine get one recode from SQLServer
    private void testHandleConsumerEvent() throws Exception {
        List<ChangeEvent<String, String>> records = new ArrayList<>();
        records.add(new ChangeEvent<String, String>() {

            @Override
            public String key() {
                return "KEY";
            }

            @Override
            public String value() {
                return "VALUE";
            }

            @Override
            public String destination() {
                return null;
            }
        });
        Method handleConsumerEvent = SQLServerSource.class.getDeclaredMethod("handleConsumerEvent", List.class,
                DebeziumEngine.RecordCommitter.class);
        handleConsumerEvent.setAccessible(true);
        handleConsumerEvent.invoke(source, records, committer);
        assertEquals(1, queue.size());
    }

    // test read one source data from queue
    private void TestReadDataFromSource() throws Exception {
        Method handleConsumerEvent = SQLServerSource.class.getDeclaredMethod("readFromSource");
        handleConsumerEvent.setAccessible(true);

        List result = (List) handleConsumerEvent.invoke(source);
        assertFalse(result.isEmpty());
        assertTrue(queue.isEmpty());
    }

    // test read
    private void TestReadEmptyFromSource() throws Exception {
        Method handleConsumerEvent = SQLServerSource.class.getDeclaredMethod("readFromSource");
        handleConsumerEvent.setAccessible(true);

        queue.clear();
        List result = (List) handleConsumerEvent.invoke(source);
        assertTrue(result.isEmpty());
        assertTrue(queue.isEmpty());
    }
}
