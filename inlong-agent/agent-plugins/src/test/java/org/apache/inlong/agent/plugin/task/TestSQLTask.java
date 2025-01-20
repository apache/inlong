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

package org.apache.inlong.agent.plugin.task;

import org.apache.inlong.agent.common.AgentThreadFactory;
import org.apache.inlong.agent.conf.TaskProfile;
import org.apache.inlong.agent.constant.CycleUnitType;
import org.apache.inlong.agent.core.task.TaskManager;
import org.apache.inlong.agent.plugin.AgentBaseTestsHelper;
import org.apache.inlong.agent.plugin.task.logcollection.SQLTask;
import org.apache.inlong.common.enums.TaskStateEnum;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.modules.junit4.PowerMockRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static org.awaitility.Awaitility.await;

@RunWith(PowerMockRunner.class)
@PowerMockIgnore({"javax.management.*"})
public class TestSQLTask {

    private static final Logger LOGGER = LoggerFactory.getLogger(TestSQLTask.class);
    private static final ClassLoader LOADER = TestSQLTask.class.getClassLoader();
    private static AgentBaseTestsHelper helper;
    private static TaskManager manager;
    private static final ThreadPoolExecutor EXECUTOR_SERVICE = new ThreadPoolExecutor(
            0, Integer.MAX_VALUE,
            1L, TimeUnit.SECONDS,
            new SynchronousQueue<>(),
            new AgentThreadFactory("TestSQLTask"));

    @BeforeClass
    public static void setup() throws Exception {
        helper = new AgentBaseTestsHelper(TestSQLTask.class.getName()).setupAgentHome();
        manager = new TaskManager();
    }

    @AfterClass
    public static void teardown() throws Exception {
        helper.teardownAgentHome();
    }

    @Test
    public void testScan() {
        doTest(1, "select * from table where field = YYYYMMDD_[0-9]+;", CycleUnitType.DAY,
                Arrays.asList("select * from table where field = 20230928_[0-9]+;",
                        "select * from table where field = 20230929_[0-9]+;",
                        "select * from table where field = 20230930_[0-9]+;"),
                Arrays.asList("20230928", "20230929", "20230930"),
                "20230928",
                "20230930");
        doTest(2, "select * from table where field = YYYYMMDDHH_[0-9]+;", CycleUnitType.HOUR,
                Arrays.asList("select * from table where field = 2023092823_[0-9]+;",
                        "select * from table where field = 2023092900_[0-9]+;",
                        "select * from table where field = 2023092901_[0-9]+;"),
                Arrays.asList("2023092823", "2023092900", "2023092901"), "2023092823", "2023092901");
        doTest(3, "select * from table where field = YYYYMMDDHHmm_[0-9]+;", CycleUnitType.MINUTE,
                Arrays.asList("select * from table where field = 202309282359_[0-9]+;",
                        "select * from table where field = 202309290000_[0-9]+;",
                        "select * from table where field = 202309290001_[0-9]+;"),
                Arrays.asList("202309282359", "202309290000", "202309290001"), "202309282359", "202309290001");
    }

    @Test
    public void testScanLowercase() {
        doTest(1, "select * from table where field = yyyyMMdd_[0-9]+;", CycleUnitType.DAY,
                Arrays.asList("select * from table where field = 20230928_[0-9]+;",
                        "select * from table where field = 20230929_[0-9]+;",
                        "select * from table where field = 20230930_[0-9]+;"),
                Arrays.asList("20230928", "20230929", "20230930"),
                "20230928",
                "20230930");
        doTest(2, "select * from table where field = yyyyMMddhh_[0-9]+;", CycleUnitType.HOUR,
                Arrays.asList("select * from table where field = 2023092823_[0-9]+;",
                        "select * from table where field = 2023092900_[0-9]+;",
                        "select * from table where field = 2023092901_[0-9]+;"),
                Arrays.asList("2023092823", "2023092900", "2023092901"), "2023092823", "2023092901");
        doTest(3, "select * from table where field = yyyyMMddhhmm_[0-9]+;", CycleUnitType.MINUTE,
                Arrays.asList("select * from table where field = 202309282359_[0-9]+;",
                        "select * from table where field = 202309290000_[0-9]+;",
                        "select * from table where field = 202309290001_[0-9]+;"),
                Arrays.asList("202309282359", "202309290000", "202309290001"), "202309282359", "202309290001");
    }

    private void doTest(int taskId, String sql, String cycle, List<String> srcSQLs, List<String> srcDataTimes,
            String startTime, String endTime) {
        TaskProfile taskProfile = helper.getSQLTaskProfile(taskId, sql, "csv", true, startTime, endTime,
                TaskStateEnum.RUNNING, cycle, "GMT+8:00");
        SQLTask sqlTask = null;
        final List<String> fileName = new ArrayList();
        final List<String> dataTime = new ArrayList();
        try {
            sqlTask = PowerMockito.spy(new SQLTask());
            PowerMockito.doAnswer(invocation -> {
                fileName.add(invocation.getArgument(0));
                dataTime.add(invocation.getArgument(1));
                return null;
            }).when(sqlTask, "addToEvenMap", Mockito.anyString(), Mockito.anyString(), Mockito.anyLong(),
                    Mockito.anyString());
            Assert.assertTrue(sqlTask.isProfileValid(taskProfile));
            manager.getTaskStore().storeTask(taskProfile);
            sqlTask.init(manager, taskProfile, manager.getInstanceBasicStore());
            EXECUTOR_SERVICE.submit(sqlTask);
        } catch (Exception e) {
            LOGGER.error("source init error", e);
            Assert.assertTrue("source init error", false);
        }
        await().atMost(10, TimeUnit.SECONDS)
                .until(() -> fileName.size() == srcDataTimes.size() && dataTime.size() == srcDataTimes.size());
        for (int i = 0; i < fileName.size(); i++) {
            Assert.assertEquals(0, fileName.get(i).compareTo(srcSQLs.get(i)));
            Assert.assertEquals(0, dataTime.get(i).compareTo(srcDataTimes.get(i)));
        }
        sqlTask.destroy();
    }
}