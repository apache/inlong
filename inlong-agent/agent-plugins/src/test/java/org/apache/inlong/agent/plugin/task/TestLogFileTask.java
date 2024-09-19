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
import org.apache.inlong.agent.constant.TaskConstants;
import org.apache.inlong.agent.core.task.TaskManager;
import org.apache.inlong.agent.plugin.AgentBaseTestsHelper;
import org.apache.inlong.agent.plugin.task.file.LogFileTask;
import org.apache.inlong.common.enums.TaskStateEnum;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static org.awaitility.Awaitility.await;

@RunWith(PowerMockRunner.class)
@PrepareForTest(LogFileTask.class)
@PowerMockIgnore({"javax.management.*"})
public class TestLogFileTask {

    private static final Logger LOGGER = LoggerFactory.getLogger(TestLogFileTask.class);
    private static final ClassLoader LOADER = TestLogFileTask.class.getClassLoader();
    private static AgentBaseTestsHelper helper;
    private static TaskManager manager;
    private static String resourceParentPath;
    private static final ThreadPoolExecutor EXECUTOR_SERVICE = new ThreadPoolExecutor(
            0, Integer.MAX_VALUE,
            1L, TimeUnit.SECONDS,
            new SynchronousQueue<>(),
            new AgentThreadFactory("TestLogfileCollectTask"));

    @BeforeClass
    public static void setup() throws Exception {
        helper = new AgentBaseTestsHelper(TestLogFileTask.class.getName()).setupAgentHome();
        resourceParentPath = new File(LOADER.getResource("testScan/temp.txt").getPath()).getParent();
        manager = new TaskManager();
    }

    @AfterClass
    public static void teardown() throws Exception {
        helper.teardownAgentHome();
    }

    @Test
    public void testScan() throws Exception {
        doTest(1, Arrays.asList("testScan/20230928_1/test_1.txt"),
                resourceParentPath + "/YYYYMMDD_[0-9]+/test_[0-9]+.txt", CycleUnitType.DAY, Arrays.asList("20230928"),
                "2023-09-28 00:00:00", "2023-09-30 23:00:00");
        doTest(2, Arrays.asList("testScan/2023092810_1/test_1.txt"),
                resourceParentPath + "/YYYYMMDDhh_[0-9]+/test_[0-9]+.txt",
                CycleUnitType.HOUR, Arrays.asList("2023092810"), "2023-09-28 00:00:00", "2023-09-30 23:00:00");
        doTest(3, Arrays.asList("testScan/202309281030_1/test_1.txt", "testScan/202309301059_1/test_1.txt"),
                resourceParentPath + "/YYYYMMDDhhmm_[0-9]+/test_[0-9]+.txt",
                CycleUnitType.MINUTE, Arrays.asList("202309281030", "202309301059"), "2023-09-28 00:00:00",
                "2023-09-30 23:00:00");
        doTest(4, Arrays.asList("testScan/20241030/23/59.txt"),
                resourceParentPath + "/YYYYMMDD/hh/mm.txt",
                CycleUnitType.MINUTE, Arrays.asList("202410302359"), "2024-10-30 00:00:00", "2024-10-31 00:00:00");
    }

    private void doTest(int taskId, List<String> resources, String pattern, String cycle, List<String> srcDataTimes,
            String startTime, String endTime)
            throws Exception {
        List<String> resourceName = new ArrayList<>();
        for (int i = 0; i < resources.size(); i++) {
            resourceName.add(LOADER.getResource(resources.get(i)).getPath());
        }
        TaskProfile taskProfile = helper.getTaskProfile(taskId, pattern, "csv", true, 0L, 0L, TaskStateEnum.RUNNING,
                cycle,
                "GMT+8:00", null);
        LogFileTask dayTask = null;
        final List<String> fileName = new ArrayList();
        final List<String> dataTime = new ArrayList();
        try {

            Date parse = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(startTime);
            long start = parse.getTime();
            parse = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(endTime);
            long end = parse.getTime();
            taskProfile.setLong(TaskConstants.TASK_START_TIME, start);
            taskProfile.setLong(TaskConstants.TASK_END_TIME, end);
            dayTask = PowerMockito.spy(new LogFileTask());
            PowerMockito.doAnswer(invocation -> {
                fileName.add(invocation.getArgument(0));
                dataTime.add(invocation.getArgument(1));
                return null;
            }).when(dayTask, "addToEvenMap", Mockito.anyString(), Mockito.anyString());
            Assert.assertTrue(dayTask.isProfileValid(taskProfile));
            manager.getTaskStore().storeTask(taskProfile);
            dayTask.init(manager, taskProfile, manager.getInstanceBasicStore());
            EXECUTOR_SERVICE.submit(dayTask);
        } catch (Exception e) {
            LOGGER.error("source init error {}", e);
            Assert.assertTrue("source init error", false);
        }
        await().atMost(10, TimeUnit.SECONDS)
                .until(() -> fileName.size() == resources.size() && dataTime.size() == resources.size());
        for (int i = 0; i < fileName.size(); i++) {
            Assert.assertEquals(0, fileName.get(i).compareTo(resourceName.get(i)));
            Assert.assertEquals(0, dataTime.get(i).compareTo(srcDataTimes.get(i)));
        }
        dayTask.destroy();
    }
}