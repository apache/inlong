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
import org.apache.inlong.agent.plugin.task.logcollection.cos.COSTask;
import org.apache.inlong.agent.plugin.utils.cos.COSUtils;
import org.apache.inlong.common.enums.TaskStateEnum;

import com.qcloud.cos.COSClient;
import com.qcloud.cos.model.COSObjectSummary;
import com.qcloud.cos.model.ListObjectsRequest;
import com.qcloud.cos.model.ObjectListing;
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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static org.awaitility.Awaitility.await;
import static org.mockito.Mockito.when;

@RunWith(PowerMockRunner.class)
@PrepareForTest({COSUtils.class, COSTask.class, COSClient.class, ObjectListing.class})
@PowerMockIgnore({"javax.management.*"})
public class TestCOSTask {

    private static final Logger LOGGER = LoggerFactory.getLogger(TestCOSTask.class);
    private static final ClassLoader LOADER = TestCOSTask.class.getClassLoader();
    private static AgentBaseTestsHelper helper;
    private static TaskManager manager;
    private static final ThreadPoolExecutor EXECUTOR_SERVICE = new ThreadPoolExecutor(
            0, Integer.MAX_VALUE,
            1L, TimeUnit.SECONDS,
            new SynchronousQueue<>(),
            new AgentThreadFactory("TestCOSTask"));
    private static COSClient cosClient;

    @BeforeClass
    public static void setup() throws Exception {
        helper = new AgentBaseTestsHelper(TestCOSTask.class.getName()).setupAgentHome();
        manager = new TaskManager();
        cosClient = Mockito.mock(COSClient.class);
        PowerMockito.mockStatic(COSUtils.class);
        Mockito.when(COSUtils.createCli(Mockito.anyString(), Mockito.anyString(), Mockito.anyString()))
                .thenReturn(cosClient);
    }

    @AfterClass
    public static void teardown() throws Exception {
        helper.teardownAgentHome();
    }

    private void mockDay(COSClient cosClient) {
        ObjectListing objectListing1_1 = Mockito.mock(ObjectListing.class);
        when(objectListing1_1.getCommonPrefixes()).thenReturn(
                Arrays.asList("some/20230928_0/", "some/20230928_1/", "some/20230928_aaa/"));
        when(objectListing1_1.getObjectSummaries()).thenReturn(getSummaries(Arrays.asList("some/20230928_test_0.txt")));

        ObjectListing objectListing1_2 = Mockito.mock(ObjectListing.class);
        when(objectListing1_2.getCommonPrefixes()).thenReturn(
                Arrays.asList("some/20230929_aaa/", "some/20230929_1/", "some/20230929_2/"));
        when(objectListing1_2.getObjectSummaries()).thenReturn(
                getSummaries(Arrays.asList("some/20230929_0_test_0.txt")));

        ObjectListing objectListing2_1 = Mockito.mock(ObjectListing.class);
        when(objectListing2_1.getCommonPrefixes()).thenReturn(
                Arrays.asList("some/20230928_0/where/", "some/20230928_0/test_1/"));
        when(objectListing2_1.getObjectSummaries()).thenReturn(getSummaries(
                Arrays.asList("some/20230928_0/test_0.txt", "some/20230928_0/test_1.txt",
                        "some/20230928_0/test_o.txt")));

        ObjectListing objectListing2_2 = Mockito.mock(ObjectListing.class);
        when(objectListing2_2.getCommonPrefixes()).thenReturn(
                Arrays.asList("some/20230929_1/where/", "some/20230929_1/test_1/"));
        when(objectListing2_2.getObjectSummaries()).thenReturn(getSummaries(
                Arrays.asList("some/20230929_1/test_0.txt", "some/20230929_1/test_1.txt",
                        "some/20230929_1/test_o.txt")));

        when(cosClient.listObjects(Mockito.any(ListObjectsRequest.class))).thenAnswer(mock -> {
            ListObjectsRequest req = mock.getArgument(0);
            if (req.getPrefix().equals("some/20230928_")) {
                return objectListing1_1;
            } else if (req.getPrefix().equals("some/20230929_")) {
                return objectListing1_2;
            } else if (req.getPrefix().equals("some/20230928_0/")) {
                return objectListing2_1;
            } else if (req.getPrefix().equals("some/20230929_1/")) {
                return objectListing2_2;
            } else {
                return new ObjectListing();
            }
        });
    }

    private void mockHour(COSClient cosClient) {
        ObjectListing objectListing1_1 = Mockito.mock(ObjectListing.class);
        when(objectListing1_1.getCommonPrefixes()).thenReturn(
                Arrays.asList("some/2023092800_0/", "some/2023092800_1/", "some/2023092800_aaa/"));
        when(objectListing1_1.getObjectSummaries()).thenReturn(
                getSummaries(Arrays.asList("some/2023092800_test_0.txt")));

        ObjectListing objectListing1_2 = Mockito.mock(ObjectListing.class);
        when(objectListing1_2.getCommonPrefixes()).thenReturn(
                Arrays.asList("some/2023092901_aaa/", "some/2023092901_1/", "some/2023092901_2/"));
        when(objectListing1_2.getObjectSummaries()).thenReturn(
                getSummaries(Arrays.asList("some/2023092901_0_test_0.txt")));

        ObjectListing objectListing2_1 = Mockito.mock(ObjectListing.class);
        when(objectListing2_1.getCommonPrefixes()).thenReturn(
                Arrays.asList("some/2023092800_0/where/", "some/2023092800_0/test_1/"));
        when(objectListing2_1.getObjectSummaries()).thenReturn(getSummaries(
                Arrays.asList("some/2023092800_0/test_0.txt", "some/2023092800_0/test_1.txt",
                        "some/2023092800_0/test_o.txt")));

        ObjectListing objectListing2_2 = Mockito.mock(ObjectListing.class);
        when(objectListing2_2.getCommonPrefixes()).thenReturn(
                Arrays.asList("some/2023092901_1/where/", "some/2023092901_1/test_1/"));
        when(objectListing2_2.getObjectSummaries()).thenReturn(getSummaries(
                Arrays.asList("some/2023092901_1/test_0.txt", "some/2023092901_1/test_1.txt",
                        "some/2023092901_1/test_o.txt")));

        when(cosClient.listObjects(Mockito.any(ListObjectsRequest.class))).thenAnswer(mock -> {
            ListObjectsRequest req = mock.getArgument(0);
            if (req.getPrefix().equals("some/2023092800_")) {
                return objectListing1_1;
            } else if (req.getPrefix().equals("some/2023092901_")) {
                return objectListing1_2;
            } else if (req.getPrefix().equals("some/2023092800_0/")) {
                return objectListing2_1;
            } else if (req.getPrefix().equals("some/2023092901_1/")) {
                return objectListing2_2;
            } else {
                return new ObjectListing();
            }
        });
    }

    private List<COSObjectSummary> getSummaries(List<String> keys) {
        List<COSObjectSummary> summaries = new ArrayList<>();
        for (int i = 0; i < keys.size(); i++) {
            COSObjectSummary summary = new COSObjectSummary();
            summary.setKey(keys.get(i));
            summary.setSize(100);
            summary.setStorageClass("what");
            summaries.add(summary);
        }
        return summaries;
    }

    @Test
    public void testScan() {
        mockDay(cosClient);
        doTest(1, "some/YYYYMMDD_[0-9]+/test_[0-9]+.txt", CycleUnitType.DAY,
                Arrays.asList("some/20230928_0/test_0.txt", "some/20230928_0/test_1.txt", "some/20230929_1/test_0.txt",
                        "some/20230929_1/test_1.txt"),
                Arrays.asList("20230928", "20230928", "20230929", "20230929"),
                "20230928",
                "20230930");
        mockHour(cosClient);
        doTest(2, "some/YYYYMMDDhh_[0-9]+/test_[0-9]+.txt", CycleUnitType.HOUR,
                Arrays.asList("some/2023092800_0/test_0.txt", "some/2023092800_0/test_1.txt",
                        "some/2023092901_1/test_0.txt",
                        "some/2023092901_1/test_1.txt"),
                Arrays.asList("2023092800", "2023092800", "2023092901", "2023092901"), "2023092800",
                "2023093023");
    }

    private void doTest(int taskId, String pattern, String cycle, List<String> srcKeys, List<String> srcDataTimes,
            String startTime, String endTime) {
        TaskProfile taskProfile = helper.getCOSTaskProfile(taskId, pattern, "csv", true, startTime, endTime,
                TaskStateEnum.RUNNING,
                cycle, "GMT+8:00", null);
        COSTask fileTask = null;
        final List<String> fileName = new ArrayList();
        final List<String> dataTime = new ArrayList();
        try {
            fileTask = PowerMockito.spy(new COSTask());
            PowerMockito.doAnswer(invocation -> {
                fileName.add(invocation.getArgument(0));
                dataTime.add(invocation.getArgument(1));
                return null;
            }).when(fileTask, "addToEvenMap", Mockito.anyString(), Mockito.anyString());
            Assert.assertTrue(fileTask.isProfileValid(taskProfile));
            manager.getTaskStore().storeTask(taskProfile);
            fileTask.init(manager, taskProfile, manager.getInstanceBasicStore());
            EXECUTOR_SERVICE.submit(fileTask);
        } catch (Exception e) {
            LOGGER.error("source init error", e);
            Assert.assertTrue("source init error", false);
        }
        await().atMost(10, TimeUnit.SECONDS)
                .until(() -> fileName.size() == srcDataTimes.size() && dataTime.size() == srcDataTimes.size());
        for (int i = 0; i < fileName.size(); i++) {
            Assert.assertEquals(0, fileName.get(i).compareTo(srcKeys.get(i)));
            Assert.assertEquals(0, dataTime.get(i).compareTo(srcDataTimes.get(i)));
        }
        fileTask.destroy();
    }
}