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

package org.apache.inlong.agent.plugin.sinks.filecollect;

import org.apache.inlong.agent.common.AgentThreadFactory;
import org.apache.inlong.agent.conf.InstanceProfile;
import org.apache.inlong.agent.conf.TaskProfile;
import org.apache.inlong.agent.constant.TaskConstants;
import org.apache.inlong.agent.message.file.OffsetAckInfo;
import org.apache.inlong.agent.message.file.SenderMessage;
import org.apache.inlong.agent.plugin.AgentBaseTestsHelper;
import org.apache.inlong.agent.plugin.task.logcollection.local.FileDataUtils;
import org.apache.inlong.agent.utils.AgentUtils;
import org.apache.inlong.common.enums.TaskStateEnum;
import org.apache.inlong.sdk.dataproxy.common.ErrorCode;
import org.apache.inlong.sdk.dataproxy.common.ProcessResult;
import org.apache.inlong.sdk.dataproxy.sender.MsgSendCallback;

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

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

@RunWith(PowerMockRunner.class)
@PrepareForTest(SenderManager.class)
@PowerMockIgnore({"javax.management.*"})
public class TestSenderManager {

    private static final Logger LOGGER = LoggerFactory.getLogger(TestSenderManager.class);
    private static final ClassLoader LOADER = TestSenderManager.class.getClassLoader();
    private static AgentBaseTestsHelper helper;
    private static InstanceProfile profile;
    private static final ThreadPoolExecutor EXECUTOR_SERVICE = new ThreadPoolExecutor(
            0, Integer.MAX_VALUE,
            1L, TimeUnit.SECONDS,
            new SynchronousQueue<>(),
            new AgentThreadFactory("TestLogfileCollectTask"));

    @BeforeClass
    public static void setup() {
        String fileName = LOADER.getResource("test/20230928_1.txt").getPath();
        helper = new AgentBaseTestsHelper(TestSenderManager.class.getName()).setupAgentHome();
        String pattern = helper.getTestRootDir() + "/YYYYMMDD.log_[0-9]+";
        TaskProfile taskProfile =
                helper.getFileTaskProfile(1, pattern, "csv", false, "", "", TaskStateEnum.RUNNING, "D",
                        "GMT+8:00", null);
        profile = taskProfile.createInstanceProfile(fileName, taskProfile.getCycleUnit(), "20230927",
                AgentUtils.getCurrentTime());
    }

    @AfterClass
    public static void teardown() throws Exception {
        helper.teardownAgentHome();
    }

    @Test
    public void testNormalAck() {
        List<MsgSendCallback> cbList = new ArrayList<>();
        try {
            profile.set(TaskConstants.INODE_INFO, FileDataUtils.getInodeInfo(profile.getInstanceId()));
            SenderManager senderManager = PowerMockito.spy(new SenderManager(profile, "inlongGroupId", "sourceName"));
            PowerMockito.doNothing().when(senderManager, "createMessageSender");

            PowerMockito.doAnswer(invocation -> {
                MsgSendCallback cb = invocation.getArgument(0);
                cbList.add(cb);
                return null;
            }).when(senderManager, "asyncSendByMessageSender", Mockito.any(),
                    Mockito.any(), Mockito.any(), Mockito.any(), Mockito.anyLong(), Mockito.any(),
                    Mockito.any(), Mockito.anyBoolean());
            senderManager.Start();
            Long offset = 0L;
            List<OffsetAckInfo> ackInfoListTotal = new ArrayList<>();
            for (int i = 0; i < 10; i++) {
                List<byte[]> bodyList = new ArrayList<>();
                List<OffsetAckInfo> ackInfoList = new ArrayList<>();
                bodyList.add("123456789".getBytes(StandardCharsets.UTF_8));
                for (int j = 0; j < bodyList.size(); j++) {
                    OffsetAckInfo ackInfo = new OffsetAckInfo(Long.toString(offset++), bodyList.get(j).length, false);
                    ackInfoList.add(ackInfo);
                    ackInfoListTotal.add(ackInfo);
                }
                SenderMessage senderMessage = new SenderMessage("taskId", "instanceId", "groupId", "streamId", bodyList,
                        AgentUtils.getCurrentTime(), null, ackInfoList);
                senderManager.sendBatch(senderMessage);
            }
            Assert.assertTrue(cbList.size() == 10);
            for (int i = 0; i < 5; i++) {
                cbList.get(4 - i).onMessageAck(new ProcessResult(ErrorCode.OK));
            }
            Assert.assertTrue(calHasAckCount(ackInfoListTotal) == 5);
            for (int i = 5; i < 10; i++) {
                cbList.get(i).onMessageAck(new ProcessResult(ErrorCode.OK));
                AgentUtils.silenceSleepInMs(10);
            }
            Assert.assertTrue(String.valueOf(calHasAckCount(ackInfoListTotal)), calHasAckCount(ackInfoListTotal) == 10);
        } catch (Exception e) {
            e.printStackTrace();
            Assert.assertTrue("testNormalAck failed", false);
        }
    }

    private int calHasAckCount(List<OffsetAckInfo> ackInfoListTotal) {
        int count = 0;
        for (int i = 0; i < ackInfoListTotal.size(); i++) {
            if (ackInfoListTotal.get(i).getHasAck()) {
                count++;
            }
        }
        return count;
    }
}