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
import org.apache.inlong.agent.conf.OffsetProfile;
import org.apache.inlong.agent.conf.TaskProfile;
import org.apache.inlong.agent.constant.AgentConstants;
import org.apache.inlong.agent.constant.TaskConstants;
import org.apache.inlong.agent.core.task.MemoryManager;
import org.apache.inlong.agent.core.task.OffsetManager;
import org.apache.inlong.agent.core.task.TaskManager;
import org.apache.inlong.agent.db.Db;
import org.apache.inlong.agent.plugin.AgentBaseTestsHelper;
import org.apache.inlong.agent.plugin.Message;
import org.apache.inlong.agent.plugin.utils.file.FileDataUtils;
import org.apache.inlong.agent.utils.AgentUtils;
import org.apache.inlong.common.enums.TaskStateEnum;

import com.google.gson.Gson;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.powermock.reflect.Whitebox;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;

import static org.apache.inlong.agent.constant.FetcherConstants.AGENT_GLOBAL_READER_QUEUE_PERMIT;
import static org.apache.inlong.agent.constant.FetcherConstants.DEFAULT_AGENT_GLOBAL_READER_QUEUE_PERMIT;
import static org.apache.inlong.agent.constant.TaskConstants.INODE_INFO;
import static org.awaitility.Awaitility.await;

public class TestLogFileSource {

    private static final Logger LOGGER = LoggerFactory.getLogger(TestLogFileSource.class);
    private static final ClassLoader LOADER = TestLogFileSource.class.getClassLoader();
    private static AgentBaseTestsHelper helper;
    private static final Gson GSON = new Gson();
    private static final String[] check = {"hello line-end-symbol aa", "world line-end-symbol",
            "agent line-end-symbol"};
    // task basic db
    private static Db taskBasicDb;
    // instance basic db
    private static Db instanceBasicDb;
    // offset basic db
    private static Db offsetBasicDb;

    @BeforeClass
    public static void setup() {
        helper = new AgentBaseTestsHelper(TestLogFileSource.class.getName()).setupAgentHome();
        taskBasicDb = TaskManager.initDb(AgentConstants.AGENT_LOCAL_DB_PATH_TASK);
        instanceBasicDb = TaskManager.initDb(AgentConstants.AGENT_LOCAL_DB_PATH_INSTANCE);
        offsetBasicDb =
                TaskManager.initDb(AgentConstants.AGENT_LOCAL_DB_PATH_OFFSET);
        OffsetManager.init(taskBasicDb, instanceBasicDb, offsetBasicDb);
    }

    private LogFileSource getSource(int taskId, long offset) {
        try {
            String pattern = helper.getTestRootDir() + "/YYYYMMDD.log_[0-9]+";
            TaskProfile taskProfile = helper.getTaskProfile(taskId, pattern, false, 0L, 0L, TaskStateEnum.RUNNING, "D");
            String fileName = LOADER.getResource("test/20230928_1.txt").getPath();
            InstanceProfile instanceProfile = taskProfile.createInstanceProfile("",
                    fileName, taskProfile.getCycleUnit(), "20230928", AgentUtils.getCurrentTime());
            instanceProfile.set(TaskConstants.INODE_INFO, FileDataUtils.getInodeInfo(instanceProfile.getInstanceId()));
            LogFileSource source = new LogFileSource();
            Whitebox.setInternalState(source, "BATCH_READ_LINE_COUNT", 1);
            Whitebox.setInternalState(source, "BATCH_READ_LINE_TOTAL_LEN", 10);
            Whitebox.setInternalState(source, "CORE_THREAD_PRINT_INTERVAL_MS", 0);
            Whitebox.setInternalState(source, "SIZE_OF_BUFFER_TO_READ_FILE", 2);
            Whitebox.setInternalState(source, "EMPTY_CHECK_COUNT_AT_LEAST", 3);
            Whitebox.setInternalState(source, "READ_WAIT_TIMEOUT_MS", 10);
            if (offset > 0) {
                OffsetProfile offsetProfile = new OffsetProfile(instanceProfile.getTaskId(),
                        instanceProfile.getInstanceId(),
                        Long.toString(offset), instanceProfile.get(INODE_INFO));
                OffsetManager.getInstance().setOffset(offsetProfile);
            }
            source.init(instanceProfile);
            source.start();
            return source;
        } catch (Exception e) {
            LOGGER.error("source init error {}", e);
            Assert.assertTrue("source init error", false);
        }
        return null;
    }

    @AfterClass
    public static void teardown() throws Exception {
        helper.teardownAgentHome();
    }

    @Test
    public void testLogFileSource() {
        testFullRead();
        testCleanQueue();
        testReadWithOffset();
    }

    private void testFullRead() {
        int srcLen = 0;
        for (int i = 0; i < check.length; i++) {
            srcLen += check[i].getBytes(StandardCharsets.UTF_8).length;
        }
        LogFileSource source = getSource(1, 0);
        int cnt = 0;
        Message msg = source.read();
        int readLen = 0;
        while (msg != null) {
            readLen += msg.getBody().length;
            String record = new String(msg.getBody());
            Assert.assertTrue(record.compareTo(check[cnt]) == 0);
            msg = source.read();
            cnt++;
        }
        await().atMost(30, TimeUnit.SECONDS).until(() -> source.sourceFinish());
        source.destroy();
        Assert.assertTrue(cnt == 3);
        Assert.assertTrue(srcLen == readLen);
        int leftAfterRead = MemoryManager.getInstance().getLeft(AGENT_GLOBAL_READER_QUEUE_PERMIT);
        Assert.assertTrue(leftAfterRead == DEFAULT_AGENT_GLOBAL_READER_QUEUE_PERMIT);
    }

    private void testCleanQueue() {
        LogFileSource source = getSource(2, 0);
        for (int i = 0; i < 2; i++) {
            source.read();
        }
        Assert.assertTrue(!source.sourceFinish());
        source.destroy();
        int leftAfterRead = MemoryManager.getInstance().getLeft(AGENT_GLOBAL_READER_QUEUE_PERMIT);
        Assert.assertTrue(leftAfterRead == DEFAULT_AGENT_GLOBAL_READER_QUEUE_PERMIT);
    }

    private void testReadWithOffset() {
        LogFileSource source = getSource(3, 1);
        for (int i = 0; i < 2; i++) {
            Message msg = source.read();
            Assert.assertTrue(msg != null);
        }
        Message msg = source.read();
        Assert.assertTrue(msg == null);
        source.destroy();

        source = getSource(4, 3);
        msg = source.read();
        Assert.assertTrue(msg == null);
        source.destroy();

        int leftAfterRead = MemoryManager.getInstance().getLeft(AGENT_GLOBAL_READER_QUEUE_PERMIT);
        Assert.assertTrue(leftAfterRead == DEFAULT_AGENT_GLOBAL_READER_QUEUE_PERMIT);
    }
}