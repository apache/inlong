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

package org.apache.inlong.agent.plugin.trigger;

import org.apache.inlong.agent.conf.TriggerProfile;
import org.apache.inlong.agent.constant.JobConstants;
import org.apache.inlong.agent.core.trigger.TriggerManager;
import org.apache.inlong.agent.plugin.AgentBaseTestsHelper;
import org.apache.inlong.agent.plugin.MiniAgent;
import org.apache.inlong.agent.plugin.TestFileAgent;
import org.apache.inlong.agent.plugin.utils.TestUtils;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.nio.file.WatchKey;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.awaitility.Awaitility.await;

public class TestTriggerManager {

    private static final Logger LOGGER = LoggerFactory.getLogger(TestTriggerManager.class);

    private static Path testRootDir;
    private static MiniAgent agent;
    private static AgentBaseTestsHelper helper;
    private static TriggerManager triggerManager;

    @ClassRule
    public static final TemporaryFolder WATCH_FOLDER = new TemporaryFolder();

    public static final String FILE_JOB_TEMPLATE = "{\n"
            + "  \"job\": {\n"
            + "    \"fileJob\": {\n"
            + "      \"trigger\": \"org.apache.inlong.agent.plugin.trigger.DirectoryTrigger\",\n"
            + "      \"dir\": {\n"
            + "        \"patterns\": \"/AgentBaseTestsHelper/"
            + "org.apache.tubemq.inlong.plugin.fetcher.TestTdmFetcher/test*.dat\"\n"
            + "      },\n"
            + "      \"thread\" : {\n"
            + "        \"running\": {\n"
            + "          \"core\": \"4\"\n"
            + "        }\n"
            + "      } \n"
            + "    },\n"
            + "    \"id\": 1,\n"
            + "    \"op\": \"0\",\n"
            + "    \"ip\": \"127.0.0.1\",\n"
            + "    \"groupId\": \"groupId\",\n"
            + "    \"streamId\": \"streamId\",\n"
            + "    \"name\": \"fileAgentTest\",\n"
            + "    \"source\": \"org.apache.inlong.agent.plugin.sources.TextFileSource\",\n"
            + "    \"sink\": \"org.apache.inlong.agent.plugin.sinks.MockSink\",\n"
            + "    \"channel\": \"org.apache.inlong.agent.plugin.channel.MemoryChannel\",\n"
            + "    \"standalone\": true,\n"
            + "    \"deliveryTime\": \"1231313\",\n"
            + "    \"splitter\": \"&\"\n"
            + "  }\n"
            + "}";

    @BeforeClass
    public static void setup() {
        try {
            helper = new AgentBaseTestsHelper(TestFileAgent.class.getName()).setupAgentHome();
            agent = new MiniAgent();
            agent.start();
            testRootDir = helper.getTestRootDir();
        } catch (Exception e) {
            LOGGER.error("setup failure");
        }
    }

    // todo:Test whether the trigger task can be restored normally after restarting
    @Test
    public void testRestart() throws Exception {
        TriggerProfile triggerProfile1 = TriggerProfile.parseJsonStr(FILE_JOB_TEMPLATE);
        triggerProfile1.set(JobConstants.JOB_ID, "1");
        triggerProfile1.set(JobConstants.JOB_DIR_FILTER_PATTERNS,
                WATCH_FOLDER.getRoot() + "/**/*.log");
        TriggerManager triggerManager = agent.getManager().getTriggerManager();
        triggerManager.submitTrigger(triggerProfile1);

        WATCH_FOLDER.newFolder("tmp");
        TestUtils.createHugeFiles("1.log", WATCH_FOLDER.getRoot().getAbsolutePath(), "asdqwdqd");
        TestUtils.createHugeFiles("2.log", WATCH_FOLDER.getRoot().getAbsolutePath(), "asdasdasd");
        TestUtils.createHugeFiles("3.log", WATCH_FOLDER.getRoot().getAbsolutePath() + "/tmp", "asdasdasd");
        await().atMost(10, TimeUnit.SECONDS).until(() -> agent.getManager().getTaskManager().getTaskSize() == 4);

        agent.restart();
        await().atMost(10, TimeUnit.SECONDS).until(() -> agent.getManager().getTaskManager().getTaskSize() == 4);
    }

    @Test
    public void testMultiTriggerWatchSameDir() throws Exception {
        TriggerProfile triggerProfile1 = TriggerProfile.parseJsonStr(FILE_JOB_TEMPLATE);
        triggerProfile1.set(JobConstants.JOB_ID, "1");
        triggerProfile1.set(JobConstants.JOB_DIR_FILTER_PATTERNS,
                WATCH_FOLDER.getRoot() + "/*.log");

        TriggerProfile triggerProfile2 = TriggerProfile.parseJsonStr(FILE_JOB_TEMPLATE);
        triggerProfile2.set(JobConstants.JOB_ID, "2");
        triggerProfile2.set(JobConstants.JOB_DIR_FILTER_PATTERNS,
                WATCH_FOLDER.getRoot() + "/*.txt");

        TriggerManager triggerManager = agent.getManager().getTriggerManager();
        triggerManager.submitTrigger(triggerProfile1);
        triggerManager.submitTrigger(triggerProfile2);

        TestUtils.createHugeFiles("1.log", WATCH_FOLDER.getRoot().getAbsolutePath(), "asdqwdqd");
        TestUtils.createHugeFiles("1.txt", WATCH_FOLDER.getRoot().getAbsolutePath(), "asdasdasd");
        await().atMost(10, TimeUnit.SECONDS).until(() -> agent.getManager().getTaskManager().getTaskSize() == 4);
    }

    @Test
    public void testSubmitAndShutdown() throws Exception {
        TriggerProfile triggerProfile1 = TriggerProfile.parseJsonStr(FILE_JOB_TEMPLATE);
        triggerProfile1.set(JobConstants.JOB_ID, "1");
        triggerProfile1.set(JobConstants.JOB_DIR_FILTER_PATTERNS,
                WATCH_FOLDER.getRoot() + "/*.log");

        TriggerManager triggerManager = agent.getManager().getTriggerManager();
        triggerManager.submitTrigger(triggerProfile1);
        TestUtils.createHugeFiles("1.log", WATCH_FOLDER.getRoot().getAbsolutePath(), "asdqwdqd");
        DirectoryTrigger trigger = (DirectoryTrigger) triggerManager.getTrigger(triggerProfile1.getTriggerId());
        await().atMost(10, TimeUnit.SECONDS).until(() -> {
            if (trigger.getWatchers().size() == 0) {
                return false;
            }

            for (Map.Entry<WatchKey, Set<DirectoryTrigger>> entry : trigger.getWatchers().entrySet()) {
                if (entry.getValue().size() != 1) {
                    return false;
                }
                if (entry.getValue().size() == 1 && !entry.getValue().stream().findAny().get().equals(trigger)) {
                    return false;
                }
            }
            return true;
        });

        triggerManager.deleteTrigger(triggerProfile1.getTriggerId());
        await().atMost(100, TimeUnit.SECONDS).until(() -> trigger.getWatchers().size() == 0);
    }
}
