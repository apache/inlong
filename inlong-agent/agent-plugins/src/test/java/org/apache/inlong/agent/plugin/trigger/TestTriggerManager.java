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

import org.apache.inlong.agent.core.trigger.TriggerManager;
import org.apache.inlong.agent.plugin.AgentBaseTestsHelper;
import org.apache.inlong.agent.plugin.MiniAgent;
import org.apache.inlong.agent.plugin.TestFileAgent;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;

public class TestTriggerManager {
    private static final Logger LOGGER = LoggerFactory.getLogger(TestTriggerManager.class);

    private static Path testRootDir;
    private static MiniAgent agent;
    private static AgentBaseTestsHelper helper;
    private static TriggerManager triggerManager;

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
    public void testRestart() {

    }

    // todo:Test multiple different triggers to monitor the same directory
    @Test
    public void testMultiTriggerWatchSameDir() {

    }

    // todo:Test trigger overrun
    @Test
    public void testExceedMaxNumTrigger() {

    }

    // todo:Test trigger submit and shutdown
    @Test
    public void testSubmitAndShutdown() {

    }
}
