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

package org.apache.inlong.agent.plugin.sinks;

import org.apache.inlong.agent.conf.InstanceProfile;
import org.apache.inlong.agent.conf.TaskProfile;
import org.apache.inlong.agent.message.ProxyMessage;
import org.apache.inlong.agent.plugin.AgentBaseTestsHelper;
import org.apache.inlong.agent.plugin.sinks.filecollect.TestSenderManager;
import org.apache.inlong.agent.utils.AgentUtils;
import org.apache.inlong.common.enums.TaskStateEnum;

import org.junit.BeforeClass;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

import static org.apache.inlong.agent.constant.CommonConstants.PROXY_KEY_GROUP_ID;
import static org.apache.inlong.agent.constant.CommonConstants.PROXY_KEY_STREAM_ID;

public class PulsarSinkTest {

    private static MockSink pulsarSink;
    private static InstanceProfile profile;
    private static AgentBaseTestsHelper helper;
    private static final ClassLoader LOADER = TestSenderManager.class.getClassLoader();

    @BeforeClass
    public static void setUp() throws Exception {
        String fileName = LOADER.getResource("test/20230928_1.txt").getPath();
        helper = new AgentBaseTestsHelper(TestSenderManager.class.getName()).setupAgentHome();
        String pattern = helper.getTestRootDir() + "/YYYYMMDD.log_[0-9]+";
        TaskProfile taskProfile =
                helper.getFileTaskProfile(1, pattern, "csv", false, "", "", TaskStateEnum.RUNNING, "D",
                        "GMT+8:00", null);
        profile = taskProfile.createInstanceProfile("", fileName,
                taskProfile.getCycleUnit(), "20230927", AgentUtils.getCurrentTime());
        pulsarSink = new MockSink();
        pulsarSink.init(profile);
    }

    @Test
    public void testWrite() {
        String body = "testMesage";
        Map<String, String> attr = new HashMap<>();
        attr.put(PROXY_KEY_GROUP_ID, "groupId");
        attr.put(PROXY_KEY_STREAM_ID, "streamId");
        long count = 5;
        for (long i = 0; i < 5; i++) {
            pulsarSink.write(new ProxyMessage(body.getBytes(StandardCharsets.UTF_8), attr));
        }
    }

}
