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

import java.nio.file.Path;
import org.apache.commons.codec.binary.Base64;
import org.apache.inlong.agent.plugin.AgentBaseTestsHelper;
import org.apache.inlong.agent.plugin.sources.snapshot.BinlogSnapshotBase;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestBinlogOffsetManager {

    private static final Logger LOGGER = LoggerFactory.getLogger(TestTextFileReader.class);
    private static Path testDir;
    private static AgentBaseTestsHelper helper;

    @BeforeClass
    public static void setup() {
        helper = new AgentBaseTestsHelper(TestTextFileReader.class.getName()).setupAgentHome();
        testDir = helper.getTestRootDir();
    }

    @AfterClass
    public static void teardown() {
        helper.teardownAgentHome();
    }

    @Test
    public void testOffset() {
        BinlogSnapshotBase snapshotManager = new BinlogSnapshotBase(testDir.toString());
        byte[] snapshotBytes = new byte[]{-65, -14, -23};
        final Base64 base64 = new Base64();
        String encodeSnapshot = base64.encodeAsString(snapshotBytes);
        snapshotManager.save(encodeSnapshot);
        Assert.assertEquals(snapshotManager.getSnapshot(), encodeSnapshot);
    }

}
