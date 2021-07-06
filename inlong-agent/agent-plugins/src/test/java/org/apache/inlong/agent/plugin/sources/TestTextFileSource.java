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

import static org.apache.inlong.agent.constants.JobConstants.JOB_DIR_FILTER_PATTERN;
import static org.apache.inlong.agent.constants.JobConstants.JOB_INSTANCE_ID;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import org.apache.inlong.agent.plugin.AgentBaseTestsHelper;
import org.apache.inlong.agent.conf.JobProfile;
import org.apache.inlong.agent.plugin.Reader;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestTextFileSource {

    private static final Logger LOGGER = LoggerFactory.getLogger(TestTextFileSource.class);

    private static AgentBaseTestsHelper helper;
    private static Path dirPath;

    @BeforeClass
    public static void setup() throws Exception {
        helper = new AgentBaseTestsHelper(TestTextFileSource.class.getName()).setupAgentHome();
        dirPath = helper.getTestRootDir();
    }

    @AfterClass
    public static void teardown() throws Exception {
        helper.teardownAgentHome();
    }

    @Test
    public void testFilePattern() throws Exception {
        JobProfile profile = new JobProfile();
        Path patternPath = Paths.get(dirPath.toFile().getAbsolutePath(), "\\d{4}\\d{2}\\d{2}.log");
        profile.set(JOB_DIR_FILTER_PATTERN, patternPath.toFile().getAbsolutePath());
        profile.set(JOB_INSTANCE_ID, "test");
        LOGGER.info("path {}, pattern {}", dirPath.toAbsolutePath(), patternPath.toAbsolutePath());

        String[] fileNames = {"a", "b", "c", "d"};
        for (String fileName : fileNames) {
            Path filePath = Paths.get(dirPath.toFile().getAbsolutePath(), fileName + ".log");
            filePath.toFile().createNewFile();
        }

        for (int i = 10; i < 30; i++) {
            Path filePath = Paths.get(dirPath.toFile().getAbsolutePath(), "197901" + i + ".log");
            filePath.toFile().createNewFile();
        }
        TextFileSource source = new TextFileSource();
        List<Reader> readerList = source.split(profile);
        Assert.assertEquals(20, readerList.size());

    }
}
