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

package org.apache.inlong.agent.plugin.filter;

import org.apache.inlong.agent.plugin.AgentBaseTestsHelper;
import org.apache.inlong.agent.plugin.task.PathPattern;
import org.apache.inlong.agent.utils.AgentUtils;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Collections;
import java.util.Locale;

public class TestDateFormatRegex {

    private static AgentBaseTestsHelper helper;
    private static Path testPath;

    @BeforeClass
    public static void setup() {
        helper = new AgentBaseTestsHelper(TestDateFormatRegex.class.getName()).setupAgentHome();
        testPath = helper.getTestRootDir();
    }

    @AfterClass
    public static void teardown() {
        helper.teardownAgentHome();
    }

    @Test
    public void testRegex() {
        File file = Paths.get(helper.getParentPath().toString(), "aad20201201_11.log").toFile();
        DateFormatRegex dateFormatRegex = DateFormatRegex
                .ofRegex(helper.getParentPath().toString() + "/\\w{3}YYYYMMDD_HH.log");
        dateFormatRegex.match(file);
        dateFormatRegex.getFormattedTime();
        Assert.assertEquals(helper.getParentPath().toString() + "/\\w{3}"
                + AgentUtils.formatCurrentTime("yyyyMMdd_HH") + ".log",
                dateFormatRegex.getFormattedRegex());
    }

    @Test
    public void testRegexAndTimeOffset() throws IOException {
        ZonedDateTime zoned = ZonedDateTime.now().plusDays(-1);
        String pathTime = DateTimeFormatter.ofPattern("yyyyMMdd").withLocale(Locale.getDefault()).format(zoned);
        File file = Paths.get(helper.getTestRootDir().toString(), pathTime.concat(".log")).toFile();
        file.createNewFile();
        PathPattern entity = new PathPattern(helper.getTestRootDir().toString(),
                Collections.singleton(helper.getTestRootDir().toString() + "/yyyyMMdd.log"), "-1d");
        boolean flag = entity.suitable(file.getPath());
        Assert.assertTrue(flag);
    }
}
