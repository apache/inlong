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

package org.apache.inlong.agent.plugin.fetcher;

import org.apache.inlong.agent.pojo.FileTask.FileTaskConfig;
import org.apache.inlong.agent.utils.EventReportUtils;
import org.apache.inlong.common.enums.TaskTypeEnum;
import org.apache.inlong.common.pojo.agent.DataConfig;

import com.google.gson.Gson;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 * Unit test for ManagerFetcher#validateFilePattern path traversal defense and allowed-dirs enforcement.
 */
public class ManagerFetcherTest {

    private static final Gson GSON = new Gson();
    private static final Set<String> NO_RESTRICTION = Collections.emptySet();

    private DataConfig buildFileDataConfig(String pattern) {
        DataConfig dataConfig = new DataConfig();
        dataConfig.setTaskType(TaskTypeEnum.FILE.getType());
        dataConfig.setTaskId(1);
        FileTaskConfig fileTaskConfig = new FileTaskConfig();
        fileTaskConfig.setPattern(pattern);
        dataConfig.setExtParams(GSON.toJson(fileTaskConfig));
        return dataConfig;
    }

    // ==================== path traversal tests (no dir restriction) ====================

    @Test
    public void testNormalPatternShouldPass() {
        DataConfig config = buildFileDataConfig("/data/logs/app.*.log");
        // Should not throw
        ManagerFetcher.validateFilePattern(config, NO_RESTRICTION);
    }

    @Test
    public void testSimpleTraversalShouldReject() {
        DataConfig config = buildFileDataConfig("../../../../etc/passwd");
        try {
            ManagerFetcher.validateFilePattern(config, NO_RESTRICTION);
            Assert.fail("Expected IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            Assert.assertTrue(e.getMessage().contains("Path traversal detected"));
        }
    }

    @Test
    public void testMixedPathWithTraversalShouldReject() {
        DataConfig config = buildFileDataConfig("/data/logs/../../etc/passwd");
        try {
            ManagerFetcher.validateFilePattern(config, NO_RESTRICTION);
            Assert.fail("Expected IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            Assert.assertTrue(e.getMessage().contains("Path traversal detected"));
        }
    }

    @Test
    public void testBackslashTraversalShouldReject() {
        DataConfig config = buildFileDataConfig("..\\..\\etc\\passwd");
        try {
            ManagerFetcher.validateFilePattern(config, NO_RESTRICTION);
            Assert.fail("Expected IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            Assert.assertTrue(e.getMessage().contains("Path traversal detected"));
        }
    }

    @Test
    public void testNonFileTaskShouldSkip() {
        DataConfig config = new DataConfig();
        config.setTaskType(TaskTypeEnum.BINLOG.getType());
        config.setTaskId(1);
        // Should not throw for non-FILE task
        ManagerFetcher.validateFilePattern(config, NO_RESTRICTION);
    }

    @Test
    public void testEmptyPatternShouldPass() {
        DataConfig config = buildFileDataConfig("");
        // Should not throw
        ManagerFetcher.validateFilePattern(config, NO_RESTRICTION);
    }

    @Test
    public void testNullPatternShouldPass() {
        DataConfig config = buildFileDataConfig(null);
        // Should not throw
        ManagerFetcher.validateFilePattern(config, NO_RESTRICTION);
    }

    @Test
    public void testDatePatternShouldPass() {
        DataConfig config = buildFileDataConfig("/data/YYYYMMDD/app.*.log");
        ManagerFetcher.validateFilePattern(config, NO_RESTRICTION);
    }

    @Test
    public void testDotInFilenameShouldPass() {
        DataConfig config = buildFileDataConfig("/data/logs/app.log");
        ManagerFetcher.validateFilePattern(config, NO_RESTRICTION);
    }

    @Test
    public void testDeepNormalPathShouldPass() {
        DataConfig config = buildFileDataConfig("/a/b/c/d/e/f/g/h/app.log");
        ManagerFetcher.validateFilePattern(config, NO_RESTRICTION);
    }

    // ==================== allowed-dirs tests ====================

    @Test
    public void testWithinAllowedDirShouldPass() {
        Set<String> allowed = allowedDirs("/data/logs");
        DataConfig config = buildFileDataConfig("/data/logs/app.log");
        ManagerFetcher.validateFilePattern(config, allowed);
    }

    @Test
    public void testOutsideAllowedDirShouldReject() {
        Set<String> allowed = allowedDirs("/data/logs");
        DataConfig config = buildFileDataConfig("/var/log/app.log");
        try {
            ManagerFetcher.validateFilePattern(config, allowed);
            Assert.fail("Expected IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            Assert.assertTrue(e.getMessage().contains("not in allowed directories"));
        }
    }

    @Test
    public void testDatePatternWithinAllowedDirShouldPass() {
        Set<String> allowed = allowedDirs("/data/logs");
        DataConfig config = buildFileDataConfig("/data/logs/YYYYMMDD/app.*.log");
        ManagerFetcher.validateFilePattern(config, allowed);
    }

    @Test
    public void testDatePatternOutsideAllowedDirShouldReject() {
        Set<String> allowed = allowedDirs("/data/logs");
        DataConfig config = buildFileDataConfig("/var/log/YYYYMMDD/app.*.log");
        try {
            ManagerFetcher.validateFilePattern(config, allowed);
            Assert.fail("Expected IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            Assert.assertTrue(e.getMessage().contains("not in allowed directories"));
        }
    }

    @Test
    public void testMultipleAllowedDirsMatchShouldPass() {
        Set<String> allowed = allowedDirs("/data/logs", "/var/log/app");
        DataConfig config = buildFileDataConfig("/var/log/app/service.log");
        ManagerFetcher.validateFilePattern(config, allowed);
    }

    @Test
    public void testTraversalRejectedBeforeAllowedDirsCheck() {
        // Path traversal should be caught first, before allowed-dirs check
        Set<String> allowed = allowedDirs("/");
        DataConfig config = buildFileDataConfig("/data/logs/../../etc/passwd");
        try {
            ManagerFetcher.validateFilePattern(config, allowed);
            Assert.fail("Expected IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            Assert.assertTrue(e.getMessage().contains("Path traversal detected"));
        }
    }

    // ==================== parseAllowedDirs tests ====================

    @Test
    public void testParseNullAllowedDirs() {
        Assert.assertEquals(Collections.emptySet(), ManagerFetcher.parseAllowedDirs(null));
    }

    @Test
    public void testParseEmptyAllowedDirs() {
        Assert.assertEquals(Collections.emptySet(), ManagerFetcher.parseAllowedDirs(""));
    }

    @Test
    public void testParseBlankAllowedDirs() {
        Assert.assertEquals(Collections.emptySet(), ManagerFetcher.parseAllowedDirs("  "));
    }

    @Test
    public void testParseSingleAllowedDir() {
        Set<String> result = ManagerFetcher.parseAllowedDirs("/data/logs");
        Assert.assertEquals(1, result.size());
        Assert.assertTrue(result.contains("/data/logs"));
    }

    @Test
    public void testParseMultipleAllowedDirs() {
        Set<String> result = ManagerFetcher.parseAllowedDirs("/data/logs, /var/log, /opt/data");
        Assert.assertEquals(3, result.size());
        Assert.assertTrue(result.contains("/data/logs"));
        Assert.assertTrue(result.contains("/var/log"));
        Assert.assertTrue(result.contains("/opt/data"));
    }

    @Test
    public void testParseAllowedDirsWithExtraCommas() {
        // trailing comma and double comma should produce empty entries that are filtered
        Set<String> result = ManagerFetcher.parseAllowedDirs("/data/logs,/var/log,, ,");
        Assert.assertEquals(2, result.size());
        Assert.assertTrue(result.contains("/data/logs"));
        Assert.assertTrue(result.contains("/var/log"));
    }

    // ==================== event reporting integration tests ====================

    @Test
    public void testTaskValidationFailedEventCode() {
        EventReportUtils.EvenCodeEnum code = EventReportUtils.EvenCodeEnum.TASK_VALIDATION_FAILED;
        Assert.assertEquals(7, code.getCode());
        Assert.assertEquals("task validation failed", code.getMessage());
    }

    @Test
    public void testRejectedTaskRetainsGroupAndStreamForReporting() {
        // Verify that a rejected DataConfig still carries groupId/streamId,
        // so the catch block can pass them to EventReportUtils.report().
        DataConfig config = buildFileDataConfig("../etc/passwd");
        config.setInlongGroupId("test_reject_group");
        config.setInlongStreamId("test_reject_stream");
        try {
            ManagerFetcher.validateFilePattern(config, NO_RESTRICTION);
            Assert.fail("Expected IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            Assert.assertTrue(e.getMessage().contains("Path traversal detected"));
            Assert.assertTrue(e.getMessage().contains("../etc/passwd"));
            // groupId/streamId survive the exception for event reporting
            Assert.assertEquals("test_reject_group", config.getInlongGroupId());
            Assert.assertEquals("test_reject_stream", config.getInlongStreamId());
        }
    }

    @Test
    public void testAllowedDirRejectionRetainsGroupAndStreamForReporting() {
        Set<String> allowed = allowedDirs("/data/logs");
        DataConfig config = buildFileDataConfig("/var/outside/app.log");
        config.setInlongGroupId("test_outside_group");
        config.setInlongStreamId("test_outside_stream");
        try {
            ManagerFetcher.validateFilePattern(config, allowed);
            Assert.fail("Expected IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            Assert.assertTrue(e.getMessage().contains("not in allowed directories"));
            Assert.assertTrue(e.getMessage().contains("/var/outside/app.log"));
            Assert.assertEquals("test_outside_group", config.getInlongGroupId());
            Assert.assertEquals("test_outside_stream", config.getInlongStreamId());
        }
    }

    private static Set<String> allowedDirs(String... dirs) {
        return new HashSet<>(Arrays.asList(dirs));
    }
}