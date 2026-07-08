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

package org.apache.inlong.common.util;

import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 * Unit test for {@link PathValidationUtils#isWithinAllowedDirs}.
 */
public class PathValidationUtilsTest {

    // ==================== null / empty allowedDirs (no restriction) ====================

    @Test
    public void testNullAllowedDirsShouldPass() {
        Assert.assertTrue(PathValidationUtils.isWithinAllowedDirs("/etc/passwd", null));
    }

    @Test
    public void testEmptyAllowedDirsShouldPass() {
        Assert.assertTrue(PathValidationUtils.isWithinAllowedDirs("/etc/passwd", Collections.emptySet()));
    }

    @Test
    public void testNullPathWithRestrictedDirsShouldFail() {
        Set<String> allowed = setOf("/data/logs");
        Assert.assertFalse(PathValidationUtils.isWithinAllowedDirs(null, allowed));
    }

    @Test
    public void testEmptyPathWithRestrictedDirsShouldFail() {
        Set<String> allowed = setOf("/data/logs");
        Assert.assertFalse(PathValidationUtils.isWithinAllowedDirs("", allowed));
    }

    // ==================== exact directory match ====================

    @Test
    public void testExactDirMatchShouldPass() {
        Set<String> allowed = setOf("/data/logs");
        Assert.assertTrue(PathValidationUtils.isWithinAllowedDirs("/data/logs", allowed));
    }

    @Test
    public void testFileUnderAllowedDirShouldPass() {
        Set<String> allowed = setOf("/data/logs");
        Assert.assertTrue(PathValidationUtils.isWithinAllowedDirs("/data/logs/app.log", allowed));
    }

    @Test
    public void testDeepFileUnderAllowedDirShouldPass() {
        Set<String> allowed = setOf("/data/logs");
        Assert.assertTrue(PathValidationUtils.isWithinAllowedDirs("/data/logs/2025/01/app.log", allowed));
    }

    @Test
    public void testSiblingDirShouldFail() {
        Set<String> allowed = setOf("/data/logs");
        Assert.assertFalse(PathValidationUtils.isWithinAllowedDirs("/data/other/app.log", allowed));
    }

    @Test
    public void testParentDirShouldFail() {
        Set<String> allowed = setOf("/data/logs");
        Assert.assertFalse(PathValidationUtils.isWithinAllowedDirs("/data", allowed));
    }

    @Test
    public void testUnrelatedDirShouldFail() {
        Set<String> allowed = setOf("/data/logs");
        Assert.assertFalse(PathValidationUtils.isWithinAllowedDirs("/etc/passwd", allowed));
    }

    // ==================== prefix overlap guard ====================

    @Test
    public void testPrefixOverlapShouldNotMatch() {
        // "/data" must NOT match "/data_other/logs" (prefix but not a directory boundary)
        Set<String> allowed = setOf("/data");
        Assert.assertTrue(PathValidationUtils.isWithinAllowedDirs("/data/logs", allowed));
        // Note: "/data_other" is NOT a subdirectory of "/data" — startsWith alone would false-match
    }

    // ==================== multiple allowed dirs ====================

    @Test
    public void testMatchAgainstSecondAllowedDir() {
        Set<String> allowed = setOf("/data/logs", "/var/log/app");
        Assert.assertTrue(PathValidationUtils.isWithinAllowedDirs("/var/log/app/service.log", allowed));
    }

    @Test
    public void testNoMatchWithMultipleAllowedDirs() {
        Set<String> allowed = setOf("/data/logs", "/var/log/app");
        Assert.assertFalse(PathValidationUtils.isWithinAllowedDirs("/home/user/file.txt", allowed));
    }

    // ==================== trailing slash variations ====================

    @Test
    public void testAllowedDirWithTrailingSlash() {
        Set<String> allowed = setOf("/data/logs/");
        Assert.assertTrue(PathValidationUtils.isWithinAllowedDirs("/data/logs/app.log", allowed));
    }

    @Test
    public void testAllowedDirWithoutTrailingSlash() {
        Set<String> allowed = setOf("/data/logs");
        Assert.assertTrue(PathValidationUtils.isWithinAllowedDirs("/data/logs/app.log", allowed));
    }

    // ==================== backslash normalization ====================

    @Test
    public void testBackslashPathShouldNormalize() {
        Set<String> allowed = setOf("/data/logs");
        Assert.assertTrue(PathValidationUtils.isWithinAllowedDirs("\\data\\logs\\app.log", allowed));
    }

    @Test
    public void testBackslashInAllowedDirShouldNormalize() {
        Set<String> allowed = setOf("\\data\\logs");
        Assert.assertTrue(PathValidationUtils.isWithinAllowedDirs("/data/logs/app.log", allowed));
    }

    // ==================== edge cases ====================

    @Test
    public void testRelativePathShouldFail() {
        Set<String> allowed = setOf("/data/logs");
        Assert.assertFalse(PathValidationUtils.isWithinAllowedDirs("./data/logs", allowed));
    }

    @Test
    public void testRootDirShouldPass() {
        Set<String> allowed = setOf("/");
        Assert.assertTrue(PathValidationUtils.isWithinAllowedDirs("/anything/here", allowed));
    }

    @Test
    public void testNullEntryInAllowedDirsShouldBeIgnored() {
        Set<String> allowed = new HashSet<>(Arrays.asList(null, "/data/logs"));
        Assert.assertTrue(PathValidationUtils.isWithinAllowedDirs("/data/logs/app.log", allowed));
    }

    @Test
    public void testEmptyEntryInAllowedDirsShouldBeIgnored() {
        Set<String> allowed = new HashSet<>(Arrays.asList("", "/data/logs"));
        Assert.assertTrue(PathValidationUtils.isWithinAllowedDirs("/data/logs/app.log", allowed));
    }

    private static Set<String> setOf(String... values) {
        return new HashSet<>(Arrays.asList(values));
    }
}