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

package org.apache.inlong.agent.utils;

import org.junit.Assert;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/* Unit tests for {@link ExecuteLinux}, focused on exePipedCmd. Requires Unix-like OS. */
public class ExecuteLinuxTest {

    @BeforeClass
    public static void assumeUnix() {
        String os = System.getProperty("os.name").toLowerCase();
        Assume.assumeFalse("skip on Windows", os.contains("win"));
    }

    /* ---------------- exePipedCmd: normal cases ---------------- */

    @Test
    public void pipedCmd_twoSegments_shouldStreamStdoutIntoNextStdin() {
        List<String[]> segments = Arrays.asList(
                new String[]{"printf", "a\nb\nc\n"},
                new String[]{"grep", "b"});
        String out = ExecuteLinux.exePipedCmd(segments, null, ExecuteLinux.DEFAULT_TIMEOUT_MS);
        Assert.assertNotNull(out);
        Assert.assertTrue("should contain matched line, got=" + out, out.contains("b"));
        Assert.assertFalse("should not contain unmatched, got=" + out, out.contains("a"));
    }

    @Test
    public void pipedCmd_threeSegments_shouldChainCorrectly() {
        List<String[]> segments = Arrays.asList(
                new String[]{"printf", "apple\nbanana\napricot\n"},
                new String[]{"grep", "ap"},
                new String[]{"wc", "-l"});
        String out = ExecuteLinux.exePipedCmd(segments, null, ExecuteLinux.DEFAULT_TIMEOUT_MS);
        Assert.assertNotNull(out);
        Assert.assertEquals("2", out.trim());
    }

    @Test
    public void pipedCmd_largeOutput_shouldNotDeadlockNorTruncate() {
        /* yes + head + wc: verifies pipe buffer never deadlocks with large stream */
        List<String[]> segments = Arrays.asList(
                new String[]{"yes", "x"},
                new String[]{"head", "-n", "2000"},
                new String[]{"wc", "-l"});
        String out = ExecuteLinux.exePipedCmd(segments, null, ExecuteLinux.DEFAULT_TIMEOUT_MS);
        Assert.assertNotNull(out);
        Assert.assertEquals("2000", out.trim());
    }

    @Test
    public void pipedCmd_singleSegment_shouldFallbackToPlainExec() {
        String out = ExecuteLinux.exePipedCmd(
                Collections.singletonList(new String[]{"echo", "single"}),
                null, ExecuteLinux.DEFAULT_TIMEOUT_MS);
        Assert.assertNotNull(out);
        Assert.assertTrue(out.contains("single"));
    }

    /* ---------------- exePipedCmd: error / edge cases ---------------- */

    @Test
    public void pipedCmd_emptyOrNullSegments_shouldReturnNull() {
        Assert.assertNull(ExecuteLinux.exePipedCmd(Collections.<String[]>emptyList(), null, 1000L));
        Assert.assertNull(ExecuteLinux.exePipedCmd(null, null, 1000L));
    }

    @Test
    public void pipedCmd_segmentWithEmptyArgv_shouldReturnNull() {
        List<String[]> segments = Arrays.asList(
                new String[]{"echo", "hello"},
                new String[0]);
        Assert.assertNull(ExecuteLinux.exePipedCmd(segments, null, 5000L));
    }

    @Test
    public void pipedCmd_tailNonZeroExit_shouldReturnNull() {
        /* grep with no match exits 1 -> tail exit non-zero -> null */
        List<String[]> segments = Arrays.asList(
                new String[]{"echo", "hello"},
                new String[]{"grep", "nonexistent"});
        String out = ExecuteLinux.exePipedCmd(segments, null, 5000L);
        Assert.assertNull("tail non-zero exit must return null", out);
    }

    @Test
    public void pipedCmd_timeout_shouldReturnNullAndNotHang() {
        List<String[]> segments = Arrays.asList(
                new String[]{"sleep", "30"},
                new String[]{"cat"});
        long begin = System.currentTimeMillis();
        String out = ExecuteLinux.exePipedCmd(segments, null, 500L);
        long cost = System.currentTimeMillis() - begin;
        Assert.assertNull("timeout should return null", out);
        Assert.assertTrue("should finish < 3s, actual=" + cost + "ms", cost < 3000L);
    }

    /* ---------------- exePipedCmd: security — metachars stay literal (no shell) ---------------- */

    @Test
    public void pipedCmd_semicolonInGrepPattern_shouldStayLiteralRegex() {
        List<String[]> segments = Arrays.asList(
                new String[]{"printf", "prefix-AgentMain; echo INJECTED-suffix\nother\n"},
                new String[]{"grep", "AgentMain; echo INJECTED"});
        String out = ExecuteLinux.exePipedCmd(segments, null, ExecuteLinux.DEFAULT_TIMEOUT_MS);
        Assert.assertNotNull("grep must match literal pattern (exit 0)", out);
        Assert.assertTrue("output must contain matched line", out.contains("AgentMain; echo INJECTED"));
        Assert.assertFalse("unmatched lines must be absent", out.contains("other"));
    }

    @Test
    public void pipedCmd_backtickInGrepPattern_shouldStayLiteral() {
        List<String[]> segments = Arrays.asList(
                new String[]{"printf", "marker-`whoami`-suffix\n"},
                new String[]{"grep", "`whoami`"});
        String out = ExecuteLinux.exePipedCmd(segments, null, ExecuteLinux.DEFAULT_TIMEOUT_MS);
        Assert.assertNotNull(out);
        Assert.assertTrue(out.contains("`whoami`"));
    }

    @Test
    public void pipedCmd_dollarParenInGrepPattern_shouldStayLiteral() {
        List<String[]> segments = Arrays.asList(
                new String[]{"printf", "prefix-$(id)-suffix\n"},
                new String[]{"grep", "$(id)"});
        String out = ExecuteLinux.exePipedCmd(segments, null, ExecuteLinux.DEFAULT_TIMEOUT_MS);
        Assert.assertNotNull(out);
        Assert.assertTrue(out.contains("$(id)"));
    }

    @Test
    public void pipedCmd_doubleAmpersandInGrepPattern_shouldStayLiteral() {
        List<String[]> segments = Arrays.asList(
                new String[]{"printf", "marker-&&-suffix\n"},
                new String[]{"grep", "&&"});
        String out = ExecuteLinux.exePipedCmd(segments, null, ExecuteLinux.DEFAULT_TIMEOUT_MS);
        Assert.assertNotNull(out);
        Assert.assertTrue(out.contains("&&"));
    }

    @Test
    public void pipedCmd_pipeCharInGrepPattern_shouldBeRegexAlternation() {
        /* "|" inside pattern acts as regex alternation, not a new pipe segment */
        List<String[]> segments = Arrays.asList(
                new String[]{"printf", "foo\nbar\nbaz\n"},
                new String[]{"grep", "-E", "foo|bar"});
        String out = ExecuteLinux.exePipedCmd(segments, null, ExecuteLinux.DEFAULT_TIMEOUT_MS);
        Assert.assertNotNull(out);
        Assert.assertTrue(out.contains("foo"));
        Assert.assertTrue(out.contains("bar"));
        Assert.assertFalse(out.contains("baz"));
    }

    /* ---------------- exeCmd(String[]): minimal sanity (non-deprecated) ---------------- */

    @Test
    public void arrayCmd_echo_shouldReturnStdout() {
        String out = ExecuteLinux.exeCmd(new String[]{"echo", "hello-world"});
        Assert.assertNotNull(out);
        Assert.assertTrue(out.contains("hello-world"));
    }

    @Test
    public void arrayCmd_emptyArgs_shouldReturnNull() {
        Assert.assertNull(ExecuteLinux.exeCmd((String[]) null));
        Assert.assertNull(ExecuteLinux.exeCmd(new String[]{}));
    }
}