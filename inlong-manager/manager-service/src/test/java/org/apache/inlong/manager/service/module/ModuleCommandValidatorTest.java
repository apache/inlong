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

package org.apache.inlong.manager.service.module;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;

/**
 * Pure unit tests for {@link ModuleCommandValidator} — no Spring context.
 * Focuses on:
 *   1. rule detection (meta-char / whitelist / sh -c);
 *   2. exact error message format returned to the caller (which becomes the
 *      user-facing message once wrapped by {@code ErrorCodeEnum.MODULE_COMMAND_NOT_IN_WHITELIST}).
 */
public class ModuleCommandValidatorTest {

    private ModuleCommandValidator validator;

    @BeforeEach
    public void setUp() {
        validator = new ModuleCommandValidator();
        // Force STRICT so getMode() != OFF and validation actually happens.
        ReflectionTestUtils.setField(validator, "whitelistModeConfig",
                ModuleCommandValidator.WhitelistMode.STRICT);
        ReflectionTestUtils.setField(validator, "extraWhitelist", "");
    }

    /* ---------------- happy path ---------------- */

    @Test
    public void validate_null_shouldReturnNull() {
        Assertions.assertNull(validator.validate(null));
        Assertions.assertNull(validator.validate(""));
        Assertions.assertNull(validator.validate("   "));
    }

    @Test
    public void validate_simpleWhitelistedCommand_shouldPass() {
        Assertions.assertNull(validator.validate("ps -ef"));
        Assertions.assertNull(validator.validate("cd /opt/inlong"));
        Assertions.assertNull(validator.validate("mkdir -p /tmp/x"));
    }

    @Test
    public void validate_pipeChainOfWhitelistedCommands_shouldPass() {
        Assertions.assertNull(validator.validate("ps -ef | grep java | awk '{print $2}'"));
    }

    @Test
    public void validate_semicolonChainOfWhitelistedCommands_shouldPass() {
        Assertions.assertNull(validator.validate("cd /opt; mkdir logs; echo done"));
    }

    /* ---------------- P0: glob wildcards must be rejected with a helpful hint ---------------- */

    @Test
    public void validate_starWildcard_shouldRejectWithGlobHint() {
        String r = validator.validate("rm /opt/inlong/*.log");
        Assertions.assertNotNull(r);
        Assertions.assertTrue(r.startsWith("DISALLOWED_META_CHAR: [*]"),
                "should start with DISALLOWED_META_CHAR: [*], got: " + r);
        Assertions.assertTrue(r.contains("glob wildcards are not supported"),
                "must explain wildcards are not supported, got: " + r);
        Assertions.assertTrue(r.contains("will NOT be expanded"),
                "must warn user the wildcard will not be expanded, got: " + r);
        Assertions.assertTrue(r.contains("explicit file paths"),
                "must suggest explicit file paths, got: " + r);
    }

    @Test
    public void validate_questionMarkWildcard_shouldRejectWithGlobHint() {
        String r = validator.validate("ls /opt/inlong/?.txt");
        Assertions.assertNotNull(r);
        Assertions.assertTrue(r.startsWith("DISALLOWED_META_CHAR: [?]"),
                "should start with DISALLOWED_META_CHAR: [?], got: " + r);
        Assertions.assertTrue(r.contains("glob wildcards are not supported"),
                "must explain wildcards are not supported, got: " + r);
    }

    @Test
    public void validate_starAsArgv_shouldRejectEvenIfArgvIsSingleChar() {
        String r = validator.validate("rm *");
        Assertions.assertNotNull(r);
        Assertions.assertTrue(r.contains("*"), "must mention '*', got: " + r);
        Assertions.assertTrue(r.contains("glob wildcards are not supported"),
                "must include glob hint, got: " + r);
    }

    /* ---------------- other meta chars — reject but WITHOUT the glob hint ---------------- */

    @Test
    public void validate_backtick_shouldRejectWithoutGlobHint() {
        String r = validator.validate("echo `whoami`");
        Assertions.assertNotNull(r);
        Assertions.assertTrue(r.startsWith("DISALLOWED_META_CHAR: [`]"), r);
        Assertions.assertFalse(r.contains("glob wildcards"),
                "non-glob meta char must NOT get the glob hint, got: " + r);
    }

    @Test
    public void validate_dollarParen_shouldReject() {
        String r = validator.validate("echo $(whoami)");
        Assertions.assertNotNull(r);
        Assertions.assertTrue(r.startsWith("DISALLOWED_META_CHAR: [$(]"), r);
    }

    @Test
    public void validate_doubleAmp_shouldReject() {
        String r = validator.validate("echo a && echo b");
        Assertions.assertNotNull(r);
        Assertions.assertTrue(r.startsWith("DISALLOWED_META_CHAR: [&&]"), r);
    }

    @Test
    public void validate_redirect_shouldReject() {
        String r = validator.validate("echo hi > /tmp/x");
        Assertions.assertNotNull(r);
        Assertions.assertTrue(r.startsWith("DISALLOWED_META_CHAR: [>]"), r);
    }

    @Test
    public void validate_lineBreak_shouldReject() {
        String r = validator.validate("echo a\necho b");
        Assertions.assertNotNull(r);
        Assertions.assertEquals("DISALLOWED_META_CHAR: line-break", r);
    }

    /* ---------------- argv[0] whitelist ---------------- */

    @Test
    public void validate_notInWhitelist_shouldReturnCmdName() {
        String r = validator.validate("python3 script.py");
        Assertions.assertEquals("python3", r,
                "non-whitelisted argv[0] should be returned verbatim (no prefix)");
    }

    @Test
    public void validate_extraWhitelist_shouldExpandBaseline() {
        ReflectionTestUtils.setField(validator, "extraWhitelist", "python3,curl");
        // reset lazy cache
        ReflectionTestUtils.setField(validator, "effectiveWhitelist", null);
        Assertions.assertNull(validator.validate("python3 script.py"));
        Assertions.assertNull(validator.validate("curl http://x"));
    }

    /* ---------------- forbid sh -c / bash -c ---------------- */

    @Test
    public void validate_shDashC_shouldReject() {
        String r = validator.validate("sh -c 'echo hi'");
        Assertions.assertNotNull(r);
        Assertions.assertTrue(r.startsWith("FORBIDDEN_SH_C_FLAG:"),
                "should start with FORBIDDEN_SH_C_FLAG, got: " + r);
        Assertions.assertTrue(r.contains("sh"), r);
    }

    @Test
    public void validate_bashDashC_shouldReject() {
        String r = validator.validate("bash -c 'ls'");
        Assertions.assertNotNull(r);
        Assertions.assertTrue(r.startsWith("FORBIDDEN_SH_C_FLAG:"), r);
        Assertions.assertTrue(r.contains("bash"), r);
    }

    /* ---------------- tokenizer: mixed quoted/unquoted is intentionally NOT supported ---------------- */

    @Test
    public void validate_mixedQuotedUnquoted_argv0IsUnquotedPrefix() {
        // Under the tokenizer, a quote character always starts a fresh token and any
        // unquoted run terminates as soon as a quote character is encountered.
        // So `python3"foo" bar` splits into: [python3, foo, bar] — argv[0] is 'python3',
        // which is NOT in the baseline whitelist and must be rejected verbatim.
        String r = validator.validate("python3\"foo\" bar");
        Assertions.assertEquals("python3", r,
                "argv[0] should be the unquoted prefix 'python3', not the concatenated 'python3foo'");
    }

    @Test
    public void validate_mixedQuotedUnquoted_whitelistedPrefix_shouldPass() {
        // `ps"aux"` splits into [ps, aux] — argv[0] 'ps' is whitelisted, so this passes.
        // (Note: under the old concat-tokenizer this would have been argv[0]='psaux',
        // which would have been rejected. The new stricter semantics are intentional.)
        Assertions.assertNull(validator.validate("ps\"aux\""));
    }

}
