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

import org.apache.inlong.manager.common.exceptions.BusinessException;
import org.apache.inlong.manager.pojo.module.ModuleRequest;
import org.apache.inlong.manager.service.ServiceBaseTest;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.util.ReflectionTestUtils;

/**
 * Integration test for {@link ModuleServiceImpl} — walks the real save() code path
 * that the {@code /api/module/save} endpoint invokes, and verifies that the
 * {@link BusinessException#getMessage()} eventually surfaced to the API caller
 * carries the expected user-facing error text (including the glob-wildcard hint
 * added for the P0 fix).
 *
 * <p>The wrapper format is defined by
 * {@code ErrorCodeEnum.MODULE_COMMAND_NOT_IN_WHITELIST}: {@code "Command validation failed: '%s'"}.
 */
public class ModuleServiceImplTest extends ServiceBaseTest {

    @Autowired
    private ModuleService moduleService;

    @Autowired
    private ModuleCommandValidator commandValidator;

    @BeforeEach
    public void forceStrictMode() {
        // Ensure the test is independent of application-*.properties: force STRICT
        // so the validator blocks rather than logs.
        ReflectionTestUtils.setField(commandValidator, "whitelistModeConfig", "STRICT");
    }

    private ModuleRequest baseRequest() {
        ModuleRequest r = new ModuleRequest();
        r.setName("m-" + System.nanoTime());
        r.setType("AGENT");
        r.setVersion("1.0.0");
        r.setPackageId(1);
        // sensible default: use only whitelisted commands
        r.setStartCommand("echo start");
        r.setStopCommand("echo stop");
        r.setCheckCommand("echo check");
        r.setInstallCommand("echo install");
        r.setUninstallCommand("echo uninstall");
        return r;
    }

    /* ---------- P0 focus: '*' rejected at save time with a helpful message ---------- */

    @Test
    public void save_startCommandContainsStar_apiCallerSeesGlobHint() {
        ModuleRequest req = baseRequest();
        req.setStartCommand("rm /opt/inlong/*.log");

        BusinessException ex = Assertions.assertThrows(BusinessException.class,
                () -> moduleService.save(req, "admin"));

        String msg = ex.getMessage();
        Assertions.assertNotNull(msg);
        // ErrorCodeEnum wrapper
        Assertions.assertTrue(msg.startsWith("Command validation failed:"),
                "API caller should see the wrapped error, got: " + msg);
        // pinpoint the offending field
        Assertions.assertTrue(msg.contains("startCommand:"),
                "must tell the user which field is offending, got: " + msg);
        // pinpoint the offending char
        Assertions.assertTrue(msg.contains("DISALLOWED_META_CHAR: '*'"),
                "must tell the user which char triggered rejection, got: " + msg);
        // and — critically — WHY it is rejected
        Assertions.assertTrue(msg.contains("glob wildcards are not supported"),
                "must explain glob wildcards are not supported, got: " + msg);
        Assertions.assertTrue(msg.contains("will NOT be expanded"),
                "must warn the wildcard is not expanded, got: " + msg);
        Assertions.assertTrue(msg.contains("explicit file paths"),
                "must guide the user toward the workaround, got: " + msg);
    }

    @Test
    public void save_installCommandContainsQuestionMark_apiCallerSeesGlobHint() {
        ModuleRequest req = baseRequest();
        req.setInstallCommand("cp /pkg/a?.tar /opt/");

        BusinessException ex = Assertions.assertThrows(BusinessException.class,
                () -> moduleService.save(req, "admin"));

        String msg = ex.getMessage();
        Assertions.assertTrue(msg.startsWith("Command validation failed:"), msg);
        Assertions.assertTrue(msg.contains("installCommand:"), msg);
        Assertions.assertTrue(msg.contains("DISALLOWED_META_CHAR: '?'"), msg);
        Assertions.assertTrue(msg.contains("glob wildcards are not supported"), msg);
    }

    /* ---------- other meta chars: rejected, but WITHOUT the glob hint ---------- */

    @Test
    public void save_backtickInCheckCommand_apiCallerSeesRejectionWithoutGlobHint() {
        ModuleRequest req = baseRequest();
        req.setCheckCommand("echo `whoami`");

        BusinessException ex = Assertions.assertThrows(BusinessException.class,
                () -> moduleService.save(req, "admin"));

        String msg = ex.getMessage();
        Assertions.assertTrue(msg.startsWith("Command validation failed:"), msg);
        Assertions.assertTrue(msg.contains("checkCommand:"), msg);
        Assertions.assertTrue(msg.contains("DISALLOWED_META_CHAR: '`'"), msg);
        Assertions.assertFalse(msg.contains("glob wildcards"),
                "non-glob meta char must NOT carry the glob hint, got: " + msg);
    }

    @Test
    public void save_shDashCInStopCommand_apiCallerSeesForbiddenShCFlag() {
        ModuleRequest req = baseRequest();
        req.setStopCommand("sh -c ls");

        BusinessException ex = Assertions.assertThrows(BusinessException.class,
                () -> moduleService.save(req, "admin"));

        String msg = ex.getMessage();
        Assertions.assertTrue(msg.startsWith("Command validation failed:"), msg);
        Assertions.assertTrue(msg.contains("stopCommand:"), msg);
        Assertions.assertTrue(msg.contains("FORBIDDEN_SH_C_FLAG"), msg);
    }

    @Test
    public void save_nonWhitelistedArgv0_apiCallerSeesCmdName() {
        ModuleRequest req = baseRequest();
        req.setUninstallCommand("python3 uninstall.py");

        BusinessException ex = Assertions.assertThrows(BusinessException.class,
                () -> moduleService.save(req, "admin"));

        String msg = ex.getMessage();
        Assertions.assertTrue(msg.startsWith("Command validation failed:"), msg);
        Assertions.assertTrue(msg.contains("uninstallCommand:"), msg);
        Assertions.assertTrue(msg.contains("python3"),
                "must tell the user the offending command name, got: " + msg);
    }

    /* ---------- OFF mode: validation is completely skipped ---------- */

    @Test
    public void save_modeOff_shouldNotBlockEvenIfCommandsAreDirty() {
        ReflectionTestUtils.setField(commandValidator, "whitelistModeConfig", "OFF");
        ModuleRequest req = baseRequest();
        req.setStartCommand("rm /opt/*.log");
        // In OFF mode, save must not throw due to validator. It may still throw for
        // other reasons; here we only assert the validator is NOT the throw source.
        try {
            moduleService.save(req, "admin");
        } catch (BusinessException e) {
            Assertions.assertFalse(String.valueOf(e.getMessage()).startsWith("Command validation failed:"),
                    "OFF mode must not surface the whitelist wrapper, got: " + e.getMessage());
        }
    }
}
