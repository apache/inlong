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

import org.apache.inlong.manager.common.consts.InlongConstants;
import org.apache.inlong.manager.common.enums.ErrorCodeEnum;
import org.apache.inlong.manager.common.exceptions.BusinessException;
import org.apache.inlong.manager.pojo.module.ModuleDTO;
import org.apache.inlong.manager.pojo.module.ModuleRequest;

import lombok.Getter;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * Manager-side command whitelist validator. Validates that command names (argv[0]) in module
 * commands (start/stop/check/install/uninstall) are in the allowed whitelist. This catches
 * misconfiguration at save time rather than waiting until the agent tries to execute.
 *
 * <p>Unlike the agent-side validator, this class does <b>not</b> perform path-under-root
 * checks, which are filesystem-dependent and only meaningful at the agent runtime.
 *
 * <p>Whitelist baseline:
 * <pre>{@code
 *   cd, sh, bash, ps, grep, awk, kill, rm, mkdir, cp, mv, ln,
 *   tar, unzip, chmod, chown, echo, cat, test, [, true, false, java
 * }</pre>
 *
 * <p>Extend via {@code module.command.extraWhitelist} (comma-separated) in application
 * properties, e.g.: {@code module.command.extraWhitelist=python3,nohup,curl}
 */
@Component
public class ModuleCommandValidator {

    private static final Logger LOGGER = LoggerFactory.getLogger(ModuleCommandValidator.class);

    /** Baseline argv[0] whitelist — kept in sync with agent-side ModuleCommandValidator. */
    private static final Set<String> BASELINE_WHITELIST = buildImmutableSet(
            "cd", "sh", "bash", "ps", "grep", "awk", "kill", "rm", "mkdir", "cp", "mv", "ln",
            "tar", "unzip", "chmod", "chown", "echo", "cat", "test", "[", "true", "false", "java");

    /**
     * Metacharacter substring blacklist — kept in sync with agent-side
     * {@code ModuleCommandValidator.META_CHAR_BLACKLIST}. These characters indicate shell
     * injection attempts (command substitution, chaining, redirection, escaping). Unlike
     * path-under-root checks, metacharacter validation is filesystem-independent and
     * belongs on both the Manager and Agent side.
     */
    private static final String[] META_CHAR_BLACKLIST = new String[]{
            "`", "$(", "${", "&&", "||", ">>", ">", "<", "\\", "\u0000",
            /*
             * glob wildcards — Agent uses ProcessBuilder without shell, so '*' and '?' are NOT expanded. Passing e.g.
             * 'rm *.log' as argv[1] would delete a literal file named "*.log" (or silently no-op), which is far more
             * dangerous than an explicit error. Reject them here with a clear hint.
             */
            "*", "?"
    };

    /** Extra whitelist entries from Spring config, comma-separated. */
    @Value("${module.command.extraWhitelist:}")
    private String extraWhitelist;

    /** Whitelist enforcement mode: STRICT (block), WARN (log only), OFF (skip). */
    @Value("${module.command.whitelistMode:WARN}")
    @Getter
    private WhitelistMode whitelistModeConfig;

    /**
     * Whitelist enforcement mode.
     *
     * <ul>
     *   <li>{@code STRICT} — save + update both block non-whitelisted commands.</li>
     *   <li>{@code WARN} — save blocks, update only logs a warning (for
     *       gradually tightening existing modules).</li>
     *   <li>{@code OFF} — all validation is skipped.</li>
     * </ul>
     */
    public enum WhitelistMode {

        STRICT, WARN, OFF;
    }

    /** Lazily-computed effective whitelist (baseline + config extras). */
    private volatile Set<String> effectiveWhitelist;

    private static Set<String> buildImmutableSet(String... items) {
        Set<String> s = new HashSet<>(items.length * 2);
        Collections.addAll(s, items);
        return Collections.unmodifiableSet(s);
    }

    /**
     * Get the effective (merged) whitelist — baseline + config extras. Lazy-init and
     * cached after the first call.
     */
    private Set<String> getEffectiveWhitelist() {
        if (effectiveWhitelist != null) {
            return effectiveWhitelist;
        }
        synchronized (this) {
            if (effectiveWhitelist != null) {
                return effectiveWhitelist;
            }
            Set<String> merged = new LinkedHashSet<>(BASELINE_WHITELIST);
            if (StringUtils.isNotBlank(extraWhitelist)) {
                for (String item : extraWhitelist.split(InlongConstants.COMMA)) {
                    String trimmed = item.trim();
                    if (trimmed.isEmpty()) {
                        continue;
                    }
                    merged.add(trimmed);
                }
            }
            effectiveWhitelist = Collections.unmodifiableSet(new HashSet<>(merged));
            LOGGER.info("ModuleCommandValidator initialized: whitelist={}",
                    new java.util.TreeSet<>(effectiveWhitelist));
        }
        return effectiveWhitelist;
    }

    /**
     * Save-time entry point. Runs the full command whitelist check and throws
     * {@link BusinessException} on the first violation; otherwise returns {@code true}.
     * Callers do <b>not</b> need to check the mode or wrap the exception themselves.
     *
     * <p>In {@link WhitelistMode#OFF} the check is skipped and {@code true} is returned.
     * In {@link WhitelistMode#STRICT} and {@link WhitelistMode#WARN} the save path is
     * always blocking (WARN only softens the update path).
     */
    public void validateOnSave(ModuleRequest request) {
        if (whitelistModeConfig == WhitelistMode.OFF || request == null) {
            return;
        }
        String violation = findFirstViolation(ModuleCommandAccessors.of(request));
        if (violation != null) {
            throw new BusinessException(ErrorCodeEnum.MODULE_COMMAND_NOT_IN_WHITELIST,
                    String.format(ErrorCodeEnum.MODULE_COMMAND_NOT_IN_WHITELIST.getMessage(),
                            violation));
        }
    }

    /**
     * Update-time entry point. Only validates command fields that actually changed
     * between {@code oldDto} (existing DB row) and the incoming {@code request}.
     *
     * <ul>
     *   <li>{@link WhitelistMode#OFF} — skip entirely, return {@code true}.</li>
     *   <li>{@link WhitelistMode#STRICT} — throw {@link BusinessException} on any
     *       violation.</li>
     *   <li>{@link WhitelistMode#WARN} — log a warning and let the update proceed.</li>
     * </ul>
     *
     * @param oldDto  ModuleDTO parsed from stored extParams; {@code null} means "treat
     *                every field as changed" (behaves like save-time)
     * @param request the incoming update request
     */
    public void validateOnUpdate(ModuleDTO oldDto, ModuleRequest request) {
        if (whitelistModeConfig == WhitelistMode.OFF || request == null) {
            return;
        }
        List<String> toCheck = changedCommands(oldDto, request);
        String violation = findFirstViolation(toCheck);
        if (violation == null) {
            return;
        }
        if (whitelistModeConfig == WhitelistMode.STRICT) {
            throw new BusinessException(ErrorCodeEnum.MODULE_COMMAND_NOT_IN_WHITELIST,
                    String.format(ErrorCodeEnum.MODULE_COMMAND_NOT_IN_WHITELIST.getMessage(),
                            violation));
        }
        // WARN mode: do not block the update, only surface the reason in logs.
        LOGGER.warn("ModuleCommandValidator: non-whitelisted command in update "
                + "(mode=WARN, not blocking): moduleId={}, {}", request.getId(),
                violation);
    }

    /**
     * Compute the sub-list of new-side commands whose value differs from the stored
     * one, positionally (both accessors return the five command fields in the same
     * order). A {@code null} {@code oldDto} means "no baseline", so every field is
     * treated as changed — which mirrors save-time semantics.
     */
    private static List<String> changedCommands(ModuleDTO oldDto, ModuleRequest request) {
        List<String> newCmds = ModuleCommandAccessors.of(request);
        if (oldDto == null) {
            return newCmds;
        }
        List<String> oldCmds = ModuleCommandAccessors.of(oldDto);
        List<String> changed = new ArrayList<>(newCmds.size());
        for (int i = 0; i < newCmds.size(); i++) {
            String oldRaw = i < oldCmds.size() ? oldCmds.get(i) : null;
            if (!Objects.equals(oldRaw, newCmds.get(i))) {
                changed.add(newCmds.get(i));
            }
        }
        return changed;
    }

    /**
     * Iterate the given commands and return {@code "<reason> in [<raw>]"} for the
     * first offending one, or {@code null} if all pass. The raw command is echoed
     * verbatim so operators can immediately locate which configured line triggered
     * the rejection.
     */
    private String findFirstViolation(List<String> commands) {
        for (String raw : commands) {
            String reason = validate(raw);
            if (StringUtils.isBlank(reason)) {
                continue;
            }
            return reason + " in [" + raw + "]";
        }
        return null;
    }

    /**
     * Scan the raw command string for any metacharacter substring from
     * {@link #META_CHAR_BLACKLIST}. Returns the first hit, or {@code null} if clean.
     * (Same logic as the agent-side validator.)
     */
    private static String firstMetaCharHit(String raw) {
        for (String meta : META_CHAR_BLACKLIST) {
            if (raw.contains(meta)) {
                return meta;
            }
        }
        return null;
    }

    /**
     * Validate a raw command string. Returns a descriptive error string if invalid, or
     * {@code null} if valid. The error prefix indicates the rejection reason:
     * {@code DISALLOWED_META_CHAR}, {@code FORBIDDEN_SH_C_FLAG}, or just the offending
     * command name (NOT_IN_WHITELIST).
     * Package-private for the unit tests.
     */
    String validate(String rawCmd) {
        if (StringUtils.isBlank(rawCmd)) {
            return null;
        }

        // metacharacter blacklist (whole string, before splitting).
        // Offending chars are wrapped with [] so callers can unambiguously see the boundary,
        // e.g. "DISALLOWED_META_CHAR: [`]" rather than a bare backtick which is easy to miss.
        String metaHit = firstMetaCharHit(rawCmd);
        if (StringUtils.isNotBlank(metaHit)) {
            if (InlongConstants.ASTERISK.equals(metaHit) || InlongConstants.QUESTION_MARK.equals(metaHit)) {
                return "DISALLOWED_META_CHAR: [" + metaHit + "]"
                        + " — glob wildcards are not supported (Agent runs commands without a shell,"
                        + " so [*] and [?] will NOT be expanded); please specify explicit file paths";
            }
            return "DISALLOWED_META_CHAR: [" + metaHit + "]";
        }
        if (rawCmd.indexOf(InlongConstants.NEW_LINE_CHAR) >= 0
                || rawCmd.indexOf(InlongConstants.CARRIAGE_RETURN_CHAR) >= 0) {
            return "DISALLOWED_META_CHAR: line-break";
        }

        Set<String> whitelist = getEffectiveWhitelist();

        // Split by ';' into sub-commands (quote-aware), then by '|' into pipe segments
        List<String> segments = splitTopLevel(rawCmd, InlongConstants.SEMICOLON_CHAR);
        if (segments == null) {
            return "DISALLOWED_META_CHAR: unterminated quote";
        }
        for (String seg : segments) {
            if (StringUtils.isBlank(seg)) {
                continue;
            }
            List<String> pipeSegs = splitTopLevel(seg, InlongConstants.PIPE_CHAR);
            if (pipeSegs == null) {
                return "DISALLOWED_META_CHAR: unterminated quote in pipe segment";
            }
            for (String pipeSeg : pipeSegs) {
                if (StringUtils.isBlank(pipeSeg)) {
                    continue;
                }
                List<String> argv = tokenize(pipeSeg);
                if (argv.isEmpty()) {
                    continue;
                }
                String cmdName = argv.get(0);

                // argv[0] whitelist
                if (!whitelist.contains(cmdName)) {
                    return cmdName;
                }

                // forbid sh/bash -c flag (inline script execution)
                if (("sh".equals(cmdName) || "bash".equals(cmdName)) && argv.size() > 1) {
                    for (int i = 1; i < argv.size(); i++) {
                        if (argv.get(i).startsWith("-c")) {
                            return "FORBIDDEN_SH_C_FLAG: " + cmdName + " " + argv.get(i);
                        }
                    }
                }
            }
        }
        return null;
    }

    /**
     * Quote-aware split of {@code raw} on the top-level {@code delim} character. Characters
     * inside a single-quoted or double-quoted region are treated as literals and do not
     * split the string. Returns {@code null} when the input has an unterminated quote.
     */
    static List<String> splitTopLevel(String raw, char delim) {
        List<String> out = new ArrayList<>();
        if (StringUtils.isEmpty(raw)) {
            return out;
        }
        StringBuilder cur = new StringBuilder();
        // current quote char (0 if not in a quote)
        char quote = 0;
        for (int i = 0; i < raw.length(); i++) {
            char c = raw.charAt(i);
            if (quote != 0) {
                cur.append(c);
                if (c == quote) {
                    quote = 0;
                }
                continue;
            }
            if (c == InlongConstants.SINGLE_QUOTE_CHAR || c == InlongConstants.DOUBLE_QUOTE_CHAR) {
                quote = c;
                cur.append(c);
                continue;
            }
            if (c == delim) {
                out.add(cur.toString());
                cur.setLength(0);
                continue;
            }
            cur.append(c);
        }
        if (quote != 0) {
            return null;
        }
        out.add(cur.toString());
        return out;
    }

    /**
     * Whitespace-based tokenizer that honours single/double quotes. (Same logic as the
     * agent-side validator.)
     */
    private static List<String> tokenize(String s) {
        List<String> tokens = new ArrayList<>();
        StringBuilder cur = new StringBuilder();
        char quote = 0;
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            if (quote != 0) {
                if (c == quote) {
                    quote = 0;
                } else {
                    cur.append(c);
                }
                continue;
            }
            if (c == InlongConstants.SINGLE_QUOTE_CHAR || c == InlongConstants.DOUBLE_QUOTE_CHAR) {
                quote = c;
                continue;
            }
            if (Character.isWhitespace(c)) {
                if (cur.length() > 0) {
                    tokens.add(cur.toString());
                    cur.setLength(0);
                }
                continue;
            }
            cur.append(c);
        }
        if (cur.length() > 0) {
            tokens.add(cur.toString());
        }
        return tokens;
    }
}
