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
import org.apache.inlong.manager.pojo.module.ModuleDTO;
import org.apache.inlong.manager.pojo.module.ModuleRequest;

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
import java.util.function.Function;
import java.util.function.Predicate;

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
     * Get the current whitelist enforcement mode from configuration.
     */
    public WhitelistMode getMode() {
        return whitelistModeConfig;
    }

    /**
     * Incremental validation — only validates command fields that have actually changed
     * between the existing module config (from DB) and the incoming update request.
     * <p>
     * This avoids blocking updates for existing modules that only change non-command
     * fields (e.g. version number, package id).
     *
     * @param oldDto  the existing ModuleDTO parsed from DB extParams (may be null if
     *                extParams was empty — all fields are treated as changed)
     * @param request the incoming update request
     * @return the offending field name + command, or {@code null} if all changed commands pass
     */
    public String validateChanged(ModuleDTO oldDto, ModuleRequest request) {
        if (oldDto == null) {
            // No existing data — validate everything (same as save)
            return validateAll(request);
        }
        Function<CommandField, String> oldReader = ModuleCommandAccessors.of(oldDto);
        Function<CommandField, String> newReader = ModuleCommandAccessors.of(request);
        // Skip fields whose value is unchanged (both null or equal).
        return validateFields(newReader,
                f -> Objects.equals(oldReader.apply(f), newReader.apply(f)));
    }

    /**
     * Core validation loop: iterate through every {@link CommandField}, read its current
     * value via {@code accessor}, and run {@link #validate(String)}. Fields for which
     * {@code skip} returns {@code true} are bypassed (used for incremental validation).
     *
     * @return {@code "<label>: <offending>"} on the first failure, or {@code null} if all pass
     */
    private String validateFields(Function<CommandField, String> accessor,
            Predicate<CommandField> skip) {
        for (CommandField f : CommandField.values()) {
            if (skip.test(f)) {
                continue;
            }
            String offending = validate(accessor.apply(f));
            if (StringUtils.isNotBlank(offending)) {
                return f.label + ": " + offending;
            }
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
     *
     * @param rawCmd the raw command string (may be null or blank — skipped)
     * @return error description, or {@code null} if valid
     */
    public String validate(String rawCmd) {
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
     * Thoroughly validate all module command fields on a {@link ModuleRequest}. Returns
     * {@code null} if all commands pass, or a descriptive message such as
     * {@code "startCommand: python3"} / {@code "startCommand: DISALLOWED_META_CHAR: '*' — ..."}
     * if one fails.
     *
     * <p>Preferred over the 5-argument overload — passing the request object avoids
     * argument-order mistakes at call sites.
     */
    public String validateAll(ModuleRequest request) {
        if (request == null) {
            return null;
        }
        return validateFields(ModuleCommandAccessors.of(request), f -> false);
    }

    /**
     * Quote-aware split of {@code raw} on the top-level {@code delim} character. Characters
     * inside a single-quoted or double-quoted region are treated as literals and do not
     * split the string. Returns {@code null} when the input has an unterminated quote.
     */
    static List<String> splitTopLevel(String raw, char delim) {
        List<String> out = new ArrayList<>();
        if (raw == null) {
            return out;
        }
        StringBuilder cur = new StringBuilder();
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
