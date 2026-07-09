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

package org.apache.inlong.agent.installer.validator;

import org.apache.inlong.agent.installer.conf.InstallerConfiguration;

import lombok.Getter;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

/**
 * Resolves the effective command whitelist and write-like command sets by merging
 * built-in baselines with deployer-supplied extras.
 *
 * <p>Extras are supplied via {@value #KEY_EXTRA_COMMAND_WHITELIST} and
 * {@value #KEY_EXTRA_WRITE_LIKE_COMMANDS} (comma-separated). Entries containing
 * whitespace, path separators, or shell metacharacters are dropped to prevent
 * config-source injection.
 *
 * <p>Default singleton backed by {@link InstallerConfiguration}.
 */
@Getter
public final class CommandWhitelistResolver {

    /** Config key for extra argv[0] whitelist entries (comma-separated). */
    public static final String KEY_EXTRA_COMMAND_WHITELIST = "installer.command.extraCommandWhitelist";
    /** Config key for extra write-like commands subject to allowed-root checks. */
    public static final String KEY_EXTRA_WRITE_LIKE_COMMANDS = "installer.command.extraWriteLikeCommands";

    /** Baseline argv[0] whitelist. */
    public static final Set<String> BASELINE_COMMAND_WHITELIST = buildImmutableSet(
            "cd", "sh", "bash", "ps", "grep", "awk", "kill", "rm", "mkdir", "cp", "mv", "ln",
            "tar", "unzip", "chmod", "chown", "echo", "cat", "test", "[", "true", "false", "java");

    /** Baseline write-like commands whose path arguments trigger allowed-root checks. */
    public static final Set<String> BASELINE_WRITE_LIKE_COMMANDS = buildImmutableSet(
            "rm", "cp", "mv", "mkdir", "ln", "chmod", "chown", "tar", "unzip");

    private static final Logger LOGGER = LoggerFactory.getLogger(CommandWhitelistResolver.class);

    /**
     * Substring blacklist shared by command-level metacharacter scan and
     * whitelist-entry validation. Includes glob wildcards ({@code *}, {@code ?})
     * because {@link ProcessBuilder} does not perform glob expansion.
     */
    public static final String[] META_CHAR_BLACKLIST = {
            "`", "$(", "${", "&&", "||", ">>", ">", "<", "\\", "\u0000", "*", "?"
    };

    /**
     * Superset of {@link #META_CHAR_BLACKLIST} used for whitelist-entry validation.
     * Adds characters legal in a command string (path separators, delimiters consumed
     * by the split layer, whitespace) but never legal in a bare command name.
     */
    private static final String[] ENTRY_ILLEGAL_SUBSTRINGS = concat(META_CHAR_BLACKLIST,
            " ", "\t", "/", ";", "|");

    private static String[] concat(String[] a, String... b) {
        String[] r = new String[a.length + b.length];
        System.arraycopy(a, 0, r, 0, a.length);
        System.arraycopy(b, 0, r, a.length, b.length);
        return r;
    }

    private static volatile CommandWhitelistResolver instance;

    private final Set<String> effectiveCommandWhitelist;
    private final Set<String> effectiveWriteLikeCommands;

    private CommandWhitelistResolver(Set<String> argv0, Set<String> writeLike) {
        this.effectiveCommandWhitelist = Collections.unmodifiableSet(argv0);
        this.effectiveWriteLikeCommands = Collections.unmodifiableSet(writeLike);
    }

    /** Return the default singleton backed by {@link InstallerConfiguration}. */
    public static CommandWhitelistResolver getDefault() {
        if (instance == null) {
            synchronized (CommandWhitelistResolver.class) {
                if (instance == null) {
                    instance = build(InstallerConfiguration.getInstallerConf());
                }
            }
        }
        return instance;
    }

    /** Build from configuration, merging baselines with extra entries from config. */
    public static CommandWhitelistResolver build(InstallerConfiguration conf) {
        Set<String> argv0 = new LinkedHashSet<>(BASELINE_COMMAND_WHITELIST);
        Set<String> writeLike = new LinkedHashSet<>(BASELINE_WRITE_LIKE_COMMANDS);
        if (conf != null) {
            argv0.addAll(parseConfigList(conf.get(KEY_EXTRA_COMMAND_WHITELIST, "")));
            for (String extra : parseConfigList(conf.get(KEY_EXTRA_WRITE_LIKE_COMMANDS, ""))) {
                writeLike.add(extra);
                if (!argv0.contains(extra)) {
                    LOGGER.warn("'{}' listed in {} but not in argv[0] whitelist; "
                            + "write-like path check will not fire for it.",
                            extra, KEY_EXTRA_WRITE_LIKE_COMMANDS);
                }
            }
        }
        LOGGER.info("CommandWhitelistResolver initialized: argv0Whitelist={}, writeLikeCommands={}",
                new TreeSet<>(argv0), new TreeSet<>(writeLike));
        return new CommandWhitelistResolver(argv0, writeLike);
    }

    private static Set<String> buildImmutableSet(String... items) {
        Set<String> s = new HashSet<>(items.length * 2);
        Collections.addAll(s, items);
        return Collections.unmodifiableSet(s);
    }

    /** Split comma-separated config value, dropping blank and illegal entries. */
    public static List<String> parseConfigList(String raw) {
        List<String> out = new ArrayList<>();
        if (StringUtils.isBlank(raw)) {
            return out;
        }
        for (String item : raw.split(",")) {
            String trimmed = item.trim();
            if (trimmed.isEmpty()) {
                continue;
            }
            String reason = firstIllegalReason(trimmed);
            if (reason != null) {
                LOGGER.warn("Dropping illegal config entry '{}': {}", trimmed, reason);
                continue;
            }
            out.add(trimmed);
        }
        return out;
    }

    private static String firstIllegalReason(String entry) {
        for (String s : ENTRY_ILLEGAL_SUBSTRINGS) {
            if (entry.contains(s)) {
                return "contains '" + s + "'";
            }
        }
        return null;
    }
}
