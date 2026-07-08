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

import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * Structured whitelist validator applied to raw command strings coming from
 * {@code ModuleConfig}. It enforces four layers of defence:
 *
 * <ol>
 *   <li><b>Structured splitting</b>: the raw command is split into sub-commands on {@code ;},
 *       each sub-command is further split into pipe segments on {@code |}, and every pipe
 *       segment is tokenized into an {@code argv[]}. Once split this way, {@code ;} and
 *       {@code |} are Java-side delimiters instead of shell metacharacters.</li>
 *   <li><b>Metacharacter blacklist</b>: reject the whole command if it contains a backtick,
 *       {@code $(}, {@code &&}, {@code ||}, {@code >}, {@code >>}, {@code <}, or a line break
 *       character. ({@code |} and {@code ;} are already consumed by the previous layer.)</li>
 *   <li><b>argv[0] whitelist</b>: the first token of every pipe segment must appear in
 *       {@link #COMMAND_WHITELIST}, otherwise {@link #RULE_NOT_IN_WHITELIST}.</li>
 *   <li><b>Argument policy</b>: for write-oriented commands, path arguments are tilde-expanded,
 *       normalized via {@link Path#normalize()}, and then checked with
 *       {@link AllowedRootsResolver#isUnderAllowedRoot(Path)}; {@code sh}/{@code bash} may not
 *       receive a {@code -c} flag ({@link #RULE_FORBIDDEN_SH_C_FLAG}).</li>
 * </ol>
 */
public final class ModuleCommandValidator {

    /**
     * Built-in baseline for the {@code argv[0]} whitelist. Deployments may extend this via
     * {@link #KEY_EXTRA_COMMAND_WHITELIST} without touching code. See ADR-shell-injection-fix.
     */
    public static final Set<String> BASELINE_COMMAND_WHITELIST = buildImmutableSet(
            "cd", "sh", "bash", "ps", "grep", "awk", "kill", "rm", "mkdir", "cp", "mv", "ln",
            "tar", "unzip", "chmod", "chown", "echo", "cat", "test", "[", "true", "false", "java");

    /**
     * Built-in baseline for write-oriented commands whose path arguments must live under an
     * allowed root. Extendable via {@link #KEY_EXTRA_WRITE_LIKE_COMMANDS}.
     */
    public static final Set<String> BASELINE_WRITE_LIKE_COMMANDS = buildImmutableSet(
            "rm", "cp", "mv", "mkdir", "ln", "chmod", "chown", "tar", "unzip");

    /**
     * @deprecated kept for backward compatibility only; use {@link #getEffectiveCommandWhitelist()}
     *     when you need the runtime-effective set. This constant remains an alias to
     *     {@link #BASELINE_COMMAND_WHITELIST}.
     */
    @Deprecated
    public static final Set<String> COMMAND_WHITELIST = BASELINE_COMMAND_WHITELIST;

    /**
     * @deprecated kept for backward compatibility only; use {@link #getEffectiveWriteLikeCommands()}
     *     when you need the runtime-effective set. This constant remains an alias to
     *     {@link #BASELINE_WRITE_LIKE_COMMANDS}.
     */
    @Deprecated
    public static final Set<String> WRITE_LIKE_COMMANDS = BASELINE_WRITE_LIKE_COMMANDS;

    /** Configuration key for extra {@code argv[0]} whitelist entries (comma separated). */
    public static final String KEY_EXTRA_COMMAND_WHITELIST = "installer.command.extraCommandWhitelist";
    /** Configuration key for extra write-like commands that must trigger the allowed-root check. */
    public static final String KEY_EXTRA_WRITE_LIKE_COMMANDS = "installer.command.extraWriteLikeCommands";

    /**
     * Illegal characters in a whitelist entry itself. A whitelist name is meant to be a bare
     * command like {@code nohup} or {@code python3}; anything containing whitespace, a path
     * separator, or shell metacharacters is rejected up-front so that the config source
     * cannot become an injection surface.
     */
    private static final char[] ILLEGAL_ENTRY_CHARS = new char[]{
            ' ', '\t', '/', '\\', ';', '|', '&', '>', '<', '$', '`'
    };

    private static Set<String> buildImmutableSet(String... items) {
        Set<String> s = new HashSet<>(items.length * 2);
        Collections.addAll(s, items);
        return Collections.unmodifiableSet(s);
    }

    /**
     * Substring blacklist for the metacharacter check. Reject anything that a POSIX shell
     * would interpret specially and that we cannot faithfully re-implement via
     * {@link ProcessBuilder}. In particular {@code *} and {@code ?} are listed here because
     * {@link ProcessBuilder} does <em>not</em> perform glob expansion: passing a literal
     * {@code *} to {@code rm} usually matches no file and silently deletes nothing, which is
     * strictly more dangerous than an outright rejection.
     */
    private static final String[] META_CHAR_BLACKLIST = new String[]{
            "`", "$(", "${", "&&", "||", ">>", ">", "<", "\\", "\u0000", "*", "?"
    };

    public static final String RULE_DISALLOWED_META_CHAR = "DISALLOWED_META_CHAR";
    public static final String RULE_NOT_IN_WHITELIST = "NOT_IN_WHITELIST";
    public static final String RULE_PATH_NOT_UNDER_ALLOWED_ROOT = "PATH_NOT_UNDER_ALLOWED_ROOT";
    public static final String RULE_FORBIDDEN_SH_C_FLAG = "FORBIDDEN_SH_C_FLAG";
    public static final String RULE_EMPTY_COMMAND = "EMPTY_COMMAND";

    private static final Logger LOGGER = LoggerFactory.getLogger(ModuleCommandValidator.class);

    private final AllowedRootsResolver allowedRootsResolver;
    private final Set<String> effectiveCommandWhitelist;
    private final Set<String> effectiveWriteLikeCommands;

    /** Construct a validator that only uses the built-in baseline whitelists. */
    public ModuleCommandValidator(AllowedRootsResolver allowedRootsResolver) {
        this(allowedRootsResolver, BASELINE_COMMAND_WHITELIST, BASELINE_WRITE_LIKE_COMMANDS);
    }

    /**
     * Construct a validator whose effective whitelists are the baseline sets extended by
     * configuration entries from the given {@link InstallerConfiguration}.
     */
    public ModuleCommandValidator(AllowedRootsResolver allowedRootsResolver, InstallerConfiguration conf) {
        this(allowedRootsResolver, buildEffective(conf));
    }

    private ModuleCommandValidator(AllowedRootsResolver allowedRootsResolver, Set<String>[] effective) {
        this(allowedRootsResolver, effective[0], effective[1]);
    }

    /**
     * Load {@code (effectiveArgv0Whitelist, effectiveWriteLikeCommands)} once so the config
     * source is read exactly once per instance.
     */
    @SuppressWarnings("unchecked")
    private static Set<String>[] buildEffective(InstallerConfiguration conf) {
        Set<String> argv0 = loadEffectiveCommandWhitelist(conf);
        Set<String> writeLike = loadEffectiveWriteLikeCommands(conf, argv0);
        return new Set[]{argv0, writeLike};
    }

    private ModuleCommandValidator(AllowedRootsResolver allowedRootsResolver,
            Set<String> effectiveCommandWhitelist, Set<String> effectiveWriteLikeCommands) {
        this.allowedRootsResolver = allowedRootsResolver;
        this.effectiveCommandWhitelist = Collections.unmodifiableSet(new HashSet<>(effectiveCommandWhitelist));
        this.effectiveWriteLikeCommands = Collections.unmodifiableSet(new HashSet<>(effectiveWriteLikeCommands));
        LOGGER.info("ModuleCommandValidator initialized: argv0Whitelist={}, writeLikeCommands={}",
                new java.util.TreeSet<>(this.effectiveCommandWhitelist),
                new java.util.TreeSet<>(this.effectiveWriteLikeCommands));
    }

    /** Effective {@code argv[0]} whitelist actually enforced at runtime. */
    public Set<String> getEffectiveCommandWhitelist() {
        return effectiveCommandWhitelist;
    }

    /** Effective write-like set that triggers the allowed-root check at runtime. */
    public Set<String> getEffectiveWriteLikeCommands() {
        return effectiveWriteLikeCommands;
    }

    /** Validate a raw command string. */
    public ValidationResult validate(String rawCmd) {
        if (StringUtils.isBlank(rawCmd)) {
            return ValidationResult.fail(RULE_EMPTY_COMMAND, rawCmd, "raw command is blank");
        }

        // Scan the whole command for metacharacters first, so a hostile sub-command cannot
        // bypass the check by hiding after ';'.
        String metaHit = firstMetaCharHit(rawCmd);
        if (metaHit != null) {
            String reason = "hit meta char: " + metaHit;
            if ("*".equals(metaHit) || "?".equals(metaHit)) {
                reason = reason + " — glob wildcards are not supported (Agent runs commands"
                        + " without a shell, so '*' and '?' will NOT be expanded);"
                        + " please specify explicit file paths";
            }
            return ValidationResult.fail(RULE_DISALLOWED_META_CHAR, rawCmd, reason);
        }
        if (rawCmd.indexOf('\n') >= 0 || rawCmd.indexOf('\r') >= 0) {
            return ValidationResult.fail(RULE_DISALLOWED_META_CHAR, rawCmd,
                    "hit line-break char");
        }

        // Split by ';' into sub-commands, then by '|' into pipe segments, then tokenize each
        // segment into argv. Both splitters honour single/double quotes so a ';' or '|'
        // inside quotes is preserved as a literal character, matching how a POSIX shell
        // parses the command line.
        List<ParsedSubCmd> subs = new ArrayList<>();
        List<String> segments = splitTopLevel(rawCmd, ';');
        if (segments == null) {
            return ValidationResult.fail(RULE_EMPTY_COMMAND, rawCmd,
                    "unterminated quote in raw command");
        }
        for (String seg : segments) {
            String trimmed = seg == null ? "" : seg.trim();
            if (trimmed.isEmpty()) {
                continue;
            }
            ParsedSubCmd sub = parseSubCmd(trimmed);
            if (sub == null) {
                return ValidationResult.fail(RULE_EMPTY_COMMAND, trimmed,
                        "sub-command tokenized to empty");
            }
            subs.add(sub);
        }
        if (subs.isEmpty()) {
            return ValidationResult.fail(RULE_EMPTY_COMMAND, rawCmd,
                    "no sub-command after split by ';'");
        }

        // run the argv[0] whitelist and the argument policy check.
        for (ParsedSubCmd sub : subs) {
            ValidationResult r = validateSubCmd(sub);
            if (!r.isOk()) {
                return r;
            }
        }

        // Absorb 'cd' sub-commands into the working directory of the following sub-command.
        List<ParsedSubCmd> parsed = extractCdAndBind(subs);

        return ValidationResult.ok(parsed);
    }

    private ValidationResult validateSubCmd(ParsedSubCmd sub) {
        for (String[] argv : sub.getPipeline()) {
            if (argv == null || argv.length == 0) {
                return ValidationResult.fail(RULE_EMPTY_COMMAND, sub.getRawSegment(),
                        "empty pipeline segment");
            }
            String cmd = argv[0];

            if (!effectiveCommandWhitelist.contains(cmd)) {
                return ValidationResult.fail(RULE_NOT_IN_WHITELIST, sub.getRawSegment(),
                        "command '" + cmd + "' is not in whitelist");
            }

            ValidationResult r = validateArguments(argv, sub.getRawSegment());
            if (!r.isOk()) {
                return r;
            }
        }
        return ValidationResult.okPending();
    }

    private ValidationResult validateArguments(String[] argv, String rawSegment) {
        String cmd = argv[0];

        if ("sh".equals(cmd) || "bash".equals(cmd)) {
            for (int i = 1; i < argv.length; i++) {
                if (argv[i].startsWith("-c")) {
                    return ValidationResult.fail(RULE_FORBIDDEN_SH_C_FLAG, rawSegment,
                            cmd + " must not use -c to run inline scripts");
                }
            }
            String script = firstNonOptionArg(argv);
            if (looksLikePath(script)) {
                ValidationResult pathR = checkPathUnderRoot(script, rawSegment);
                if (!pathR.isOk()) {
                    return pathR;
                }
            }
            return ValidationResult.okPending();
        }

        if ("cd".equals(cmd)) {
            if (argv.length < 2) {
                return ValidationResult.okPending();
            }
            return checkPathUnderRoot(argv[1], rawSegment);
        }

        if (effectiveWriteLikeCommands.contains(cmd)) {
            for (int i = 1; i < argv.length; i++) {
                String arg = argv[i];
                if (arg.startsWith("-") || !looksLikePath(arg)) {
                    continue;
                }
                ValidationResult pathR = checkPathUnderRoot(arg, rawSegment);
                if (!pathR.isOk()) {
                    return pathR;
                }
            }
        }

        return ValidationResult.okPending();
    }

    private ValidationResult checkPathUnderRoot(String rawPath, String rawSegment) {
        try {
            String expanded = expandTilde(rawPath);
            Path p = Paths.get(expanded).toAbsolutePath().normalize();
            if (!allowedRootsResolver.isUnderAllowedRoot(p)) {
                return ValidationResult.fail(RULE_PATH_NOT_UNDER_ALLOWED_ROOT, rawSegment,
                        "path '" + rawPath + "' (normalized=" + p + ") is not under any allowed root: "
                                + allowedRootsResolver.getRoots());
            }
        } catch (Exception e) {
            return ValidationResult.fail(RULE_PATH_NOT_UNDER_ALLOWED_ROOT, rawSegment,
                    "path '" + rawPath + "' cannot be normalized: " + e.getMessage());
        }
        return ValidationResult.okPending();
    }

    /** Expand a leading {@code ~} on the Java side; {@link ProcessBuilder} does not. */
    public static String expandTilde(String path) {
        if (path == null) {
            return null;
        }
        String userHome = System.getProperty("user.home");
        if (StringUtils.isBlank(userHome)) {
            return path;
        }
        if ("~".equals(path)) {
            return userHome;
        }
        if (path.startsWith("~/")) {
            return userHome + path.substring(1);
        }
        return path;
    }

    /**
     * Remove {@code cd DIR} sub-commands from the execution sequence and turn each of them
     * into the {@link ParsedSubCmd#workDir} of the sub-commands that follow it.
     *
     * <p>Semantics: a {@code cd} affects every subsequent sub-command until the next
     * {@code cd} is encountered, matching what a POSIX shell does with a single CWD per
     * session. This means the following raw command runs {@code mkdir} <em>and</em>
     * {@code tar} both inside {@code /opt/packages}:
     *
     * <pre>{@code cd /opt/packages ; mkdir -p inlong-agent ; tar -xzvf pkg.tar.gz -C inlong-agent}</pre>
     */
    public static List<ParsedSubCmd> extractCdAndBind(List<ParsedSubCmd> subs) {
        List<ParsedSubCmd> result = new ArrayList<>(subs.size());
        File currentWorkDir = null;
        for (ParsedSubCmd sub : subs) {
            String[] argv = sub.getPipeline().get(0);
            if (argv.length >= 1 && "cd".equals(argv[0])) {
                if (argv.length >= 2) {
                    String expanded = expandTilde(argv[1]);
                    currentWorkDir = Paths.get(expanded).toAbsolutePath().normalize().toFile();
                }
                continue;
            }
            if (currentWorkDir != null) {
                sub.setWorkDir(currentWorkDir);
            }
            result.add(sub);
        }
        return result;
    }

    private static ParsedSubCmd parseSubCmd(String segment) {
        List<String> pipeSegs = splitTopLevel(segment, '|');
        if (pipeSegs == null) {
            // unterminated quote within a sub-command; caller treats null as empty/invalid.
            return null;
        }
        List<String[]> pipeline = new ArrayList<>(pipeSegs.size());
        for (String pipeSeg : pipeSegs) {
            String trimmed = pipeSeg == null ? "" : pipeSeg.trim();
            if (trimmed.isEmpty()) {
                return null;
            }
            String[] argv = tokenize(trimmed);
            if (argv.length == 0) {
                return null;
            }
            pipeline.add(argv);
        }
        boolean piped = pipeline.size() > 1;
        return new ParsedSubCmd(pipeline, segment, piped);
    }

    /**
     * Quote-aware split of {@code raw} on the top-level {@code delim} character. Characters
     * inside a single-quoted or double-quoted region are treated as literals and do not
     * split the string. Returns {@code null} when the input has an unterminated quote so
     * that the caller can surface a validation error rather than silently mis-parse.
     *
     * <p>Unlike {@link String#split(String)}, an empty leading, middle or trailing region is
     * preserved in the result so that callers can decide how to handle it.
     */
    static List<String> splitTopLevel(String raw, char delim) {
        List<String> out = new ArrayList<>();
        if (raw == null) {
            out.add("");
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
            if (c == '\'' || c == '"') {
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
     * Whitespace-based tokenizer that honours single/double quotes so that quoted spaces are
     * preserved. The metacharacter layer has already rejected backticks, {@code $(} and
     * friends, so no further shell-style escaping is needed here.
     */
    private static String[] tokenize(String s) {
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
            if (c == '\'' || c == '"') {
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
        return tokens.toArray(new String[0]);
    }

    private static String firstMetaCharHit(String raw) {
        for (String meta : META_CHAR_BLACKLIST) {
            if (raw.contains(meta)) {
                return meta;
            }
        }
        return null;
    }

    private static String firstNonOptionArg(String[] argv) {
        for (int i = 1; i < argv.length; i++) {
            if (!argv[i].startsWith("-")) {
                return argv[i];
            }
        }
        return null;
    }

    /** {@code true} when the argument looks like a path (absolute/relative/contains {@code /}/starts with {@code ~}). */
    private static boolean looksLikePath(String arg) {
        if (arg == null || arg.isEmpty()) {
            return false;
        }
        return arg.startsWith("/") || arg.startsWith("~") || arg.startsWith("./") || arg.startsWith("../")
                || arg.contains("/");
    }

    /** Split a comma-separated config value; blank items are dropped. Illegal entries are dropped with a WARN. */
    static List<String> parseConfigList(String key, String raw) {
        List<String> out = new ArrayList<>();
        if (StringUtils.isBlank(raw)) {
            return out;
        }
        for (String item : raw.split(",")) {
            String trimmed = item == null ? "" : item.trim();
            if (trimmed.isEmpty()) {
                continue;
            }
            String reason = firstIllegalReason(trimmed);
            if (reason != null) {
                LOGGER.warn("ModuleCommandValidator: dropping illegal config entry from {}: '{}' ({})",
                        key, trimmed, reason);
                continue;
            }
            out.add(trimmed);
        }
        return out;
    }

    private static String firstIllegalReason(String entry) {
        for (char c : ILLEGAL_ENTRY_CHARS) {
            if (entry.indexOf(c) >= 0) {
                return "contains forbidden char '" + c + "'";
            }
        }
        for (int i = 0; i < entry.length(); i++) {
            char c = entry.charAt(i);
            if (Character.isWhitespace(c)) {
                return "contains whitespace";
            }
        }
        return null;
    }

    private static Set<String> loadEffectiveCommandWhitelist(InstallerConfiguration conf) {
        Set<String> merged = new LinkedHashSet<>(BASELINE_COMMAND_WHITELIST);
        if (conf != null) {
            for (String extra : parseConfigList(KEY_EXTRA_COMMAND_WHITELIST,
                    conf.get(KEY_EXTRA_COMMAND_WHITELIST, ""))) {
                merged.add(extra);
            }
        }
        return merged;
    }

    private static Set<String> loadEffectiveWriteLikeCommands(InstallerConfiguration conf,
            Set<String> effectiveCommandWhitelist) {
        Set<String> merged = new LinkedHashSet<>(BASELINE_WRITE_LIKE_COMMANDS);
        if (conf != null) {
            for (String extra : parseConfigList(KEY_EXTRA_WRITE_LIKE_COMMANDS,
                    conf.get(KEY_EXTRA_WRITE_LIKE_COMMANDS, ""))) {
                merged.add(extra);
                if (!effectiveCommandWhitelist.contains(extra)) {
                    LOGGER.warn("ModuleCommandValidator: '{}' is listed in {} but not in the effective argv[0] "
                            + "whitelist; the write-like path check will not fire for it.",
                            extra, KEY_EXTRA_WRITE_LIKE_COMMANDS);
                }
            }
        }
        return merged;
    }

    /**
     * A single sub-command produced by splitting the raw command on {@code ;}. It may contain
     * multiple pipe segments produced by splitting on {@code |}.
     */
    @Getter
    public static final class ParsedSubCmd {

        private final List<String[]> pipeline;
        private final String rawSegment;
        private final boolean piped;
        private File workDir;
        private boolean allowFailure;
        private boolean pipedThroughShell;

        public ParsedSubCmd(List<String[]> pipeline, String rawSegment, boolean piped) {
            this.pipeline = pipeline;
            this.rawSegment = rawSegment;
            this.piped = piped;
        }

        /** Return the argv of a plain sub-command, or the argv of the first pipe segment. */
        public String[] getArgv() {
            return pipeline.get(0);
        }

        public void setWorkDir(File workDir) {
            this.workDir = workDir;
        }

        public void setAllowFailure(boolean allowFailure) {
            this.allowFailure = allowFailure;
        }

        public void setPipedThroughShell(boolean pipedThroughShell) {
            this.pipedThroughShell = pipedThroughShell;
        }

        @Override
        public String toString() {
            List<List<String>> readable = new ArrayList<>(pipeline.size());
            for (String[] seg : pipeline) {
                readable.add(Arrays.asList(seg));
            }
            return "ParsedSubCmd{pipeline=" + readable + ", workDir=" + workDir + "}";
        }
    }

    /**
     * Validation result. When {@link #isOk()} is {@code true}, {@link #getParsed()} returns
     * the split sub-commands ready for execution; otherwise {@link #getRuleName()},
     * {@link #getFailedSubCmd()} and {@link #getMessage()} describe the failure.
     */
    @Getter
    public static final class ValidationResult {

        private static final ValidationResult OK_PENDING = new ValidationResult(true, null, null, null,
                Collections.<ParsedSubCmd>emptyList());

        private final boolean ok;
        private final String ruleName;
        private final String failedSubCmd;
        private final String message;
        private final List<ParsedSubCmd> parsed;

        private ValidationResult(boolean ok, String ruleName, String failedSubCmd, String message,
                List<ParsedSubCmd> parsed) {
            this.ok = ok;
            this.ruleName = ruleName;
            this.failedSubCmd = failedSubCmd;
            this.message = message;
            this.parsed = parsed;
        }

        static ValidationResult ok(List<ParsedSubCmd> parsed) {
            return new ValidationResult(true, null, null, null, Collections.unmodifiableList(parsed));
        }

        /**
         * Internal marker returned when a single sub-command has passed but the whole command
         * is still being aggregated.
         */
        static ValidationResult okPending() {
            return OK_PENDING;
        }

        static ValidationResult fail(String ruleName, String failedSubCmd, String message) {
            LOGGER.debug("ModuleCommandValidator reject: rule={}, sub={}, msg={}",
                    ruleName, failedSubCmd, message);
            return new ValidationResult(false, ruleName, failedSubCmd, message,
                    Collections.<ParsedSubCmd>emptyList());
        }

        @Override
        public String toString() {
            return ok ? ("ValidationResult{ok=true, parsed=" + parsed + "}")
                    : ("ValidationResult{ok=false, rule=" + ruleName + ", failedSubCmd=" + failedSubCmd
                            + ", msg=" + message + "}");
        }
    }
}
