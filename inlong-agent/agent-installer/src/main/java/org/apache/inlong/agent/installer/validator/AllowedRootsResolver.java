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

import lombok.Getter;
import org.apache.inlong.agent.constant.AgentConstants;
import org.apache.inlong.agent.installer.conf.InstallerConfiguration;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Set;

/**
 * Resolves the whitelist of root directories under which write-oriented commands
 * (e.g. {@code rm/cp/mv/mkdir/ln/chmod/chown/tar/unzip}) are allowed to operate. Paths are
 * compared with normalized {@link Path#startsWith(Path)}, which naturally rejects traversal
 * attempts such as {@code ~/inlong/../../../etc}.
 *
 * <p>Default roots: {@code $user.home/inlong}, {@code $user.home/inlong-agent},
 * {@code agent.home} (from {@code -Dagent.home} JVM property or {@code installer.properties}),
 * {@code $java.io.tmpdir}. Extra roots may be appended via the configuration key
 * {@code installer.command.extraAllowedRoots} (comma separated).
 */
@Getter
public final class AllowedRootsResolver {

    /** Configuration key for extra allowed root directories (comma separated). */
    public static final String KEY_EXTRA_ALLOWED_ROOTS = "installer.command.extraAllowedRoots";

    private static final Logger LOGGER = LoggerFactory.getLogger(AllowedRootsResolver.class);

    private static volatile AllowedRootsResolver instance;

    /**
     * -- GETTER --
     * Return the immutable set of roots, mainly for logging and tests.
     */
    private final Set<Path> roots;

    private AllowedRootsResolver(Set<Path> roots) {
        this.roots = Collections.unmodifiableSet(roots);
    }

    /** Build an instance from the given path set (normalized). Intended primarily for tests. */
    public static AllowedRootsResolver ofPaths(Path... paths) {
        Set<Path> roots = new LinkedHashSet<>();
        if (paths != null) {
            for (Path p : paths) {
                addRoot(roots, p);
            }
        }
        return new AllowedRootsResolver(roots);
    }

    /** Return the default singleton, backed by {@link InstallerConfiguration}. */
    public static AllowedRootsResolver getDefault() {
        if (instance == null) {
            synchronized (AllowedRootsResolver.class) {
                if (instance == null) {
                    instance = build(InstallerConfiguration.getInstallerConf());
                }
            }
        }
        return instance;
    }

    /** Build an instance from the given configuration. */
    public static AllowedRootsResolver build(InstallerConfiguration conf) {
        Set<Path> roots = new LinkedHashSet<>();

        String userHome = System.getProperty("user.home");
        if (StringUtils.isNotBlank(userHome)) {
            addRoot(roots, Paths.get(userHome, "inlong"));
            addRoot(roots, Paths.get(userHome, "inlong-agent"));
        }

        // agent.home resolution: -Dagent.home JVM system property first, then the same key
        // from installer.properties as an override.
        String agentHome = System.getProperty(AgentConstants.AGENT_HOME);
        if (conf != null) {
            String fromConf = conf.get(AgentConstants.AGENT_HOME, null);
            if (StringUtils.isNotBlank(fromConf)) {
                agentHome = fromConf;
            }
        }
        if (StringUtils.isNotBlank(agentHome)) {
            addRoot(roots, Paths.get(agentHome));
        }

        String tmpDir = System.getProperty("java.io.tmpdir");
        if (StringUtils.isNotBlank(tmpDir)) {
            addRoot(roots, Paths.get(tmpDir));
        }

        if (conf != null) {
            String extra = conf.get(KEY_EXTRA_ALLOWED_ROOTS, "");
            if (StringUtils.isNotBlank(extra)) {
                for (String item : extra.split(",")) {
                    String trimmed = item == null ? "" : item.trim();
                    if (StringUtils.isNotBlank(trimmed)) {
                        addRoot(roots, Paths.get(trimmed));
                    }
                }
            }
        }

        LOGGER.info("AllowedRootsResolver initialized with roots: {}", roots);
        return new AllowedRootsResolver(roots);
    }

    private static void addRoot(Set<Path> roots, Path p) {
        if (p == null) {
            return;
        }
        Path normalized = p.toAbsolutePath().normalize();
        roots.add(normalized);
    }

    /**
     * Test whether the given path lies under any allowed root (equal to a root also counts).
     *
     * @param p a path; it is made absolute and normalized internally before the comparison.
     */
    public boolean isUnderAllowedRoot(Path p) {
        if (p == null) {
            return false;
        }
        Path normalized = p.toAbsolutePath().normalize();
        for (Path root : roots) {
            if (normalized.startsWith(root)) {
                return true;
            }
        }
        return false;
    }

}
