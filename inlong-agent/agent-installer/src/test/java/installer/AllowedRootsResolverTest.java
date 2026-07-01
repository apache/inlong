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

import org.apache.inlong.agent.constant.AgentConstants;
import org.apache.inlong.agent.installer.conf.InstallerConfiguration;
import org.apache.inlong.agent.installer.validator.AllowedRootsResolver;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Set;

/**
 * Unit tests for {@link AllowedRootsResolver}. This class is the last line of the shell
 * injection defence chain: {@code ModuleCommandValidator} ultimately delegates every path
 * check to {@link AllowedRootsResolver#isUnderAllowedRoot(Path)}. The tests below pin the
 * behaviours that are easy to break silently during refactors:
 *
 * <ul>
 *   <li>path traversal (e.g. {@code root/../etc}) is defeated by {@link Path#normalize()};</li>
 *   <li>prefix confusion (e.g. {@code /home/user/inlong-agent-evil} vs
 *       {@code /home/user/inlong-agent}) is defeated because {@link Path#startsWith(Path)}
 *       compares by segments, not by string prefix;</li>
 *   <li>the {@code agent.home} resolution order &mdash; system property first, configuration
 *       override second &mdash; behaves as documented;</li>
 *   <li>{@code installer.command.extraAllowedRoots} CSV parsing skips blank items;</li>
 *   <li>null / blank inputs and {@code conf == null} are handled without throwing.</li>
 * </ul>
 */
public class AllowedRootsResolverTest {

    private String savedAgentHomeSysProp;
    private String savedAgentHomeConf;
    private String savedExtraRootsConf;

    @Before
    public void setUp() {
        // Snapshot the system property and any conf entries the tests may mutate, so we can
        // restore them in @After. This keeps the shared InstallerConfiguration singleton
        // clean for every subsequent test.
        savedAgentHomeSysProp = System.getProperty(AgentConstants.AGENT_HOME);
        InstallerConfiguration conf = InstallerConfiguration.getInstallerConf();
        savedAgentHomeConf = conf.get(AgentConstants.AGENT_HOME, null);
        savedExtraRootsConf = conf.get(AllowedRootsResolver.KEY_EXTRA_ALLOWED_ROOTS, null);
    }

    @After
    public void tearDown() {
        // Restore agent.home system property.
        if (savedAgentHomeSysProp == null) {
            System.clearProperty(AgentConstants.AGENT_HOME);
        } else {
            System.setProperty(AgentConstants.AGENT_HOME, savedAgentHomeSysProp);
        }
        // Restore conf entries. AbstractConfiguration#set(key, null) removes the entry.
        InstallerConfiguration conf = InstallerConfiguration.getInstallerConf();
        conf.set(AgentConstants.AGENT_HOME, savedAgentHomeConf);
        conf.set(AllowedRootsResolver.KEY_EXTRA_ALLOWED_ROOTS, savedExtraRootsConf);
    }

    @Test
    public void ofPaths_rootItselfAndChildren_shouldMatch() {
        Path root = Paths.get(System.getProperty("java.io.tmpdir"), "inlong-test-root");
        AllowedRootsResolver resolver = AllowedRootsResolver.ofPaths(root);

        Assert.assertTrue("root itself should match", resolver.isUnderAllowedRoot(root));
        Assert.assertTrue("direct child should match",
                resolver.isUnderAllowedRoot(root.resolve("child")));
        Assert.assertTrue("nested descendant should match",
                resolver.isUnderAllowedRoot(root.resolve("a/b/c")));
    }

    @Test
    public void ofPaths_siblingPath_shouldNotMatch() {
        Path root = Paths.get(System.getProperty("java.io.tmpdir"), "inlong-test-root");
        Path sibling = Paths.get(System.getProperty("java.io.tmpdir"), "inlong-test-other");
        AllowedRootsResolver resolver = AllowedRootsResolver.ofPaths(root);

        Assert.assertFalse("sibling directory must not match",
                resolver.isUnderAllowedRoot(sibling));
    }

    /**
     * Path traversal: {@code root/../etc} normalises to {@code /etc} which is outside every
     * allowed root. Regressing this check would neutralise the whole shell-injection defence.
     */
    @Test
    public void pathTraversal_shouldNotMatch() {
        Path root = Paths.get(System.getProperty("java.io.tmpdir"), "inlong-test-root");
        AllowedRootsResolver resolver = AllowedRootsResolver.ofPaths(root);

        Path traversal = root.resolve("../../etc/passwd");
        Assert.assertFalse("traversal path must be rejected after normalize()",
                resolver.isUnderAllowedRoot(traversal));
    }

    /**
     * Prefix confusion: {@code /home/user/inlong-agent-evil} shares a string prefix with
     * {@code /home/user/inlong-agent} but is a different path segment; {@link Path#startsWith}
     * must reject it. If a future refactor swaps the check for {@link String#startsWith} the
     * test below fails.
     */
    @Test
    public void prefixConfusion_shouldNotMatch() {
        Path root = Paths.get("/home/user/inlong-agent");
        Path evil = Paths.get("/home/user/inlong-agent-evil/x");
        AllowedRootsResolver resolver = AllowedRootsResolver.ofPaths(root);

        Assert.assertFalse("path with same string prefix but different segment must be rejected",
                resolver.isUnderAllowedRoot(evil));
    }

    @Test
    public void nullInput_shouldReturnFalse() {
        AllowedRootsResolver resolver = AllowedRootsResolver.ofPaths(
                Paths.get(System.getProperty("java.io.tmpdir")));
        Assert.assertFalse(resolver.isUnderAllowedRoot(null));
    }

    @Test
    public void build_defaultRoots_shouldContainUserHomeAndTmp() {
        // Ensure agent.home is not set from either source, so only the guaranteed defaults
        // (user.home/inlong, user.home/inlong-agent, java.io.tmpdir) are added.
        System.clearProperty(AgentConstants.AGENT_HOME);
        InstallerConfiguration conf = InstallerConfiguration.getInstallerConf();
        conf.set(AgentConstants.AGENT_HOME, null);
        conf.set(AllowedRootsResolver.KEY_EXTRA_ALLOWED_ROOTS, null);

        AllowedRootsResolver resolver = AllowedRootsResolver.build(conf);
        Set<Path> roots = resolver.getRoots();

        String userHome = System.getProperty("user.home");
        String tmpDir = System.getProperty("java.io.tmpdir");
        Assert.assertTrue("expected " + userHome + "/inlong in " + roots,
                roots.contains(Paths.get(userHome, "inlong").toAbsolutePath().normalize()));
        Assert.assertTrue("expected " + userHome + "/inlong-agent in " + roots,
                roots.contains(Paths.get(userHome, "inlong-agent").toAbsolutePath().normalize()));
        Assert.assertTrue("expected java.io.tmpdir in " + roots,
                roots.contains(Paths.get(tmpDir).toAbsolutePath().normalize()));
    }

    /**
     * When {@code -Dagent.home} is set on the JVM (as {@code bin/*.sh} does via
     * {@code BASE_DIR=$(cd "$(dirname "$0")"/../;pwd)}), it becomes an allowed root even when
     * the configuration file does not mention it.
     */
    @Test
    public void build_agentHomeFromSystemProperty_shouldBeAdded() {
        String appHome = Paths.get(System.getProperty("java.io.tmpdir"), "app-home-sys")
                .toAbsolutePath().normalize().toString();
        System.setProperty(AgentConstants.AGENT_HOME, appHome);
        InstallerConfiguration conf = InstallerConfiguration.getInstallerConf();
        conf.set(AgentConstants.AGENT_HOME, null);

        AllowedRootsResolver resolver = AllowedRootsResolver.build(conf);
        Assert.assertTrue("agent.home from -D should be added: " + resolver.getRoots(),
                resolver.getRoots().contains(Paths.get(appHome)));
    }

    /**
     * Configuration entry overrides the {@code -Dagent.home} system property. This mirrors
     * the documented resolution order: system property first, config override second.
     */
    @Test
    public void build_agentHomeConfOverridesSystemProperty() {
        String fromSys = Paths.get(System.getProperty("java.io.tmpdir"), "app-home-sys")
                .toAbsolutePath().normalize().toString();
        String fromConf = Paths.get(System.getProperty("java.io.tmpdir"), "app-home-conf")
                .toAbsolutePath().normalize().toString();
        System.setProperty(AgentConstants.AGENT_HOME, fromSys);
        InstallerConfiguration conf = InstallerConfiguration.getInstallerConf();
        conf.set(AgentConstants.AGENT_HOME, fromConf);

        AllowedRootsResolver resolver = AllowedRootsResolver.build(conf);
        Set<Path> roots = resolver.getRoots();
        Assert.assertTrue("conf value should be present: " + roots,
                roots.contains(Paths.get(fromConf)));
        Assert.assertFalse("system property value should have been overridden: " + roots,
                roots.contains(Paths.get(fromSys)));
    }

    /**
     * When neither the system property nor the configuration key is set, the resolver still
     * builds successfully and simply skips the {@code agent.home} root. Other defaults must
     * remain available.
     */
    @Test
    public void build_noAgentHome_shouldSkipWithoutError() {
        System.clearProperty(AgentConstants.AGENT_HOME);
        InstallerConfiguration conf = InstallerConfiguration.getInstallerConf();
        conf.set(AgentConstants.AGENT_HOME, null);

        AllowedRootsResolver resolver = AllowedRootsResolver.build(conf);
        // At minimum the tmpdir default should still be there.
        Assert.assertTrue(resolver.getRoots().contains(
                Paths.get(System.getProperty("java.io.tmpdir")).toAbsolutePath().normalize()));
    }

    /**
     * {@code conf == null} must not throw; the resolver falls back to system property /
     * environment defaults only.
     */
    @Test
    public void build_nullConf_shouldFallBackToDefaultsOnly() {
        System.clearProperty(AgentConstants.AGENT_HOME);
        AllowedRootsResolver resolver = AllowedRootsResolver.build(null);
        Assert.assertFalse("default roots should not be empty", resolver.getRoots().isEmpty());
    }

    @Test
    public void build_extraAllowedRoots_shouldSplitAndTrim() {
        String tmp = System.getProperty("java.io.tmpdir");
        String extra = "/opt/inlong, " + tmp + "/data ,, "; // includes blanks & empty item
        InstallerConfiguration conf = InstallerConfiguration.getInstallerConf();
        conf.set(AllowedRootsResolver.KEY_EXTRA_ALLOWED_ROOTS, extra);

        AllowedRootsResolver resolver = AllowedRootsResolver.build(conf);
        Set<Path> roots = resolver.getRoots();
        Assert.assertTrue("expected /opt/inlong to be added: " + roots,
                roots.contains(Paths.get("/opt/inlong").toAbsolutePath().normalize()));
        Assert.assertTrue("expected trimmed second entry to be added: " + roots,
                roots.contains(Paths.get(tmp, "data").toAbsolutePath().normalize()));
    }

    @Test
    public void build_extraAllowedRoots_blankValue_shouldBeIgnored() {
        InstallerConfiguration conf = InstallerConfiguration.getInstallerConf();
        conf.set(AllowedRootsResolver.KEY_EXTRA_ALLOWED_ROOTS, "   ");
        // Should not throw; simply produces the default root set.
        AllowedRootsResolver resolver = AllowedRootsResolver.build(conf);
        Assert.assertFalse(resolver.getRoots().isEmpty());
    }
}
