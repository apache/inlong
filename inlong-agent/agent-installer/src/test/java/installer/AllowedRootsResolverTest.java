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

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Comparator;
import java.util.Set;
import java.util.stream.Stream;

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
                roots.contains(toRealOrNormalized(Paths.get(tmpDir).toAbsolutePath())));
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
                toRealOrNormalized(Paths.get(System.getProperty("java.io.tmpdir")).toAbsolutePath())));
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

    // ------------------------------------------------------------------
    // Symlink bypass defence
    // ------------------------------------------------------------------

    /**
     * When a path exists, {@link AllowedRootsResolver#isUnderAllowedRoot(Path)} must use
     * {@code Path#toRealPath()} to defeat symlink-based bypass. A symlink inside the allowed
     * root that points outside must not grant access to the outside target.
     */
    @Test
    public void symlinkBypass_existingPath_shouldBeRejected() throws IOException {
        Path base = Files.createTempDirectory("allowedroot-test-");
        try {
            Path root = base.resolve("safe");
            Files.createDirectory(root);

            // Create a directory outside the allowed root.
            Path outside = base.resolve("outside");
            Files.createDirectory(outside);
            Files.createFile(outside.resolve("secrets.txt"));

            // Create a symlink inside the root pointing to the outside dir.
            Path symlink = root.resolve("workspace");
            Files.createSymbolicLink(symlink, outside);

            AllowedRootsResolver resolver = AllowedRootsResolver.ofPaths(root);

            // workspace/secrets.txt → outside/secrets.txt (outside root) → must reject.
            Path evilPath = symlink.resolve("secrets.txt");
            Assert.assertTrue("should exist for this test", Files.exists(evilPath));
            Assert.assertFalse("symlink to outside root must be rejected for existing path",
                    resolver.isUnderAllowedRoot(evilPath));
        } finally {
            deleteRecursively(base);
        }
    }

    /**
     * When the target path does not exist yet (e.g. a {@code mkdir} payload) but an ancestor
     * directory is a symlink pointing outside the allowed root, the resolver must walk up to
     * the nearest existing parent, resolve its real path, and reject the spliced result.
     */
    @Test
    public void symlinkBypass_nonExistingPath_shouldBeRejected() throws IOException {
        Path base = Files.createTempDirectory("allowedroot-test-");
        try {
            Path root = base.resolve("safe");
            Files.createDirectory(root);

            // Outside directory acting as the symlink target.
            Path outside = base.resolve("outside");
            Files.createDirectory(outside);

            // Symlink inside the allowed root → outside directory.
            Path symlink = root.resolve("workspace");
            Files.createSymbolicLink(symlink, outside);

            AllowedRootsResolver resolver = AllowedRootsResolver.ofPaths(root);

            // workspace/malicious does not exist; workspace → outside → parent resolves
            // to outside, remainder = malicious, must be rejected.
            Path nonExistent = symlink.resolve("malicious");
            Assert.assertFalse("should not exist for this test", Files.exists(nonExistent));
            Assert.assertFalse("symlink parent pointing outside root must be rejected for non-existing path",
                    resolver.isUnderAllowedRoot(nonExistent));
        } finally {
            deleteRecursively(base);
        }
    }

    /**
     * A non-existent path whose existing ancestors are all genuine (no symlink) and lie
     * under the allowed root must be accepted &mdash; this is the normal {@code mkdir} case.
     */
    @Test
    public void nonExistingPath_underGenuineRoot_shouldBeAccepted() throws IOException {
        Path base = Files.createTempDirectory("allowedroot-test-");
        try {
            Path root = base.resolve("safe");
            Files.createDirectory(root);

            AllowedRootsResolver resolver = AllowedRootsResolver.ofPaths(root);

            // safe/new_dir does not exist, parent "safe" is genuine → must accept.
            Path newDir = root.resolve("new_dir");
            Assert.assertFalse("should not exist for this test", Files.exists(newDir));
            Assert.assertTrue("non-existent path under genuine root must be accepted",
                    resolver.isUnderAllowedRoot(newDir));
        } finally {
            deleteRecursively(base);
        }
    }

    /**
     * When no part of the path exists at all (including every ancestor up to the root),
     * the resolver falls back to {@link Path#normalize()} and must still reject traversal
     * attempts.
     */
    @Test
    public void fullyNonExistentPath_shouldFallBackToNormalize() throws IOException {
        Path base = Files.createTempDirectory("allowedroot-test-");
        try {
            Path root = base.resolve("safe");
            Files.createDirectory(root);

            AllowedRootsResolver resolver = AllowedRootsResolver.ofPaths(root);

            // /tmp/.../completely/nowhere does not exist and neither does any ancestor.
            // Fallback to normalize: "../evil" resolves to outside root → reject.
            Path nonExistent = base.resolve("completely/nowhere/../evil");
            Assert.assertFalse("should not exist for this test", Files.exists(nonExistent));
            Assert.assertFalse("path traversal via .. must be rejected in fallback path",
                    resolver.isUnderAllowedRoot(nonExistent));
        } finally {
            deleteRecursively(base);
        }
    }

    /**
     * {@code ofPaths} with no argument, a {@code null} array, or a {@code null} element must
     * all succeed and simply produce an empty root set (or skip the null element). Prevents a
     * future refactor from dropping the defensive null-checks.
     */
    @Test
    public void ofPaths_nullAndEmptyInputs_shouldNotThrow() {
        Assert.assertTrue("no argument should yield empty roots",
                AllowedRootsResolver.ofPaths().getRoots().isEmpty());
        Assert.assertTrue("null Path[] should yield empty roots",
                AllowedRootsResolver.ofPaths((Path[]) null).getRoots().isEmpty());

        Path root = Paths.get(System.getProperty("java.io.tmpdir"), "inlong-test-root");
        AllowedRootsResolver resolver = AllowedRootsResolver.ofPaths(root, null);
        Assert.assertTrue("non-null element must still be honoured",
                resolver.isUnderAllowedRoot(root.resolve("child")));
    }

    /**
     * A root that does not yet exist on disk (only the lexical form was stored) must still
     * match itself. This pins the "branch ③ lexical fallback" positive case.
     */
    @Test
    public void isUnderAllowedRoot_rootItselfWhenRootMissing_shouldMatch() {
        Path root = Paths.get(System.getProperty("java.io.tmpdir"),
                "inlong-nonexistent-root-" + System.nanoTime());
        Assert.assertFalse("precondition: root must not exist", Files.exists(root));

        AllowedRootsResolver resolver = AllowedRootsResolver.ofPaths(root);
        Assert.assertTrue("root itself must match even when it does not yet exist",
                resolver.isUnderAllowedRoot(root));
    }

    /**
     * A relative path input must be resolved against the current working directory before
     * the containment check. Guards against a refactor that accidentally compares raw
     * relative paths against absolute roots.
     */
    @Test
    public void isUnderAllowedRoot_relativePath_shouldBeResolvedAgainstCwd() {
        Path cwd = Paths.get("").toAbsolutePath();
        AllowedRootsResolver resolver = AllowedRootsResolver.ofPaths(cwd);

        Path relative = Paths.get("relative-child-" + System.nanoTime());
        Assert.assertFalse(relative.isAbsolute());
        Assert.assertTrue("relative path must resolve under the CWD root",
                resolver.isUnderAllowedRoot(relative));
    }

    /**
     * End-to-end check that a root injected via {@code installer.command.extraAllowedRoots}
     * is not only present in {@code getRoots()} but also accepted by
     * {@link AllowedRootsResolver#isUnderAllowedRoot(Path)}.
     */
    @Test
    public void build_extraAllowedRoots_shouldBeUsedByIsUnderAllowedRoot() {
        String extraRoot = Paths.get(System.getProperty("java.io.tmpdir"),
                "extra-root-" + System.nanoTime()).toAbsolutePath().normalize().toString();
        InstallerConfiguration conf = InstallerConfiguration.getInstallerConf();
        conf.set(AllowedRootsResolver.KEY_EXTRA_ALLOWED_ROOTS, extraRoot);

        AllowedRootsResolver resolver = AllowedRootsResolver.build(conf);
        Assert.assertTrue("child of extra root must be accepted",
                resolver.isUnderAllowedRoot(Paths.get(extraRoot, "child")));
    }

    /**
     * A CSV made entirely of commas and blanks must not add any spurious root. Exercises the
     * inner {@code for} loop's per-item {@code isNotBlank} guard, which the existing
     * "blankValue" test does not reach (that one is short-circuited by the outer check).
     */
    @Test
    public void build_extraAllowedRoots_allBlankItems_shouldAddNoRoot() {
        System.clearProperty(AgentConstants.AGENT_HOME);
        InstallerConfiguration conf = InstallerConfiguration.getInstallerConf();
        conf.set(AgentConstants.AGENT_HOME, null);

        // Baseline: no extra roots configured.
        conf.set(AllowedRootsResolver.KEY_EXTRA_ALLOWED_ROOTS, null);
        Set<Path> baseline = AllowedRootsResolver.build(conf).getRoots();

        // Same config, but the extra-roots CSV is all blanks/commas.
        conf.set(AllowedRootsResolver.KEY_EXTRA_ALLOWED_ROOTS, ",, , ,");
        Set<Path> withBlankCsv = AllowedRootsResolver.build(conf).getRoots();

        Assert.assertEquals("CSV of only blanks/commas must not add roots",
                baseline, withBlankCsv);
    }

    /**
     * A root whose canonical location differs from its lexical form (e.g. on macOS
     * {@code /tmp} is a symlink to {@code /private/tmp}) must still match children queried
     * via either form. This regression-guards the cross-platform issue where addRoot stores
     * both the normalized and real path forms.
     */
    @Test
    public void isUnderAllowedRoot_rootBehindSymlink_shouldMatchViaEitherForm() throws IOException {
        Path base = Files.createTempDirectory("allowedroot-symlink-root-");
        try {
            Path realDir = base.resolve("real");
            Files.createDirectory(realDir);
            Path linkDir = base.resolve("link");
            try {
                Files.createSymbolicLink(linkDir, realDir);
            } catch (UnsupportedOperationException | IOException e) {
                // Filesystem does not support symlinks; nothing to verify.
                return;
            }

            // Register the root via the symlinked path.
            AllowedRootsResolver resolver = AllowedRootsResolver.ofPaths(linkDir);

            Files.createFile(linkDir.resolve("file.txt"));

            Assert.assertTrue("child queried via symlink path must match",
                    resolver.isUnderAllowedRoot(linkDir.resolve("file.txt")));
            Assert.assertTrue("child queried via real path must also match",
                    resolver.isUnderAllowedRoot(realDir.resolve("file.txt")));
        } finally {
            deleteRecursively(base);
        }
    }

    // ------------------------------------------------------------------
    // Helpers
    // ------------------------------------------------------------------

    /** Resolve to real path when the path exists, otherwise normalize. */
    private static Path toRealOrNormalized(Path p) {
        try {
            return p.toRealPath();
        } catch (IOException e) {
            return p.normalize();
        }
    }

    private static void deleteRecursively(Path dir) throws IOException {
        if (Files.exists(dir)) {
            try (Stream<Path> stream = Files.walk(dir)) {
                stream.sorted(Comparator.reverseOrder())
                        .forEach(p -> {
                            try {
                                Files.delete(p);
                            } catch (IOException ignored) {
                            }
                        });
            }
        }
    }
}
