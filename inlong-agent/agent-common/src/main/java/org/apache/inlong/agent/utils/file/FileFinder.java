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

package org.apache.inlong.agent.utils.file;

import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.collect.Lists;

import java.io.File;
import java.util.Iterator;
import java.util.List;

/**
 * The {@link FileFinder} is used to configure the options for the search. This is done by following
 * the builder pattern. Several methods can be used to influence the search.
 * <p/>
 * To define if you want only files, only directories or both in your result you can use the {@link
 * #yieldFiles()}, {@link #yieldDirectories()} or {@link #yieldFilesAndDirectories()} methods.
 * <p/>
 * Without any options only the current directory is searched. The {@link #recursive()} and {@link
 * #recursive(Predicate)} methods allow you to enable recursive searching.
 * <p/>
 * You can search for specific filter by using {@link #withName(String)}, {@link
 * #withExtension(String)} or the custom {@link #withFileFilter(Predicate)} methods. The string
 * comparison of filenames can be fine tuned with the {@link #caseSensitive()} and {@link
 * #ignoreCase()} methods.
 * <p/>
 * All these methods can be chained together with the following limitations.
 * <ul>
 * <li>{@link #caseSensitive()} and {@link #ignoreCase()} only affect the
 * following filter definitions.
 * <li>Only one yield* makes sense. If you use multiple the last one wins.
 * </ul>
 * To finally execute the search you have two options.
 * <ul>
 * <li>Use the {@link #list()} method to execute the search in one go and get
 * all the results in a {@link List<File>}.
 * <li>Call {@link #iterator()} and get the results piece by piece. Since the
 * {@link FileFinder} implements {@link Iterable} it can even be used in a for
 * each loop.
 * </ul>
 * <p/>
 * Examples: <code><pre>
 * // Iterate over all files in the windows directory
 * for (File f : Files.find("c:\\windows")) { ... }
 * <p/>
 * // Get all the files in a directory as a list of files.
 * List<File> allFiles = Files.find(somedir).list();
 * <p/>
 * // Skip all .svn directories within a source tree
 * Predicate<File> noSvnDirs = new Predicate<File>() {
 *   boolean apply(File file) {
 *     return !file.getName().equals(".svn");
 *   }
 * }
 * for (File f : Files.find("src/java/").recursive(noSvnDir)) { ... }
 * </code></pre>
 *
 */
public class FileFinder implements Iterable<File> {

    /**
     * Predicate that returns true, when a {@link File}-object points actually to a file.
     */
    private final static Predicate<File> isFile = new Predicate<File>() {

        public boolean apply(File input) {
            return input.isFile();
        }

        ;
    };

    /**
     * Predicate that returns true, when a {@link File}-object points to a directory.
     */
    private final static Predicate<File> isDirectory = new Predicate<File>() {

        public boolean apply(File input) {
            return input.isDirectory();
        }

        ;
    };

    /**
     * The base directory which will be used for the search
     */
    private final File baseDir;

    /**
     * A filter which determines which type of files will be returned (files, directories or both).
     */
    private Predicate<File> yieldFilter = isFile;

    /**
     * A {@link Predicate}-filter which determines which will be processed recursively.
     */
    private Predicate<File> branchFilter = Predicates.alwaysFalse();

    /**
     * A filter that can be used to filter specific files.
     */
    private Predicate<File> fileFilter = Predicates.alwaysTrue();

    private Predicate<File> dirFilter = Predicates.alwaysTrue();

    /**
     * A boolean option that that defines if {@link String} comparisons are made case sensitive or
     * insensitive, e.g. for filenames and extensions. The default are case sensitive comparisons.
     */
    private boolean caseSensitive = true;

    private int maxDepth = 1;

    /**
     * Creates a new {@link FileFinder} object for a given base directory.
     *
     * @param baseDir The base directory where the search starts.
     */
    public FileFinder(File baseDir) {
        this.baseDir = baseDir;
    }

    public FileFinder withDepth(int depth) {
        maxDepth = depth;
        return this;
    }

    /**
     * Returns the result of the search as a list.
     *
     * @return A list with the files that where found.
     */
    public List<File> list() {
        return Lists.newArrayList(iterator());
    }

    /**
     * Creates an Iterator that can be used to iterate through the results of the search. Note: This
     * actually works iterative, i.e. the recursion happens as you fetch files from the iterator.
     * The result is not fetched into a huge list.
     *
     * @return An {@link Iterator<File>} that retrieves the results bit by bit.
     * @see Iterable#iterator()
     */
    public Iterator<File> iterator() {
        return new FileFinderIterator(baseDir, yieldFilter, branchFilter,
                fileFilter, dirFilter, maxDepth);
    }

    /**
     * Configures the {@link FileFinder} to return files.
     *
     * @return The current {@link FileFinder} to perform method chaining.
     */
    public FileFinder yieldFiles() {
        yieldFilter = isFile;
        return this;
    }

    /**
     * Configures the {@link FileFinder} to return directories.
     *
     * @return The current {@link FileFinder} to perform method chaining.
     */
    public FileFinder yieldDirectories() {
        yieldFilter = isDirectory;
        return this;
    }

    /**
     * Configures the {@link FileFinder} to return files and directories.
     *
     * @return The current {@link FileFinder} to perform method chaining.
     */
    public FileFinder yieldFilesAndDirectories() {
        yieldFilter = Predicates.or(isFile, isDirectory);
        return this;
    }

    /**
     * Configures the {@link FileFinder} to use case sensitive comparisons for filenames.
     *
     * @return The current {@link FileFinder} to perform method chaining.
     */
    public FileFinder caseSensitive() {
        caseSensitive = true;
        return this;
    }

    /**
     * Configures the {@link FileFinder} to ignore the case for comparisons of filenames.
     *
     * @return The current {@link FileFinder} to perform method chaining.
     */
    public FileFinder ignoreCase() {
        caseSensitive = false;
        return this;
    }

    public FileFinder withFileNameRegex(final String name) {
        return withFileFilter(new FileNameRegexMatchPredicate(name, caseSensitive));
    }

    public FileFinder withDirNameRegex(final String name) {
        return withDirFilter(new DirNameRegexMatchPredicate(name, caseSensitive));
    }

    /**
     * Enables a recursive search that processes all sub directories recursively with a depth-first
     * search.
     *
     * @return The current {@link FileFinder} to perform method chaining.
     */
    public FileFinder recursive() {
        branchFilter = Predicates.alwaysTrue();
        return this;
    }

    /**
     * Enables a recursive search that processes sub directories that match the given {@link
     * Predicate} recursively with a depth-first search.
     *
     * @param branchFilter The {@link Predicate<File>} that returns true for all directories
     *         that should be used in the search.
     * @return The current {@link FileFinder} to perform method chaining.
     */
    public FileFinder recursive(Predicate<File> branchFilter) {
        this.branchFilter = branchFilter;
        return this;
    }

    /**
     * Accepts a custom filter to search for specific files or directories. All files that match the
     * {@link Predicate} (and match the yield* file type) will be returned in the result.
     * <p/>
     * Multiple calls of {@link #withFileFilter(Predicate)} as well as other filter methods like
     * {@link #withName(String)} will be combined with an AND condition, i.e. all filters have to
     * match.
     *
     * @param filter The {@link Predicate} that should be used to filter files.
     * @return The current {@link FileFinder} to perform method chaining.
     */
    public FileFinder withFileFilter(Predicate<File> filter) {
        this.fileFilter = Predicates.and(fileFilter, filter);
        return this;
    }

    public FileFinder withDirFilter(Predicate<File> filter) {
        this.dirFilter = Predicates.and(dirFilter, filter);
        return this;
    }

    public FileFinder containingFile(final Predicate<File> fileInDir) {
        return withFileFilter(new Predicate<File>() {

            public boolean apply(File directory) {
                return directoryContainsFile(directory, fileInDir);
            }
        });
    }

    public FileFinder contains(byte[] bytes) {
        throw new IllegalArgumentException();
    }

    private boolean directoryContainsFile(File directory,
            final Predicate<File> containgFileFilter) {
        if (directory.isDirectory()) {
            File[] allFiles = directory.listFiles();
            for (File file : allFiles) {
                if (containgFileFilter.apply(file)) {
                    return true;
                }
            }
        }
        return false;
    }

}