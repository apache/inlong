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

import java.io.File;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Queue;

/**
 * Implements an {@link Iterator<File>} that runs the recursive search that has been defined by the
 * {@link FileFinder} builder object.
 *
 */
public class FileFinderIterator implements Iterator<File> {

    /**
     * The {@link Predicate} that defines the type of files that will be returned.
     */
    private final Predicate<File> yieldFilter;
    /**
     * The {@link Predicate} that is used for branching with recursion.
     */
    private final Predicate<File> branchFilter;
    /**
     * The {@link Predicate} that is used as filter to find specific files.
     */
    private final Predicate<File> fileFilter;
    private final Predicate<File> dirFilter;
    /**
     * A stack that stores all files and directories that still habe to be processed.
     */
    private LinkedList<DepthControl> depthStack = new LinkedList<DepthControl>();
    /**
     * A queue to cache results that will be offered one by one by the {@link Iterator}.
     */
    private Queue<File> resultQueue = new LinkedList<File>();
    private int maxDepth = 1;

    /**
     * Creates the Iterator with all configuration options for the search.
     *
     * @param baseDir The directory where the search will start.
     * @param yieldFilter The {@link Predicate} that defines the type of files that will be
     *         returned.
     * @param branchFilter The {@link Predicate} that is used for branching with recursion.
     * @param fileFilter The {@link Predicate} that is used as filter to find specific
     *         files.
     */
    public FileFinderIterator(File baseDir, Predicate<File> yieldFilter,
            Predicate<File> branchFilter, Predicate<File> fileFilter,
            Predicate<File> dirFilter, int maxDepth) {
        this.yieldFilter = yieldFilter;
        this.branchFilter = branchFilter;
        this.fileFilter = fileFilter;
        this.maxDepth = maxDepth;
        this.dirFilter = dirFilter;
        File[] listFiles = baseDir.listFiles();
        if (listFiles != null) {
            for (File f : listFiles) {
                depthStack.add(new DepthControl(1, f));
            }
        }
    }

    /**
     * Fills the result queue if necessary and tests if another result is available.
     *
     * @see Iterator#hasNext()
     */
    public boolean hasNext() {
        if (resultQueue.isEmpty()) {
            populateResults();
        }
        return !resultQueue.isEmpty();
    }

    /**
     * Returns the next file from the result queue.
     *
     * @see Iterator#next()
     */
    public File next() {
        if (resultQueue.isEmpty()) {
            populateResults();
        }
        return resultQueue.poll();
    }

    /**
     * Fills the result queue by processing the files and directories from fileStack.
     */
    private void populateResults() {
        while (!depthStack.isEmpty() && resultQueue.isEmpty()) {
            DepthControl currentDepthControl = depthStack.pop();
            File currentFile = currentDepthControl.getFile();
            int currentDepth = currentDepthControl.getDepth();

            if (yieldFilter.apply(currentFile)) {
                if (currentFile.isDirectory() && dirFilter.apply(currentFile)) {
                    if (branchFilter.apply(currentFile) && currentDepth < maxDepth) {
                        File[] subFiles = currentFile.listFiles();
                        if (subFiles != null) {
                            for (File f : subFiles) {
                                depthStack.add(new DepthControl(currentDepth + 1, f));
                            }
                        }
                    }
                } else if (currentFile.isFile() && fileFilter.apply(currentFile)) {
                    resultQueue.offer(currentFile);
                }
            }
        }

    }

    /**
     * The remove method of the {@link Iterator} is not implemented.
     *
     * @see Iterator#remove()
     */
    public void remove() {
        // not implemented
    }

    public class DepthControl {

        private int depth;
        private File file;

        public DepthControl(int depth, File file) {

            this.depth = depth;
            this.file = file;
        }

        public File getFile() {
            return file;
        }

        public int getDepth() {
            return depth;
        }
    }
}
