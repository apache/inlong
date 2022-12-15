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

package org.apache.inlong.agent.plugin.trigger;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.inlong.agent.plugin.filter.DateFormatRegex;
import org.apache.inlong.agent.utils.PathUtils;
import org.apache.inlong.agent.utils.ThreadUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Path pattern for file filter.
 * It’s identified by watchDir, which matches {@link PathPattern#whiteList} and filters {@link PathPattern#blackList}.
 */
public class PathPattern {
    private static final Logger LOGGER = LoggerFactory.getLogger(PathPattern.class);

    private final String rootDir;
    private final Set<String> subDirs;
    // regex for those files should be matched
    private final Set<DateFormatRegex> whiteList;
    // regex for those files should be filtered
    private final Set<String> blackList;

    public PathPattern(String rootDir, Set<String> whiteList, Set<String> blackList) {
        this(rootDir, whiteList, blackList, null);
    }

    public PathPattern(String rootDir, Set<String> whiteList, Set<String> blackList, String offset) {
        this.rootDir = rootDir;
        this.subDirs = new HashSet<>();
        this.blackList = blackList;
        if (offset != null && StringUtils.isNotBlank(offset)) {
            this.whiteList = whiteList.stream()
                    .map(whiteRegex -> DateFormatRegex.ofRegex(whiteRegex).withOffset(offset))
                    .collect(Collectors.toSet());
            updateDateFormatRegex();
        } else {
            this.whiteList = whiteList.stream()
                    .map(whiteRegex -> DateFormatRegex.ofRegex(whiteRegex))
                    .collect(Collectors.toSet());
        }
    }

    /**
     * walk all suitable files under directory.
     */
    private void walkAllSuitableFiles(File dirPath, final Collection<File> collectResult,
            int maxNum) throws IOException {
        if (collectResult.size() > maxNum) {
            LOGGER.warn("max num of files is {}, please check", maxNum);
            return;
        }
        // remove blacklist path
        if (blackList.contains(dirPath.toString())) {
            LOGGER.info("find blacklist path {}, ignore it.", dirPath);
            return;
        }
        Optional matched = whiteList.stream()
                .filter(whiteRegex -> whiteRegex.match(dirPath))
                .findAny();
        if (dirPath.isFile() && matched.isPresent()) {
            collectResult.add(dirPath);
        } else if (dirPath.isDirectory()) {
            try (final Stream<Path> pathStream = Files.list(dirPath.toPath())) {
                pathStream.forEach(path -> {
                    try {
                        walkAllSuitableFiles(path.toFile(), collectResult, maxNum);
                    } catch (IOException ex) {
                        LOGGER.warn("cannot add {}, please check it", path, ex);
                    }
                });
            } catch (Exception e) {
                LOGGER.error("exception caught", e);
            } catch (Throwable t) {
                ThreadUtils.threadThrowableHandler(Thread.currentThread(), t);
            }
        }
    }

    /**
     * walk root directory
     */
    public void walkAllSuitableFiles(final Collection<File> collectResult,
            int maxNum) throws IOException {
        walkAllSuitableFiles(new File(rootDir), collectResult, maxNum);
    }

    /**
     * cleanup local cache, subDirs is only used to filter duplicated directories
     * in one term watch key check.
     */
    public void cleanup() {
        subDirs.clear();
    }

    /**
     * whether path is suitable
     *
     * @param pathStr pathString
     * @return true if suit else false.
     */
    public boolean suitForWatch(String pathStr) {
        // remove blacklist path
        if (blackList.contains(pathStr)) {
            LOGGER.info("find blacklist path {}, ignore it.", pathStr);
            return false;
        }
        // remove common root path
        String briefSubDir = StringUtils.substringAfter(pathStr, rootDir);
        // if already watched, then stop deep find
        if (subDirs.contains(briefSubDir)) {
            LOGGER.info("already watched {}", pathStr);
            return false;
        }

        File path = new File(pathStr);
        if (path.isDirectory()) {
            LOGGER.info("add path {}", pathStr);
            subDirs.add(briefSubDir);
            return true;
        } else {
            return whiteList.stream()
                    .filter(whiteRegex -> whiteRegex.match(path))
                    .findAny()
                    .isPresent();
        }
    }

    /**
     * when a new file is found, update regex since time may change.
     */
    public void updateDateFormatRegex() {
        whiteList.forEach(DateFormatRegex::setRegexWithCurrentTime);
    }

    /**
     * when job is retry job, the time for searching file should be specified.
     */
    public void updateDateFormatRegex(String time) {
        whiteList.forEach(whiteRegex -> whiteRegex.setRegexWithTime(time));
    }

    @Override
    public String toString() {
        return rootDir;
    }

    @Override
    public int hashCode() {
        return HashCodeBuilder.reflectionHashCode(rootDir, false);
    }

    @Override
    public boolean equals(Object object) {
        if (object instanceof PathPattern) {
            PathPattern entity = (PathPattern) object;
            return entity.rootDir.equals(this.rootDir);
        } else {
            return false;
        }
    }

    public String getRootDir() {
        return rootDir;
    }

    public String getSuitTime() {
        return whiteList.stream().findAny().get().getFormattedTime(); // todo:适配多regex情况下的datetime
    }
}
