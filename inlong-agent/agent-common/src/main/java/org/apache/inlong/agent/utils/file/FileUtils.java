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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.attribute.BasicFileAttributes;

public class FileUtils {

    private static final Logger LOGGER = LoggerFactory.getLogger(FileUtils.class);

    /**
     * Finds files or sub directories within a given base directory.
     *
     * @param baseDirectory A path string representing a directory to search within.
     * @return A {@link FileFinder}-Object to specify the search parameters using a builder
     *         pattern.
     */
    public static FileFinder find(String baseDirectory) {
        return find(new File(baseDirectory));
    }

    /**
     * Finds files or sub directories within a given base directory.
     *
     * @param baseDirectory A path as {@link File} object to search within.
     * @return A {@link FileFinder} object to specify the search parameters using a builder
     *         pattern.
     */
    public static FileFinder find(File baseDirectory) {
        return new FileFinder(baseDirectory);
    }

    public static long getFileCreationTime(String fileName) {
        long creationTime = 0L;
        try {
            creationTime = Files.readAttributes(Paths.get(fileName), BasicFileAttributes.class).creationTime()
                    .toMillis();
        } catch (IOException e) {
            LOGGER.error("getFileCreationTime error.", e);
        }
        return creationTime;
    }

    public static long getFileLastModifyTime(String fileName) {
        long lastModify = 0L;
        try {
            lastModify = Files.getLastModifiedTime(Paths.get(fileName)).toMillis();
        } catch (IOException e) {
            LOGGER.error("getFileLastModifyTime error.", e);
        }
        return lastModify;
    }

}