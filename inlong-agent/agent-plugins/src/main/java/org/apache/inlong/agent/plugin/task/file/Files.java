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

package org.apache.inlong.agent.plugin.task.file;

import org.apache.inlong.agent.utils.file.FileFinder;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.nio.file.attribute.BasicFileAttributes;

public class Files {

    /**
     * Finds files or sub directories within a given base directory.
     *
     * @param baseDirectory A path string representing a directory to search within.
     * @return A {@link org.apache.inlong.agent.plugin.utils.FileFinder}-Object to specify the search
     *     parameters using a builder pattern.
     */
    public static FileFinder find(String baseDirectory) {
        return find(new File(baseDirectory));
    }

    /**
     * Finds files or sub directories within a given base directory.
     *
     * @param baseDirectory A path as {@link File} object to search within.
     * @return A {@link org.apache.inlong.agent.plugin.utils.FileFinder} object to specify the search
     *     parameters using a builder pattern.
     */
    public static FileFinder find(File baseDirectory) {
        return new FileFinder(baseDirectory);
    }

    public static long getFileCreationTime(String fileName) {
        long createTime = 0L;
        try {
            createTime = java.nio.file.Files.readAttributes(Paths.get(fileName),
                    BasicFileAttributes.class).creationTime().toMillis();
        } catch (IOException ignore) {

        }
        return createTime;
    }

    public static long getFileLastModifyTime(String fileName) {
        long lastModify = 0L;
        try {
            lastModify = java.nio.file.Files.getLastModifiedTime(Paths.get(fileName)).toMillis();
        } catch (IOException ioe) {

        }
        return lastModify;
    }

}