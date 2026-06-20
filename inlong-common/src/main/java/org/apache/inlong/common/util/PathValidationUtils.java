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

package org.apache.inlong.common.util;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

import java.util.Set;

import static org.apache.inlong.common.constant.Constants.SLASH;
import static org.apache.inlong.common.constant.Constants.SLASH_CHAR;

/**
 * Utility class for validating file path patterns, providing protection
 * against path traversal attacks and allowed-directory enforcement.
 */
public class PathValidationUtils {

    /**
     * Check whether the given path contains path traversal patterns.
     * A traversal is detected when ".." appears as a complete path segment.
     *
     * @param path the path string to validate, may be null or empty
     * @return true if the path contains a traversal segment, false otherwise
     */
    public static boolean containsPathTraversal(String path) {
        if (path == null || path.isEmpty()) {
            return false;
        }
        // Normalize backslashes to forward slashes and split into segments
        String normalized = path.replace('\\', '/');
        for (String segment : normalized.split("/")) {
            if ("..".equals(segment)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Check whether the given path starts with any of the allowed directory prefixes.
     * When allowedDirs is null or empty, all paths are considered allowed (no restriction).
     *
     * @param path        the path to validate, may be null or empty
     * @param allowedDirs the set of allowed directory prefixes (normalized);
     *                    if null or empty, the check passes for any path
     * @return true if the path is within an allowed directory or no restrictions exist
     */
    public static boolean isWithinAllowedDirs(String path, Set<String> allowedDirs) {
        if (StringUtils.isBlank(path)) {
            return false;
        }
        if (CollectionUtils.isEmpty(allowedDirs)) {
            return true;
        }
        String normalizedPath = path.replace('\\', SLASH_CHAR);
        for (String allowedDir : allowedDirs) {
            if (allowedDir == null || allowedDir.isEmpty()) {
                continue;
            }
            String normalizedAllowedDir = allowedDir.replace('\\', SLASH_CHAR);
            if (!normalizedAllowedDir.endsWith(SLASH)) {
                normalizedAllowedDir += SLASH;
            }
            if ((normalizedPath + SLASH).startsWith(normalizedAllowedDir)) {
                return true;
            }
        }
        return false;
    }
}
