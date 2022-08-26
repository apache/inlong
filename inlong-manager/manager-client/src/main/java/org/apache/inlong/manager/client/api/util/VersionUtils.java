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

package org.apache.inlong.manager.client.api.util;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

@Slf4j
public class VersionUtils {

    public static boolean checkInlongVersion(String current, String target) {
        InlongVersion currentVersion = new InlongVersion(current);
        InlongVersion targetVersion = new InlongVersion(target);
        return currentVersion.compareTo(targetVersion) >= 0;
    }

    @Data
    public static class InlongVersion {

        private int majorVersion;
        private int subVersion;
        private int incrementalVersion;

        public InlongVersion(String version) {
            if (StringUtils.isBlank(version)) {
                return;
            }
            try {
                String[] versionParts = version.split("\\.");
                majorVersion = Integer.parseInt(versionParts[0]);
                subVersion = Integer.parseInt(versionParts[1]);
                if (versionParts.length >= 3) {
                    incrementalVersion = Integer.parseInt(versionParts[2]);
                }
            } catch (Exception e) {
                String errMsg = String.format("Unsupported version [%s],only support verion type [1-9].[0-9].[0-9]",
                        version);
                throw new IllegalArgumentException(errMsg);
            }
        }

        public int compareTo(InlongVersion inlongVersion) {
            if (this.majorVersion < inlongVersion.getMajorVersion()) {
                return -1;
            } else if (this.majorVersion > inlongVersion.getMajorVersion()) {
                return 1;
            }
            if (this.subVersion < inlongVersion.getSubVersion()) {
                return -1;
            } else if (this.subVersion > inlongVersion.getSubVersion()) {
                return 1;
            }
            if (this.incrementalVersion < inlongVersion.getIncrementalVersion()) {
                return -1;
            } else if (this.incrementalVersion == inlongVersion.getIncrementalVersion()) {
                return 0;
            } else {
                return 1;
            }
        }
    }
}
