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

package org.apache.inlong.audit.service.utils;

/**
 * Cache utils
 */
public class CacheUtils {

    public static String buildCacheKey(String logTs, String inlongGroupId, String inlongStreamId,
            String auditId, String auditTag) {
        return new StringBuilder()
                .append(logTs)
                .append(inlongGroupId)
                .append(inlongStreamId)
                .append(auditId)
                .append(auditTag)
                .toString();
    }

    public static long calculateAverageDelay(long totalCount, long totalDelay) {
        return totalCount == 0 ? 0 : (totalDelay / Math.abs(totalCount));
    }
}
