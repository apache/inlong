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

package org.apache.inlong.audit.utils;

import org.apache.inlong.audit.entity.AuditRoute;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

public class RouteUtils {

    private static final Logger LOG = LoggerFactory.getLogger(RouteUtils.class);
    private static final Pattern JDBC_ADDRESS_PATTERN = Pattern.compile("^jdbc:\\w+://([\\w.-]+):(\\d+)");
    private static final String HOST_PORT_SEPARATOR = ":";
    private static final int EXPECTED_GROUP_COUNT = 2;

    public static boolean isValidRegex(String regex) {
        if (regex == null || regex.isEmpty()) {
            return false;
        }
        try {
            Pattern.compile(regex);
            return true;
        } catch (PatternSyntaxException e) {
            return false;
        }
    }

    public static boolean matchesAuditRoute(String auditId, String inlongGroupId, List<AuditRoute> auditRouteList) {
        if (auditRouteList == null || auditRouteList.isEmpty()) {
            return true;
        }
        try {
            for (AuditRoute route : auditRouteList) {
                boolean auditIdMatch = Pattern.matches(route.getAuditId(), auditId);
                boolean inlongGroupIdIsEmpty = (inlongGroupId == null || inlongGroupId.trim().isEmpty());

                boolean includeGroupId =
                        inlongGroupIdIsEmpty || Pattern.matches(route.getInlongGroupIdsInclude(), inlongGroupId);
                boolean excludeGroupId = !inlongGroupIdIsEmpty && route.getInlongGroupIdsExclude() != null
                        && !route.getInlongGroupIdsExclude().trim().isEmpty()
                        && Pattern.matches(route.getInlongGroupIdsExclude(), inlongGroupId);

                if (auditIdMatch && includeGroupId && !excludeGroupId) {
                    return true;
                }
            }
        } catch (Exception e) {
            return false;
        }
        return false;
    }

    /**
     * Extracts the host:port address from a JDBC URL.
     *
     * @param jdbcUrl the JDBC URL, e.g. "jdbc:mysql://127.0.0.1:3306/testdb"
     * @return host:port string, or null if the URL is null, blank, or does not match
     */
    public static String extractAddress(String jdbcUrl) {
        if (jdbcUrl == null || jdbcUrl.trim().isEmpty()) {
            return null;
        }
        try {
            Matcher matcher = JDBC_ADDRESS_PATTERN.matcher(jdbcUrl);
            if (matcher.find() && matcher.groupCount() >= EXPECTED_GROUP_COUNT) {
                return matcher.group(1) + HOST_PORT_SEPARATOR + matcher.group(2);
            }
        } catch (Exception e) {
            LOG.warn("Failed to extract address from JDBC URL: {}", jdbcUrl, e);
        }
        return null;
    }
}
