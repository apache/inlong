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

import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

public class RouteUtils {

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

    public static String extractAddress(String jdbcUrl) {
        if (jdbcUrl == null || jdbcUrl.trim().isEmpty()) {
            return null;
        }
        Pattern pattern = Pattern.compile("^jdbc:\\w+://([\\d\\.]+):(\\d+)");
        Matcher matcher = pattern.matcher(jdbcUrl);
        if (matcher.find()) {
            return matcher.group(1) + ":" + matcher.group(2);
        }
        return null;
    }
}
