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

import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class RouteUtilsTest {

    @Test
    public void extractAddress_ValidJdbcUrl() {
        String jdbcUrl = "jdbc:mysql://127.0.0.1:3306/testdb";
        String result = RouteUtils.extractAddress(jdbcUrl);
        assertEquals("127.0.0.1:3306", result);
    }

    @Test
    public void extractAddress_InvalidJdbcUrl_NoPort() {
        String jdbcUrl = "jdbc:mysql://127.0.0.1/testdb";
        String result = RouteUtils.extractAddress(jdbcUrl);
        assertNull(result);
    }

    @Test
    public void extractAddress_InvalidJdbcUrl_NoProtocol() {
        String jdbcUrl = "127.0.0.1:3306/testdb";
        String result = RouteUtils.extractAddress(jdbcUrl);
        assertNull(result);
    }

    @Test
    public void extractAddress_EmptyJdbcUrl() {
        String jdbcUrl = "";
        String result = RouteUtils.extractAddress(jdbcUrl);
        assertNull(result);
    }

    @Test
    public void extractAddress_NullJdbcUrl() {
        String result = RouteUtils.extractAddress(null);
        assertNull(result);
    }

    @Test
    public void extractAddress_ValidJdbcUrlWithDifferentProtocol() {
        String jdbcUrl = "jdbc:postgresql://192.168.1.100:5432/mydb";
        String result = RouteUtils.extractAddress(jdbcUrl);
        assertEquals("192.168.1.100:5432", result);
    }

    @Test
    public void matchesAuditRoute_EmptyAuditRouteList() {
        List<AuditRoute> auditRouteList = new ArrayList<>();
        boolean result = RouteUtils.matchesAuditRoute("auditId1", "groupId1", auditRouteList);
        assertTrue(result);
    }

    @Test
    public void matchesAuditRoute_NullAuditRouteList() {
        boolean result = RouteUtils.matchesAuditRoute("auditId1", "groupId1", null);
        assertTrue(result);
    }

    @Test
    public void matchesAuditRoute_MatchingIncludeAndExclude() {
        List<AuditRoute> auditRouteList = new ArrayList<>();
        AuditRoute route = new AuditRoute();
        route.setAuditId("1");
        route.setInlongGroupIdsInclude("groupId.*");
        route.setInlongGroupIdsExclude("groupId2");
        auditRouteList.add(route);

        boolean result = RouteUtils.matchesAuditRoute("1", "groupId1", auditRouteList);
        assertTrue(result);
    }

    @Test
    public void matchesAuditRoute_ExcludeRegexMatches() {
        List<AuditRoute> auditRouteList = new ArrayList<>();
        AuditRoute route = new AuditRoute();
        route.setAuditId("1");
        route.setInlongGroupIdsInclude("groupId1");
        route.setInlongGroupIdsExclude("groupId1");
        auditRouteList.add(route);

        boolean result = RouteUtils.matchesAuditRoute("1", "groupId1", auditRouteList);
        assertFalse(result);
    }

    @Test
    public void matchesAuditRoute_NoMatchingAuditId() {
        List<AuditRoute> auditRouteList = new ArrayList<>();
        AuditRoute route = new AuditRoute();
        route.setAuditId("1");
        route.setInlongGroupIdsInclude("groupId1");
        auditRouteList.add(route);

        boolean result = RouteUtils.matchesAuditRoute("2", "groupId1", auditRouteList);
        assertFalse(result);
    }

    @Test
    public void matchesAuditRoute_NoMatchingIncludeRegex() {
        List<AuditRoute> auditRouteList = new ArrayList<>();
        AuditRoute route = new AuditRoute();
        route.setAuditId("1");
        route.setInlongGroupIdsInclude("groupId2");
        auditRouteList.add(route);

        boolean result = RouteUtils.matchesAuditRoute("1", "groupId1", auditRouteList);
        assertFalse(result);
    }

    @Test
    public void matchesAuditRoute_IncludeRegex() {
        List<AuditRoute> auditRouteList = new ArrayList<>();
        AuditRoute route = new AuditRoute();
        route.setAuditId("1");
        route.setInlongGroupIdsInclude("groupId[0-9]+");
        route.setInlongGroupIdsExclude(null);
        auditRouteList.add(route);

        boolean result = RouteUtils.matchesAuditRoute("1", "groupId123", auditRouteList);
        assertTrue(result);

        result = RouteUtils.matchesAuditRoute("1", "groupIdx", auditRouteList);
        assertFalse(result);
    }

    @Test
    public void matchesAuditRoute_ExcludeRegex() {
        List<AuditRoute> auditRouteList = new ArrayList<>();
        AuditRoute route = new AuditRoute();
        route.setAuditId("1");
        route.setInlongGroupIdsInclude("groupId.*");
        route.setInlongGroupIdsExclude("groupId[0-9]+");
        auditRouteList.add(route);

        boolean result = RouteUtils.matchesAuditRoute("1", "groupId123", auditRouteList);
        assertFalse(result);

        result = RouteUtils.matchesAuditRoute("1", "groupIdABC", auditRouteList);
        assertTrue(result);
    }
    @Test
    public void matchesAuditRoute_AllRegex() {
        List<AuditRoute> auditRouteList = new ArrayList<>();
        AuditRoute route = new AuditRoute();
        route.setAuditId("*");
        route.setInlongGroupIdsInclude("groupId.*");
        route.setInlongGroupIdsExclude("groupId[0-9]+");
        auditRouteList.add(route);

        boolean result = RouteUtils.matchesAuditRoute("1", "groupId123", auditRouteList);
        assertFalse(result);

        result = RouteUtils.matchesAuditRoute("1", "groupIdABC", auditRouteList);
        assertFalse(result);
    }
}
