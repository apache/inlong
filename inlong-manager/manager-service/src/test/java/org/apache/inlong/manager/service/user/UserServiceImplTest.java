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

package org.apache.inlong.manager.service.user;

import org.apache.inlong.manager.common.consts.InlongConstants;
import org.apache.inlong.manager.common.enums.TenantUserTypeEnum;
import org.apache.inlong.manager.common.util.Preconditions;
import org.apache.inlong.manager.pojo.user.UserInfo;
import org.apache.inlong.manager.pojo.user.UserRoleCode;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Test for {@link UserServiceImpl}
 */
public class UserServiceImplTest {

    private final UserServiceImpl userService = new UserServiceImpl();

    @Test
    public void testAdminAccountTypeAlwaysTrue() {
        UserInfo userInfo = new UserInfo();
        userInfo.setAccountType(TenantUserTypeEnum.TENANT_ADMIN.getCode());
        userInfo.setName("admin");

        boolean result = userService.isAdminOrInCharge(userInfo, "someone", "other");
        assertTrue(result);
    }

    @Test
    public void testAdminRoleOverridesAccountType() {
        UserInfo userInfo = new UserInfo();
        userInfo.setAccountType(TenantUserTypeEnum.TENANT_OPERATOR.getCode());
        userInfo.setName("user");
        Set<String> roles = new HashSet<>();
        roles.add(UserRoleCode.INLONG_ADMIN);
        userInfo.setRoles(roles);

        boolean result = userService.isAdminOrInCharge(userInfo, "user", "other");
        assertTrue(result);
    }

    @Test
    public void testInChargeMatchesWhenUsernameBlank() {
        UserInfo userInfo = new UserInfo();
        userInfo.setAccountType(TenantUserTypeEnum.TENANT_OPERATOR.getCode());
        userInfo.setName("alice");
        userInfo.setRoles(Collections.emptySet());

        String inCharges = String.join(InlongConstants.COMMA, "bob", "alice");
        boolean result = userService.isAdminOrInCharge(userInfo, "", inCharges);
        assertTrue(result);
    }

    @Test
    public void testNotInChargeReturnsFalse() {
        UserInfo userInfo = new UserInfo();
        userInfo.setAccountType(TenantUserTypeEnum.TENANT_OPERATOR.getCode());
        userInfo.setName("alice");
        userInfo.setRoles(Collections.emptySet());

        String inCharges = String.join(InlongConstants.COMMA, "bob", "carol");
        boolean result = userService.isAdminOrInCharge(userInfo, "alice", inCharges);
        assertFalse(result);
    }

    @Test
    public void testInSeparatedStringEquivalentToSplitContains() {
        // Explicitly verify Preconditions.inSeparatedString behaves like split + List.contains
        String inCharges = String.join(InlongConstants.COMMA, "alice", "bob", "carol");
        String username = "bob";
        boolean expected = Arrays.asList(inCharges.split(InlongConstants.COMMA)).contains(username);
        boolean actual = Preconditions.inSeparatedString(username, inCharges, InlongConstants.COMMA);
        assertEquals(expected, actual);

        username = "dave";
        expected = Arrays.asList(inCharges.split(InlongConstants.COMMA)).contains(username);
        actual = Preconditions.inSeparatedString(username, inCharges, InlongConstants.COMMA);
        assertEquals(expected, actual);

        inCharges = "";
        username = "alice";
        expected = Arrays.asList(inCharges.split(InlongConstants.COMMA)).contains(username);
        actual = Preconditions.inSeparatedString(username, inCharges, InlongConstants.COMMA);
        assertEquals(expected, actual);
    }
}