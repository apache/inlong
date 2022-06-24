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

package org.apache.inlong.manager.web.controller;

import org.apache.inlong.manager.common.beans.Response;
import org.apache.inlong.manager.common.enums.UserTypeEnum;
import org.apache.inlong.manager.common.pojo.user.LoginUser;
import org.apache.inlong.manager.common.pojo.user.UserInfo;
import org.apache.inlong.manager.common.util.JsonUtils;
import org.apache.inlong.manager.web.WebBaseTest;
import org.apache.shiro.SecurityUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MvcResult;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

class AnnoControllerTest extends WebBaseTest {

    // Password contains uppercase and lowercase numeric special characters
    private static final String TEST_PWD = "test_#$%%Y@UI$123";

    @Test
    void testLogin() throws Exception {
        LoginUser loginUser = new LoginUser();
        loginUser.setUsername("admin");
        loginUser.setPassword("inlong");

        MvcResult mvcResult = mockMvc.perform(
                        post("/anno/login")
                                .content(JsonUtils.toJsonString(loginUser))
                                .contentType(MediaType.APPLICATION_JSON_UTF8)
                                .accept(MediaType.APPLICATION_JSON)
                )
                .andExpect(status().isOk())
                .andReturn();

        String resBodyObj = getResBodyObj(mvcResult, String.class);
        Assertions.assertNotNull(resBodyObj);

        Assertions.assertTrue(SecurityUtils.getSubject().isAuthenticated());
    }

    @Test
    void testLoginFailByWrongPwd() throws Exception {
        LoginUser loginUser = new LoginUser();
        loginUser.setUsername("admin");
        // Wrong pwd
        loginUser.setPassword("test_wrong_pwd");

        MvcResult mvcResult = mockMvc.perform(
                        post("/anno/login")
                                .content(JsonUtils.toJsonString(loginUser))
                                .contentType(MediaType.APPLICATION_JSON_UTF8)
                                .accept(MediaType.APPLICATION_JSON)
                )
                .andExpect(status().isOk())
                .andReturn();

        Response<String> response = getResBody(mvcResult, String.class);
        Assertions.assertFalse(response.isSuccess());
        Assertions.assertEquals("Username or password was incorrect, or the account has expired",
                response.getErrMsg());
    }

    @Test
    void testRegister() throws Exception {
        UserInfo userInfo = UserInfo.builder()
                .username("test_name")
                .password(TEST_PWD)
                .type(UserTypeEnum.ADMIN.getCode())
                .validDays(88888)
                .build();

        MvcResult mvcResult = mockMvc.perform(
                        post("/anno/doRegister")
                                .content(JsonUtils.toJsonString(userInfo))
                                .contentType(MediaType.APPLICATION_JSON_UTF8)
                                .accept(MediaType.APPLICATION_JSON)
                )
                .andExpect(status().isOk())
                .andReturn();

        Response<Boolean> resBody = getResBody(mvcResult, Boolean.class);
        Assertions.assertTrue(resBody.isSuccess() && resBody.getData());
    }

    @Test
    void testRegisterFailByExistName() throws Exception {
        UserInfo userInfo = UserInfo.builder()
                // Username already exists in the init sql
                .username("admin")
                .password(TEST_PWD)
                .type(UserTypeEnum.ADMIN.getCode())
                .validDays(88888)
                .build();

        MvcResult mvcResult = mockMvc.perform(
                        post("/anno/doRegister")
                                .content(JsonUtils.toJsonString(userInfo))
                                .contentType(MediaType.APPLICATION_JSON_UTF8)
                                .accept(MediaType.APPLICATION_JSON)
                )
                .andExpect(status().isOk())
                .andReturn();

        Response<Boolean> resBody = getResBody(mvcResult, Boolean.class);
        Assertions.assertFalse(resBody.isSuccess());
        Assertions.assertEquals("User [admin] already exists", resBody.getErrMsg());
    }

    @Test
    void testLogout() throws Exception {
        testLogin();

        MvcResult mvcResult = mockMvc.perform(
                        get("/anno/logout")
                                .contentType(MediaType.APPLICATION_JSON_UTF8)
                                .accept(MediaType.APPLICATION_JSON)
                )
                .andExpect(status().isOk())
                .andReturn();

        Response<String> resBody = getResBody(mvcResult, String.class);
        Assertions.assertTrue(resBody.isSuccess());
        Assertions.assertFalse(SecurityUtils.getSubject().isAuthenticated());
    }

    @Test
    void testRegisterFailByInvalidType() throws Exception {
        UserInfo userInfo = UserInfo.builder()
                .username("admin11")
                .password(TEST_PWD)
                // invalidType
                .type(3)
                .validDays(88888)
                .build();

        MvcResult mvcResult = mockMvc.perform(
                        post("/anno/doRegister")
                                .content(JsonUtils.toJsonString(userInfo))
                                .contentType(MediaType.APPLICATION_JSON_UTF8)
                                .accept(MediaType.APPLICATION_JSON)
                )
                .andExpect(status().isOk())
                .andReturn();

        Response<Boolean> resBody = getResBody(mvcResult, Boolean.class);
        Assertions.assertFalse(resBody.isSuccess());
        Assertions.assertEquals("type: must in 0,1\n", resBody.getErrMsg());
    }

}
