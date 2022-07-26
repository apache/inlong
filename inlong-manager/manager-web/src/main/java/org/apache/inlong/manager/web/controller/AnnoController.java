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

import io.swagger.annotations.Api;
import lombok.extern.slf4j.Slf4j;
import org.apache.inlong.manager.common.beans.Response;
import org.apache.inlong.manager.common.pojo.user.UserLoginRequest;
import org.apache.inlong.manager.common.pojo.user.UserRequest;
import org.apache.inlong.manager.common.pojo.user.UserInfo;
import org.apache.inlong.manager.common.util.LoginUserUtils;
import org.apache.inlong.manager.service.core.UserService;
import org.apache.shiro.SecurityUtils;
import org.apache.shiro.authc.UsernamePasswordToken;
import org.apache.shiro.subject.Subject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

/**
 * Anno controller, such as login, register, etc.
 */
@Slf4j
@RestController
@Api(tags = "User-Anno-API")
public class AnnoController {

    @Autowired
    UserService userService;

    @PostMapping("/anno/login")
    public Response<String> login(@Validated @RequestBody UserLoginRequest loginRequest) {
        Subject subject = SecurityUtils.getSubject();
        UsernamePasswordToken token = new UsernamePasswordToken(loginRequest.getUsername(), loginRequest.getPassword());
        subject.login(token);
        LoginUserUtils.setUserLoginInfo((UserInfo) subject.getPrincipal());

        return Response.success("success");
    }

    @PostMapping("/anno/register")
    public Response<Integer> register(@Validated @RequestBody UserRequest request) {
        return Response.success(userService.save(request));
    }

    @GetMapping("/anno/logout")
    public Response<String> logout() {
        SecurityUtils.getSubject().logout();
        return Response.success("success");
    }

}
