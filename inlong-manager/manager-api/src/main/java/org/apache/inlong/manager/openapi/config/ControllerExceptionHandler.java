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

package org.apache.inlong.manager.openapi.config;


import lombok.extern.slf4j.Slf4j;
import org.apache.inlong.manager.common.beans.Response;
import org.apache.inlong.manager.common.exceptions.BusinessException;
import org.apache.inlong.manager.common.pojo.user.UserDetail;
import org.apache.inlong.manager.common.util.LoginUserUtil;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.ResponseBody;

import javax.servlet.http.HttpServletRequest;
import javax.validation.ConstraintViolation;
import javax.validation.ConstraintViolationException;
import java.util.Set;

@Slf4j
@ControllerAdvice
public class ControllerExceptionHandler {

    @ExceptionHandler(ConstraintViolationException.class)
    @ResponseBody
    public Response<String> handleConstraintViolationException(HttpServletRequest request,
            ConstraintViolationException e) {
        Set<ConstraintViolation<?>> violations = e.getConstraintViolations();
        StringBuilder stringBuilder = new StringBuilder(64);
        for (ConstraintViolation<?> violation : violations) {
            stringBuilder.append(violation.getMessage()).append(".");
        }
        UserDetail userDetail = LoginUserUtil.getLoginUserDetail();
        log.error("Failed to handle request on path: " + request.getRequestURI()
                + (userDetail != null ? ", user: " + userDetail.getUserName() : ""), e);
        return Response.fail(stringBuilder.toString());
    }

    @ExceptionHandler(MethodArgumentNotValidException.class)
    @ResponseBody
    public Response<String> handleMethodArgumentNotValidException(HttpServletRequest request,
            MethodArgumentNotValidException e) {
        UserDetail userDetail = LoginUserUtil.getLoginUserDetail();
        log.error("Failed to handle request on path: " + request.getRequestURI()
                + (userDetail != null ? ", user: " + userDetail.getUserName() : ""), e);
        return Response.fail(e.getBindingResult().getAllErrors().get(0).getDefaultMessage());
    }

    @ResponseBody
    @ExceptionHandler(value = BusinessException.class)
    public Response<String> handleBusinessExceptionHandler(HttpServletRequest request, BusinessException e) {
        UserDetail userDetail = LoginUserUtil.getLoginUserDetail();
        log.error("Failed to handle request on path:" + request.getRequestURI()
                + (userDetail != null ? ", user: " + userDetail.getUserName() : ""), e);
        return Response.fail(e.getMessage());
    }

    @ExceptionHandler(Exception.class)
    @ResponseBody
    public Response<String> handle(HttpServletRequest request, Exception e) {
        UserDetail userDetail = LoginUserUtil.getLoginUserDetail();
        log.error("Failed to handle request on path: " + request.getRequestURI()
                + (userDetail != null ? ", user: " + userDetail.getUserName() : ""), e);
        return Response.fail(e.getMessage());
    }
}
