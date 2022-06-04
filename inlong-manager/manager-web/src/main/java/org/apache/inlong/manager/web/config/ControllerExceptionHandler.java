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

package org.apache.inlong.manager.web.config;

import lombok.extern.slf4j.Slf4j;
import org.apache.inlong.manager.common.beans.Response;
import org.apache.inlong.manager.common.exceptions.BusinessException;
import org.apache.inlong.manager.common.exceptions.WorkflowException;
import org.apache.inlong.manager.common.pojo.user.UserDetail;
import org.apache.inlong.manager.common.util.LoginUserUtils;
import org.apache.shiro.authc.AuthenticationException;
import org.apache.shiro.authz.AuthorizationException;
import org.apache.shiro.authz.UnauthorizedException;
import org.springframework.http.converter.HttpMessageConversionException;
import org.springframework.validation.BindException;
import org.springframework.validation.FieldError;
import org.springframework.validation.ObjectError;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

import javax.servlet.http.HttpServletRequest;
import javax.validation.ConstraintViolation;
import javax.validation.ConstraintViolationException;
import java.util.List;
import java.util.Set;

/**
 * Handler of controller exception.
 */
@Slf4j
@RestControllerAdvice
public class ControllerExceptionHandler {

    @ExceptionHandler(ConstraintViolationException.class)
    public Response<String> handleConstraintViolationException(HttpServletRequest request,
            ConstraintViolationException e) {
        UserDetail userDetail = LoginUserUtils.getLoginUserDetail();
        log.error("Failed to handle request on path:{}, user:{}", request.getRequestURI(),
                (userDetail != null ? userDetail.getUserName() : ""), e);

        Set<ConstraintViolation<?>> violations = e.getConstraintViolations();
        StringBuilder stringBuilder = new StringBuilder(64);
        for (ConstraintViolation<?> violation : violations) {
            stringBuilder.append(violation.getMessage()).append(".");
        }

        return Response.fail(stringBuilder.toString());
    }

    @ExceptionHandler(MethodArgumentNotValidException.class)
    public Response<String> handleMethodArgumentNotValidException(HttpServletRequest request,
            MethodArgumentNotValidException e) {
        UserDetail userDetail = LoginUserUtils.getLoginUserDetail();
        log.error("Failed to handle request on path:{}, user:{}", request.getRequestURI(),
                (userDetail != null ? userDetail.getUserName() : ""), e);

        StringBuilder stringBuilder = new StringBuilder();
        List<FieldError> fieldErrors = e.getBindingResult().getFieldErrors();
        fieldErrors.forEach(
                fieldError -> stringBuilder.append(fieldError.getField())
                        .append(":")
                        .append(fieldError.getDefaultMessage()).append(System.lineSeparator())
        );

        List<ObjectError> globalErrors = e.getBindingResult().getGlobalErrors();
        globalErrors.forEach(
                objectError -> stringBuilder.append(objectError.getDefaultMessage()).append(System.lineSeparator())
        );

        return Response.fail(stringBuilder.toString());
    }

    @ExceptionHandler(value = IllegalArgumentException.class)
    public Response<String> handleIllegalArgumentException(HttpServletRequest request, IllegalArgumentException e) {
        UserDetail userDetail = LoginUserUtils.getLoginUserDetail();
        log.error("Failed to handle request on path:{}, user:{}", request.getRequestURI(),
                (userDetail != null ? userDetail.getUserName() : ""), e);
        return Response.fail(e.getMessage());
    }

    @ExceptionHandler(value = BindException.class)
    public Response<String> handleBindExceptionHandler(HttpServletRequest request, BindException e) {
        UserDetail userDetail = LoginUserUtils.getLoginUserDetail();
        log.error("Failed to handle request on path:{}, user:{}", request.getRequestURI(),
                (userDetail != null ? userDetail.getUserName() : ""), e);

        StringBuilder sb = new StringBuilder();
        e.getBindingResult().getFieldErrors()
                .forEach(
                        fieldError -> sb.append(fieldError.getField())
                                .append(":")
                                .append(fieldError.getDefaultMessage()).append(System.lineSeparator())
                );
        return Response.fail(sb.toString());
    }

    @ExceptionHandler(value = HttpMessageConversionException.class)
    public Response<String> handleHttpMessageConversionExceptionHandler(HttpServletRequest request,
            HttpMessageConversionException e) {
        UserDetail userDetail = LoginUserUtils.getLoginUserDetail();
        log.error("Failed to handle request on path:" + request.getRequestURI()
                + (userDetail != null ? ", user:" + userDetail.getUserName() : ""), e);
        return Response.fail("http message convert exception! pls check params");
    }

    @ExceptionHandler(value = WorkflowException.class)
    public Response<String> handleWorkflowException(HttpServletRequest request, WorkflowException e) {
        UserDetail userDetail = LoginUserUtils.getLoginUserDetail();
        log.error("Failed to handle request on path:{}, user:{}", request.getRequestURI(),
                (userDetail != null ? userDetail.getUserName() : ""), e);
        return Response.fail(e.getMessage());
    }

    @ExceptionHandler(value = BusinessException.class)
    public Response<String> handleBusinessExceptionHandler(HttpServletRequest request, BusinessException e) {
        UserDetail userDetail = LoginUserUtils.getLoginUserDetail();
        log.error("Failed to handle request on path:{}, user:{}", request.getRequestURI(),
                (userDetail != null ? userDetail.getUserName() : ""), e);
        return Response.fail(e.getMessage());
    }

    @ExceptionHandler(value = AuthenticationException.class)
    public Response<String> handleAuthenticationException(HttpServletRequest request, AuthenticationException e) {
        log.error("Failed to handle request on path:{}", request.getRequestURI(), e);
        return Response.fail("username or password is incorrect, or the account has expired");
    }

    @ExceptionHandler(value = UnauthorizedException.class)
    public Response<String> handleUnauthorizedException(HttpServletRequest request, AuthorizationException e) {
        log.error("Failed to handle request on path:{}", request.getRequestURI(), e);
        UserDetail userDetail = LoginUserUtils.getLoginUserDetail();
        return Response.fail(String.format("Current user [%s] has no permission to access URL",
                (userDetail != null ? userDetail.getUserName() : "")));
    }

    @ExceptionHandler(Exception.class)
    public Response<String> handle(HttpServletRequest request, Exception e) {
        UserDetail userDetail = LoginUserUtils.getLoginUserDetail();
        log.error("Failed to handle request on path:{}, user:{}", request.getRequestURI(),
                (userDetail != null ? userDetail.getUserName() : ""), e);
        return Response.fail("There was an error in the service..."
                + "Please try again later! "
                + "If there are still problems, please contact the administrator");
    }
}
