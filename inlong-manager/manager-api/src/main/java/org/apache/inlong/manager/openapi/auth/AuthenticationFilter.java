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

package org.apache.inlong.manager.openapi.auth;

import lombok.extern.slf4j.Slf4j;
import org.apache.inlong.manager.common.pojo.user.UserDetail;
import org.apache.inlong.manager.common.util.LoginUserUtil;
import org.apache.shiro.SecurityUtils;
import org.apache.shiro.subject.Subject;

import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;

@Slf4j
public class AuthenticationFilter implements Filter {

    private boolean supportMockUsername;

    public AuthenticationFilter(boolean supportMockUsername) {
        if (supportMockUsername) {
            log.warn(" Ensure that you are not using test mode in production environment.");
        }
        this.supportMockUsername = supportMockUsername;
    }

    @Override
    public void init(FilterConfig filterConfig) throws ServletException {

    }

    @Override
    public void doFilter(ServletRequest servletRequest, ServletResponse servletResponse, FilterChain filterChain)
            throws IOException, ServletException {
        HttpServletRequest httpServletRequest = (HttpServletRequest) servletRequest;

        MockAuthenticationToken mockAuthenticationToken = new MockAuthenticationToken(httpServletRequest);

        BasicAuthenticationToken token = null;
        if (supportMockUsername && (!mockAuthenticationToken.isEmpty())) {
            token = mockAuthenticationToken;
        }

        Subject subject = SecurityUtils.getSubject();
        if (token != null) {
            SecurityUtils.getSubject().login(token);
            if (log.isDebugEnabled()) {
                log.debug("Login user: " + SecurityUtils.getSubject().getPrincipal() + ", token " + token.getClass()
                        .getSimpleName());
            }
        }

        if (!subject.isAuthenticated()) {
            log.error("Access denied for anonymous user:{}  path:{} ", subject.getPrincipal(),
                    httpServletRequest.getServletPath());
            ((HttpServletResponse) servletResponse).sendError(HttpServletResponse.SC_FORBIDDEN);
            return;
        }

        LoginUserUtil.setUserLoginInfo(new UserDetail().setUserName((String) subject.getPrincipal()));
        filterChain.doFilter(servletRequest, servletResponse);
        LoginUserUtil.removeUserLoginInfo();
    }

    @Override
    public void destroy() {

    }
}
