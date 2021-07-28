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

package org.apache.inlong.manager.web.auth;

import java.util.LinkedHashMap;
import java.util.Map;
import javax.servlet.Filter;
import org.apache.inlong.manager.service.core.UserService;
import org.apache.shiro.authc.credential.HashedCredentialsMatcher;
import org.apache.shiro.mgt.SecurityManager;
import org.apache.shiro.realm.AuthorizingRealm;
import org.apache.shiro.spring.security.interceptor.AuthorizationAttributeSourceAdvisor;
import org.apache.shiro.spring.web.ShiroFilterFactoryBean;
import org.apache.shiro.web.mgt.DefaultWebSecurityManager;
import org.apache.shiro.web.mgt.WebSecurityManager;
import org.apache.shiro.web.session.mgt.DefaultWebSessionManager;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class WebShiroConfig {

    @Autowired
    private UserService userService;

    @Bean
    public AuthorizingRealm shiroRealm(HashedCredentialsMatcher matcher) {
        AuthorizingRealm authorizingRealm = new WebAuthorizingRealm(userService);
        authorizingRealm.setCredentialsMatcher(matcher);
        return authorizingRealm;
    }

    @Bean
    public WebSecurityManager securityManager(@Qualifier("hashedCredentialsMatcher")
            HashedCredentialsMatcher matcher) {
        DefaultWebSecurityManager securityManager = new DefaultWebSecurityManager();
        securityManager.setRealm(shiroRealm(matcher));
        return securityManager;
    }

    @Bean
    public DefaultWebSessionManager sessionManager() {
        DefaultWebSessionManager sessionManager = new DefaultWebSessionManager();
        sessionManager.setGlobalSessionTimeout(1000 * 60 * 60);
        return sessionManager;
    }

    @Bean(name = "hashedCredentialsMatcher")
    public HashedCredentialsMatcher hashedCredentialsMatcher() {
        HashedCredentialsMatcher hashedCredentialsMatcher = new HashedCredentialsMatcher();
        hashedCredentialsMatcher.setHashAlgorithmName("MD5");
        hashedCredentialsMatcher.setHashIterations(1024);
        return hashedCredentialsMatcher;
    }

    /**
     * Filter for annon / authc
     */
    @Bean
    public ShiroFilterFactoryBean shiroFilter(SecurityManager securityManager) {
        ShiroFilterFactoryBean shiroFilterFactoryBean = new ShiroFilterFactoryBean();
        shiroFilterFactoryBean.setSecurityManager(securityManager);
        Map<String, Filter> filters = new LinkedHashMap<>();
        filters.put("authc", new AuthenticationFilter());
        shiroFilterFactoryBean.setFilters(filters);
        Map<String, String> pathDefinitions = new LinkedHashMap<>();
        // login, register request
        pathDefinitions.put("/anno/**/*", "anon");

        // swagger api
        pathDefinitions.put("/doc.html", "anon");
        pathDefinitions.put("/v2/api-docs/**/**", "anon");
        pathDefinitions.put("/webjars/**/*", "anon");
        pathDefinitions.put("/swagger-resources/**/*", "anon");
        pathDefinitions.put("/swagger-resources", "anon");

        pathDefinitions.put("/**", "authc");

        shiroFilterFactoryBean.setFilterChainDefinitionMap(pathDefinitions);
        return shiroFilterFactoryBean;
    }

    /**
     * Enable permission verification annotation
     */
    @Bean
    public AuthorizationAttributeSourceAdvisor authorizationAttributeSourceAdvisor() {
        AuthorizationAttributeSourceAdvisor advisor = new AuthorizationAttributeSourceAdvisor();
        advisor.setSecurityManager(securityManager(hashedCredentialsMatcher()));
        return advisor;
    }
}
