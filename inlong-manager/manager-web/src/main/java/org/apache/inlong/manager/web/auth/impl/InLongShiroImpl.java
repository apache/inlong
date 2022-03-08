//
// Source code recreated from a .class file by IntelliJ IDEA
// (powered by FernFlower decompiler)
//

package org.apache.inlong.manager.web.auth.impl;

import java.util.LinkedHashMap;
import java.util.Map;
import javax.servlet.Filter;
import org.apache.inlong.manager.common.auth.InLongShiro;
import org.apache.inlong.manager.service.core.UserService;
import org.apache.inlong.manager.web.auth.AuthenticationFilter;
import org.apache.inlong.manager.web.auth.WebAuthorizingRealm;
import org.apache.shiro.authc.credential.CredentialsMatcher;
import org.apache.shiro.authc.credential.HashedCredentialsMatcher;
import org.apache.shiro.mgt.SecurityManager;
import org.apache.shiro.realm.AuthorizingRealm;
import org.apache.shiro.spring.web.ShiroFilterFactoryBean;
import org.apache.shiro.web.mgt.DefaultWebSecurityManager;
import org.apache.shiro.web.mgt.WebSecurityManager;
import org.apache.shiro.web.session.mgt.DefaultWebSessionManager;
import org.apache.shiro.web.session.mgt.WebSessionManager;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

@ConditionalOnProperty(name = "type", prefix = "inlong.auth", havingValue = "default")
@Component
public class InLongShiroImpl implements InLongShiro {

    @Autowired
    private UserService userService;

    @Override
    public WebSecurityManager getWebSecurityManager() {
        return new DefaultWebSecurityManager();
    }

    @Override
    public AuthorizingRealm getShiroRealm() {
        return new WebAuthorizingRealm(userService);
    }

    @Override
    public WebSessionManager getWebSessionManager() {
        return new DefaultWebSessionManager();
    }

    @Override
    public CredentialsMatcher getCredentialsMatcher() {
        HashedCredentialsMatcher hashedCredentialsMatcher = new HashedCredentialsMatcher();
        hashedCredentialsMatcher.setHashAlgorithmName("MD5");
        hashedCredentialsMatcher.setHashIterations(1024);
        return hashedCredentialsMatcher;
    }

    @Override
    public ShiroFilterFactoryBean getShiroFilter(SecurityManager securityManager) {
        ShiroFilterFactoryBean shiroFilterFactoryBean = new ShiroFilterFactoryBean();
        shiroFilterFactoryBean.setSecurityManager(securityManager);
        // anon: can be accessed by anyone, authc: only authentication is successful can be accessed
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

        // openapi
        pathDefinitions.put("/openapi/**/*", "anon");

        pathDefinitions.put("/**", "authc");

        shiroFilterFactoryBean.setFilterChainDefinitionMap(pathDefinitions);
        return shiroFilterFactoryBean;
    }
}
