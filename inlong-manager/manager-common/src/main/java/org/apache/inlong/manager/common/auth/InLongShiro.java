package org.apache.inlong.manager.common.auth;

import org.apache.shiro.authc.credential.CredentialsMatcher;
import org.apache.shiro.mgt.SecurityManager;
import org.apache.shiro.realm.AuthorizingRealm;
import org.apache.shiro.spring.web.ShiroFilterFactoryBean;
import org.apache.shiro.web.mgt.WebSecurityManager;
import org.apache.shiro.web.session.mgt.WebSessionManager;

public interface InLongShiro {

    WebSecurityManager getWebSecurityManager();

    AuthorizingRealm getShiroRealm();

    WebSessionManager getWebSessionManager();

    CredentialsMatcher getCredentialsMatcher();

    ShiroFilterFactoryBean getShiroFilter(SecurityManager securityManager);
}
