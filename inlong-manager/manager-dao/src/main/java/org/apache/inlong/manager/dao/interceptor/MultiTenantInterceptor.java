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

package org.apache.inlong.manager.dao.interceptor;

import org.apache.inlong.manager.common.consts.InlongConstants;
import org.apache.inlong.manager.pojo.user.LoginUserUtils;
import org.apache.inlong.manager.pojo.user.UserInfo;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.parser.CCJSqlParserManager;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import org.apache.commons.lang3.StringUtils;
import org.apache.ibatis.executor.statement.StatementHandler;
import org.apache.ibatis.mapping.BoundSql;
import org.apache.ibatis.mapping.MappedStatement;
import org.apache.ibatis.plugin.Interceptor;
import org.apache.ibatis.plugin.Intercepts;
import org.apache.ibatis.plugin.Invocation;
import org.apache.ibatis.plugin.Plugin;
import org.apache.ibatis.plugin.Signature;
import org.apache.ibatis.reflection.DefaultReflectorFactory;
import org.apache.ibatis.reflection.MetaObject;
import org.apache.ibatis.reflection.SystemMetaObject;

import java.io.StringReader;
import java.sql.Connection;
import java.util.Properties;

/**
 * Interceptor for multi-tenant.
 */
@Intercepts({
        @Signature(type = StatementHandler.class, method = "prepare", args = {Connection.class, Integer.class})
})
public class MultiTenantInterceptor implements Interceptor {

    private static final String TENANT_CONDITION = "tenant=";

    @Override
    public Object intercept(Invocation invocation) throws Throwable {
        StatementHandler statementHandler = (StatementHandler) invocation.getTarget();
        MetaObject metaObject = MetaObject.forObject(statementHandler, SystemMetaObject.DEFAULT_OBJECT_FACTORY,
                SystemMetaObject.DEFAULT_OBJECT_WRAPPER_FACTORY, new DefaultReflectorFactory());
        MappedStatement mappedStatement = (MappedStatement) metaObject.getValue("delegate.mappedStatement");

        String fullMethodName = mappedStatement.getId();
        if (!MultiTenantQueryFilter.isMultiTenantQuery(fullMethodName.split(InlongConstants.UNDERSCORE)[0])) {
            return invocation.proceed();
        }

        BoundSql boundSql = statementHandler.getBoundSql();
        String sql = boundSql.getSql();

        CCJSqlParserManager parserManager = new CCJSqlParserManager();
        Select select = (Select) parserManager.parse(new StringReader(sql));
        PlainSelect plain = (PlainSelect) select.getSelectBody();

        StringBuilder whereSql = new StringBuilder();
        whereSql.append(TENANT_CONDITION).append(getTenant());

        Expression where = plain.getWhere();
        if (where == null) {
            Expression expression = CCJSqlParserUtil.parseCondExpression(whereSql.toString());
            plain.setWhere(expression);
        } else {
            if (where.toString().contains(TENANT_CONDITION)) {
                return invocation.proceed();
            }

            // else, append the tenant condition
            whereSql.append(" and ( ").append(where).append(" )");
            Expression expression = CCJSqlParserUtil.parseCondExpression(whereSql.toString());
            plain.setWhere(expression);
        }
        metaObject.setValue("delegate.boundSql.sql", select.toString());
        return invocation.proceed();
    }

    private static String getTenant() {
        UserInfo userInfo = LoginUserUtils.getLoginUser();
        if (userInfo == null) {
            throw new IllegalStateException("current login user is null, please login first");
        }
        String tenant = userInfo.getTenant();
        if (StringUtils.isBlank(tenant)) {
            throw new IllegalStateException("get no target tenant of userInfo=" + userInfo);
        }
        return tenant;
    }

    @Override
    public Object plugin(Object target) {
        if (target instanceof StatementHandler) {
            return Plugin.wrap(target, this);
        } else {
            return target;
        }
    }

    @Override
    public void setProperties(Properties properties) {

    }
}
