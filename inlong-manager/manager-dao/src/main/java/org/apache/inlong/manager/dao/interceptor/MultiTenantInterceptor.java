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
import org.apache.inlong.manager.common.exceptions.BusinessException;
import org.apache.inlong.manager.common.tenant.MultiTenantQuery;
import org.apache.inlong.manager.common.util.JsonUtils;
import org.apache.inlong.manager.pojo.user.LoginUserUtils;
import org.apache.inlong.manager.pojo.user.UserInfo;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.ibatis.cache.CacheKey;
import org.apache.ibatis.executor.Executor;
import org.apache.ibatis.executor.parameter.ParameterHandler;
import org.apache.ibatis.mapping.BoundSql;
import org.apache.ibatis.mapping.MappedStatement;
import org.apache.ibatis.mapping.ParameterMapping;
import org.apache.ibatis.plugin.Interceptor;
import org.apache.ibatis.plugin.Intercepts;
import org.apache.ibatis.plugin.Invocation;
import org.apache.ibatis.plugin.Plugin;
import org.apache.ibatis.plugin.Signature;
import org.apache.ibatis.reflection.DefaultReflectorFactory;
import org.apache.ibatis.reflection.MetaObject;
import org.apache.ibatis.reflection.ReflectorFactory;
import org.apache.ibatis.reflection.factory.DefaultObjectFactory;
import org.apache.ibatis.reflection.factory.ObjectFactory;
import org.apache.ibatis.reflection.wrapper.DefaultObjectWrapperFactory;
import org.apache.ibatis.reflection.wrapper.ObjectWrapperFactory;
import org.apache.ibatis.session.ResultHandler;
import org.apache.ibatis.session.RowBounds;

import java.sql.PreparedStatement;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * This interceptor intercept those queries annotated by {@link MultiTenantQuery}.
 *
 * <p>The main idea of MultiTenantInterceptor is that developer define sql template
 * support multiple tenant in mapper.xml, but no need to pass the tenant explicitly in mapper.java.</p>
 *
 * <p>MultiTenantInterceptor will insert <strong>tenant</strong> into the parameter maps in
 * {@link Executor} and {@link ParameterHandler} stages.</p>
 */
@Slf4j
@Intercepts({
        @Signature(type = ParameterHandler.class, method = "setParameters", args = PreparedStatement.class),
        @Signature(type = Executor.class, method = "query", args = {MappedStatement.class, Object.class,
                RowBounds.class, ResultHandler.class, CacheKey.class, BoundSql.class}),
        @Signature(type = Executor.class, method = "query", args = {MappedStatement.class, Object.class,
                RowBounds.class, ResultHandler.class})
})
public class MultiTenantInterceptor implements Interceptor {

    private static final String KEY_TENANT = "tenant";
    private static final ObjectFactory DEFAULT_OBJECT_FACTORY = new DefaultObjectFactory();
    private static final ObjectWrapperFactory DEFAULT_OBJECT_WRAPPER_FACTORY = new DefaultObjectWrapperFactory();
    private static final ReflectorFactory REFLECTOR_FACTORY = new DefaultReflectorFactory();
    @Override
    public Object intercept(Invocation invocation) throws Throwable {
        if (invocation.getTarget() instanceof ParameterHandler) {
            return doParameterHandler((ParameterHandler) invocation.getTarget(), invocation);
        } else if (invocation.getTarget() instanceof Executor) {
            return doExecutor(invocation);
        } else {
            throw new Throwable("do not support type of target=" + invocation.getTarget());
        }
    }

    private Object doExecutor(Invocation invocation) throws Throwable {
        MappedStatement mappedStatement = (MappedStatement) invocation.getArgs()[0];
        String fullMethodName = mappedStatement.getId();
        if (!MultiTenantQueryFilter.isMultiTenantQuery(fullMethodName.split(InlongConstants.UNDERSCORE)[0])) {
            return invocation.proceed();
        }
        try {
            Object[] args = invocation.getArgs();
            MappedStatement ms = (MappedStatement) args[0];
            Object parameter = args[1];
            BoundSql boundSql;
            if (args.length == 4) {
                // 4 params
                boundSql = ms.getBoundSql(parameter);
            } else {
                // 6 params
                boundSql = (BoundSql) args[5];
            }

            List<ParameterMapping> parameterMappings = boundSql.getParameterMappings();
            // new param mapping
            Map<String, Object> newParameter = makeNewParameters(parameter, parameterMappings);
            // update params
            invocation.getArgs()[1] = newParameter;

            return invocation.proceed();
        } catch (Exception e) {
            log.error("failed to do executor in MultiTenantInterceptor", e);
            throw e;
        }
    }

    private Object doParameterHandler(ParameterHandler parameterHandler, Invocation invocation) throws Throwable {
        MetaObject metaResultSetHandler = MetaObject.forObject(parameterHandler, DEFAULT_OBJECT_FACTORY,
                DEFAULT_OBJECT_WRAPPER_FACTORY, REFLECTOR_FACTORY);
        MappedStatement mappedStatement = (MappedStatement) metaResultSetHandler.getValue("mappedStatement");
        String fullMethodName = mappedStatement.getId();
        if (!MultiTenantQueryFilter.isMultiTenantQuery(fullMethodName.split(InlongConstants.UNDERSCORE)[0])) {
            return invocation.proceed();
        }

        Object parameterObject = metaResultSetHandler.getValue("parameterObject");
        BoundSql boundSql = (BoundSql) metaResultSetHandler.getValue("boundSql");
        Map<String, Object> newParams = makeNewParameters(parameterObject, boundSql.getParameterMappings());

        metaResultSetHandler.setValue("parameterObject", newParams);
        return invocation.proceed();
    }

    private Map<String, Object> makeNewParameters(Object parameterObject, List<ParameterMapping> parameters) {
        Map<String, Object> params;

        // only the single param query has no property name, find it in parameters.
        if (isPrimitiveOrWrapper(parameterObject) && parameters.size() == 2) {
            params = new LinkedHashMap<>();

            // find the param not tenant
            int idx = 0;
            if (KEY_TENANT.equals(parameters.get(0).getProperty())) {
                idx = 1;
            }
            params.put(parameters.get(idx).getProperty(), parameterObject);
        } else {
            String jsonStr = JsonUtils.toJsonString(parameterObject);
            params = JsonUtils.parseObject(jsonStr, Map.class);
        }
        params.put(KEY_TENANT, getTenant());
        return params;
    }

    private boolean isPrimitiveOrWrapper(Object obj) {
        try {
            Class<?> clazz = obj.getClass();
            return (obj instanceof String)
                    || clazz.isPrimitive()
                    || ((Class<?>) clazz.getField("TYPE").get(null)).isPrimitive();
        } catch (Exception e) {
            return false;
        }
    }

    private String getTenant() {
        UserInfo userInfo = LoginUserUtils.getLoginUser();
        if (userInfo == null) {
            throw new BusinessException("Current user is null, please login first");
        }
        String tenant = userInfo.getTenant();
        if (StringUtils.isBlank(tenant)) {
            throw new BusinessException(String.format("User tenant is blank for user id=%s and username=%s",
                    userInfo.getId(), userInfo.getName()));
        }
        return tenant;
    }

    @Override
    public Object plugin(Object target) {
        return Plugin.wrap(target, this);
    }

    @Override
    public void setProperties(Properties properties) {

    }
}
