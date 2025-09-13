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

package org.apache.inlong.audit.tool.util;

import org.apache.inlong.audit.tool.mapper.AuditMapper;

import org.apache.ibatis.datasource.pooled.PooledDataSource;
import org.apache.ibatis.session.SqlSession;
import org.apache.ibatis.session.SqlSessionFactory;
import org.apache.ibatis.session.SqlSessionFactoryBuilder;

import javax.sql.DataSource;

import java.util.Properties;

public class AuditSQLUtil {

    private static SqlSessionFactory sqlSessionFactory;
    private static Properties appProperties;

    public static void initialize(Properties properties) {
        appProperties = properties;
        try {
            // Create a data source
            DataSource dataSource = createDataSourceFromProperties();

            // Create a SqlSessionFactory
            org.apache.ibatis.session.Configuration configuration = new org.apache.ibatis.session.Configuration();
            configuration.setEnvironment(new org.apache.ibatis.mapping.Environment(
                    "development",
                    new org.apache.ibatis.transaction.jdbc.JdbcTransactionFactory(),
                    dataSource));

            configuration.addMapper(AuditMapper.class);

            sqlSessionFactory = new SqlSessionFactoryBuilder().build(configuration);

        } catch (Exception e) {
            throw new RuntimeException("Error initializing MyBatis with application properties", e);
        }
    }

    private static DataSource createDataSourceFromProperties() {
        String url = appProperties.getProperty("audit.data.source.url");
        String username = appProperties.getProperty("audit.data.source.username");
        String password = appProperties.getProperty("audit.data.source.password");

        PooledDataSource dataSource = new PooledDataSource();
        dataSource.setDriver("com.mysql.cj.jdbc.Driver");
        dataSource.setUrl(url);
        dataSource.setUsername(username);
        dataSource.setPassword(password);

        dataSource.setPoolMaximumActiveConnections(10);
        dataSource.setPoolMaximumIdleConnections(5);

        return dataSource;
    }

    public static SqlSession getSqlSession() {
        if (sqlSessionFactory == null) {
            throw new IllegalStateException("MyBatisUtil not initialized. Call initialize() first.");
        }
        return sqlSessionFactory.openSession();
    }
}