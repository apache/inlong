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

package org.apache.inlong.audit.service.utils;

import org.apache.inlong.audit.service.config.Configuration;
import org.apache.inlong.audit.service.entities.JdbcConfig;

import com.zaxxer.hikari.HikariConfig;

import java.util.Objects;

import static org.apache.inlong.audit.service.config.ConfigConstants.CACHE_PREP_STMTS;
import static org.apache.inlong.audit.service.config.ConfigConstants.DEFAULT_CACHE_PREP_STMTS;
import static org.apache.inlong.audit.service.config.ConfigConstants.DEFAULT_CONNECTION_TIMEOUT;
import static org.apache.inlong.audit.service.config.ConfigConstants.DEFAULT_DATASOURCE_POOL_SIZE;
import static org.apache.inlong.audit.service.config.ConfigConstants.DEFAULT_PREP_STMT_CACHE_SIZE;
import static org.apache.inlong.audit.service.config.ConfigConstants.DEFAULT_PREP_STMT_CACHE_SQL_LIMIT;
import static org.apache.inlong.audit.service.config.ConfigConstants.KEY_CACHE_PREP_STMTS;
import static org.apache.inlong.audit.service.config.ConfigConstants.KEY_DATASOURCE_CONNECTION_TIMEOUT;
import static org.apache.inlong.audit.service.config.ConfigConstants.KEY_DATASOURCE_POOL_SIZE;
import static org.apache.inlong.audit.service.config.ConfigConstants.KEY_DEFAULT_MYSQL_DRIVER;
import static org.apache.inlong.audit.service.config.ConfigConstants.KEY_MYSQL_DRIVER;
import static org.apache.inlong.audit.service.config.ConfigConstants.KEY_MYSQL_JDBC_URL;
import static org.apache.inlong.audit.service.config.ConfigConstants.KEY_MYSQL_PASSWORD;
import static org.apache.inlong.audit.service.config.ConfigConstants.KEY_MYSQL_USERNAME;
import static org.apache.inlong.audit.service.config.ConfigConstants.KEY_PREP_STMT_CACHE_SIZE;
import static org.apache.inlong.audit.service.config.ConfigConstants.KEY_PREP_STMT_CACHE_SQL_LIMIT;
import static org.apache.inlong.audit.service.config.ConfigConstants.PREP_STMT_CACHE_SIZE;
import static org.apache.inlong.audit.service.config.ConfigConstants.PREP_STMT_CACHE_SQL_LIMIT;

/**
 * Jdbc utils
 */
public class JdbcUtils {

    /**
     * Build mysql config
     * @return
     */
    public static JdbcConfig buildMysqlConfig() {
        return doBuild(Configuration.getInstance().get(KEY_MYSQL_DRIVER, KEY_DEFAULT_MYSQL_DRIVER),
                Configuration.getInstance().get(KEY_MYSQL_JDBC_URL),
                Configuration.getInstance().get(KEY_MYSQL_USERNAME),
                Configuration.getInstance().get(KEY_MYSQL_PASSWORD));
    }

    /**
     * Do build config
     * @param driverClass
     * @param jdbcUrl
     * @param userName
     * @param password
     * @return
     */
    private static JdbcConfig doBuild(String driverClass, String jdbcUrl, String userName, String password) {
        assert (Objects.nonNull(driverClass)
                && Objects.nonNull(jdbcUrl)
                && Objects.nonNull(userName)
                && Objects.nonNull(password));

        return new JdbcConfig(
                driverClass,
                jdbcUrl,
                userName,
                password);
    }

    public static HikariConfig buildHikariConfig(String driverClassName, String jdbcUrl, String userName,
            String passWord) {
        HikariConfig hikariConfig = new HikariConfig();
        hikariConfig.setDriverClassName(driverClassName);
        hikariConfig.setJdbcUrl(jdbcUrl);
        hikariConfig.setUsername(userName);
        hikariConfig.setPassword(passWord);
        Configuration configuration = Configuration.getInstance();
        hikariConfig.setConnectionTimeout(configuration.get(KEY_DATASOURCE_CONNECTION_TIMEOUT,
                DEFAULT_CONNECTION_TIMEOUT));
        hikariConfig.addDataSourceProperty(CACHE_PREP_STMTS,
                configuration.get(KEY_CACHE_PREP_STMTS, DEFAULT_CACHE_PREP_STMTS));
        hikariConfig.addDataSourceProperty(PREP_STMT_CACHE_SIZE,
                configuration.get(KEY_PREP_STMT_CACHE_SIZE, DEFAULT_PREP_STMT_CACHE_SIZE));
        hikariConfig.addDataSourceProperty(PREP_STMT_CACHE_SQL_LIMIT,
                configuration.get(KEY_PREP_STMT_CACHE_SQL_LIMIT, DEFAULT_PREP_STMT_CACHE_SQL_LIMIT));
        hikariConfig.setMaximumPoolSize(
                configuration.get(KEY_DATASOURCE_POOL_SIZE,
                        DEFAULT_DATASOURCE_POOL_SIZE));
        return hikariConfig;
    }
}
