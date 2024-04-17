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

package org.apache.inlong.audit.utils;

import org.apache.inlong.audit.config.Configuration;
import org.apache.inlong.audit.entities.JdbcConfig;

import java.util.Objects;

import static org.apache.inlong.audit.config.ConfigConstants.KEY_DEFAULT_MYSQL_DRIVER;
import static org.apache.inlong.audit.config.ConfigConstants.KEY_MYSQL_DRIVER;
import static org.apache.inlong.audit.config.ConfigConstants.KEY_MYSQL_JDBC_URL;
import static org.apache.inlong.audit.config.ConfigConstants.KEY_MYSQL_PASSWORD;
import static org.apache.inlong.audit.config.ConfigConstants.KEY_MYSQL_USERNAME;

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
}
