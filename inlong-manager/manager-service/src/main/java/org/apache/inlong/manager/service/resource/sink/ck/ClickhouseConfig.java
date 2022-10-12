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

package org.apache.inlong.manager.service.resource.sink.ck;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.sql.Connection;
import java.sql.Statement;

/**
 * Clickhouse config information, including host, port, etc.
 */
@Component
public class ClickhouseConfig {

    @Value("${ck.index.search.url}")
    private String url;

    @Value("${ck.auth.user}")
    private String user;

    @Value("${ck.auth.password}")
    private String password;

    private Statement ckStatement;

    /**
     * Get ClickHouse statement from config file
     */
    public Statement getCkStatement() throws Exception {
        if (ckStatement == null) {
            Connection connection = ClickHouseJdbcUtils.getConnection(url, user, password);
            ckStatement = connection.createStatement();
        }
        return ckStatement;
    }
}
