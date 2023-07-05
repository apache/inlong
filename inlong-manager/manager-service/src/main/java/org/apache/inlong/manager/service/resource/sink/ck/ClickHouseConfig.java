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

import org.apache.inlong.manager.dao.entity.AuditQuerySourceConfigEntity;
import org.apache.inlong.manager.dao.mapper.AuditQuerySourceConfigEntityMapper;

import com.alibaba.druid.pool.DruidDataSourceFactory;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.stereotype.Service;

import javax.sql.DataSource;

import java.sql.Connection;
import java.util.Properties;

/**
 * Clickhouse config information, including url, user, etc.
 */
@Component
@Service
@Slf4j
public class ClickHouseConfig {

    @Autowired
    private AuditQuerySourceConfigEntityMapper querySourceConfigEntityMapper;
    private static volatile DataSource source;
    private static volatile String currentJdbcUrl = null;
    private static volatile String currentUserName = null;
    private static volatile String currentPassword = null;

    public void updateCkSource() {
        try {
            if (querySourceConfigEntityMapper == null) {
                log.warn("querySourceConfigEntityMapper is null");
            }
            AuditQuerySourceConfigEntity querySourceConfigEntity = querySourceConfigEntityMapper.findByInUse();
            String jdbcUrl = querySourceConfigEntity.getHosts();
            String username = querySourceConfigEntity.getUserName();
            String password = querySourceConfigEntity.getPassword();
            String pwd = (password == null) ? "" : password;
            log.info("current jdbc is: {}", currentJdbcUrl);
            log.info("jdbc in db is: {}", jdbcUrl);
            if (currentJdbcUrl == null || currentUserName == null || currentPassword == null
                    || !(currentJdbcUrl.equals(jdbcUrl) && currentUserName.equals(username)
                            && currentPassword.equals(pwd))) {
                synchronized (ClickHouseConfig.class) {
                    currentJdbcUrl = jdbcUrl;
                    currentUserName = username;
                    currentPassword = pwd;

                    Properties pros = new Properties();
                    pros.put("url", jdbcUrl);
                    pros.put("username", username);
                    pros.put("password", pwd);
                    try {
                        source = DruidDataSourceFactory.createDataSource(pros);
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                    log.info("Connected to {}", jdbcUrl);
                }
            }
        } catch (Exception e) {
            log.error("Error occurred while reading CK source: {}", e.getCause());
        }
    }

    /**
     * Get ClickHouse connection from data source
     */
    public Connection getCkConnection() throws Exception {
        log.info("Start to get connection to CLICKHOUSE...");
        while (source == null) {
            updateCkSource();
        }
        return source.getConnection();
    }
}
