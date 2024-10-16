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

package org.apache.inlong.audit.service.selector.api;

import lombok.Data;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.util.Objects;

/**
 * Selector config
 */
@Data
public class SelectorConfig {

    private static final Logger LOGGER = LoggerFactory.getLogger(SelectorConfig.class);
    public static final String MONITOR_COMMON_NAME = "audit";
    private final String serviceId;
    private final String leaderId;
    private String defaultLeaderId;
    private boolean useDefaultLeader = false;
    private String dbDriver;
    private String dbUrl;
    private String dbUser;
    private String dbPasswd;
    private String selectorDbName = "leader_selector";
    private int leaderTimeout = 20;
    private int tryToBeLeaderInterval = 5;
    private int dbMonitorRunInterval = 20;
    private int connectionTimeout = 10000;
    private int idleTimeout = 60000;
    private int maxLifetime = 1800000;
    private int maximumPoolSize = 2;
    private String cachePrepStmts = "true";
    private int prepStmtCacheSize = 250;
    private int prepStmtCacheSqlLimit = 2048;
    private String monitorName = "selector_leader_state";
    private String ip;
    private SelectorChangeListener selectorChangeListener;

    public SelectorConfig(String serviceId, String leaderId, String dbUrl, String dbUser, String dbPasswd,
            String dbDriver) {
        assert (Objects.nonNull(serviceId)
                && Objects.nonNull(leaderId)
                && Objects.nonNull(dbUrl)
                && Objects.nonNull(dbUser)
                && Objects.nonNull(dbPasswd)
                && Objects.nonNull(dbDriver));

        this.serviceId = serviceId;
        this.leaderId = leaderId;
        this.dbUrl = dbUrl;
        this.dbUser = dbUser;
        this.dbPasswd = dbPasswd;
        this.dbDriver = dbDriver;

    }

    public String getIp() {
        if (StringUtils.isEmpty(ip))
            try {
                ip = InetAddress.getLocalHost().getHostAddress();
            } catch (Exception e) {
                LOGGER.error("Get local ip has exception:{}", e.getMessage());
                ip = "N/A";
            }
        return ip;
    }

}
