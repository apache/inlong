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

package org.apache.inlong.audit.service.node;

import org.apache.inlong.audit.service.config.Configuration;
import org.apache.inlong.audit.service.entities.JdbcConfig;
import org.apache.inlong.audit.service.metric.MetricsManager;
import org.apache.inlong.audit.service.selector.api.Selector;
import org.apache.inlong.audit.service.selector.api.SelectorConfig;
import org.apache.inlong.audit.service.selector.api.SelectorFactory;
import org.apache.inlong.audit.service.utils.JdbcUtils;
import org.apache.inlong.common.util.NetworkUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;

import static org.apache.inlong.audit.service.config.ConfigConstants.DEFAULT_SELECTOR_FOLLOWER_LISTEN_CYCLE_MS;
import static org.apache.inlong.audit.service.config.ConfigConstants.DEFAULT_SELECTOR_SERVICE_ID;
import static org.apache.inlong.audit.service.config.ConfigConstants.KEY_SELECTOR_FOLLOWER_LISTEN_CYCLE_MS;
import static org.apache.inlong.audit.service.config.ConfigConstants.KEY_SELECTOR_SERVICE_ID;

public class Application {

    private static final Logger LOGGER = LoggerFactory.getLogger(Application.class);
    private static final EtlService etlService = new EtlService();
    private static final ApiService apiService = new ApiService();
    private static Selector selector;
    private static boolean running = true;

    public static void main(String[] args) {
        try {
            // Periodically obtain audit id and audit course configuration from DB
            ConfigService.getInstance().start();

            PartitionManager.getInstance().start();

            MetricsManager.getInstance().init();

            // Etl service aggregate the data from the data source and store the aggregated data to the target storage
            etlService.start();

            // Api service provide audit data interface to external services
            apiService.start();

            // Cleanup resource when program exit.
            stopIfKilled();

            // Waiting to become the leader node.
            waitToBeLeader();
        } catch (Exception ex) {
            LOGGER.error("Running exception: ", ex);
        }
    }

    private static void stopIfKilled() {
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                running = false;
                etlService.stop();
                apiService.stop();
                selector.close();
                MetricsManager.getInstance().shutdown();
                LOGGER.info("Stopping gracefully");
            } catch (Exception ex) {
                LOGGER.error("Stop error: ", ex);
            }
        }));
    }

    /**
     * Init selector
     */
    private static void initSelector() {
        JdbcConfig jdbcConfig = JdbcUtils.buildMysqlConfig();
        String leaderId = NetworkUtils.getLocalIp() + "-" + UUID.randomUUID();
        LOGGER.info("Init selector. Leader id is :{}", leaderId);
        if (selector == null) {
            SelectorConfig electorConfig = new SelectorConfig(
                    Configuration.getInstance().get(KEY_SELECTOR_SERVICE_ID, DEFAULT_SELECTOR_SERVICE_ID),
                    leaderId,
                    jdbcConfig.getJdbcUrl(),
                    jdbcConfig.getUserName(), jdbcConfig.getPassword(), jdbcConfig.getDriverClass());

            selector = SelectorFactory.getNewElector(electorConfig);
            try {
                selector.init();
            } catch (Exception e) {
                LOGGER.error("Init selector has exception:", e);
            }
        }
    }

    /**
     * Wait to be leader
     */
    private static void waitToBeLeader() {
        initSelector();
        while (running) {
            try {
                Thread.sleep(Configuration.getInstance().get(KEY_SELECTOR_FOLLOWER_LISTEN_CYCLE_MS,
                        DEFAULT_SELECTOR_FOLLOWER_LISTEN_CYCLE_MS));
            } catch (Exception e) {
                LOGGER.error("Wait to be Leader has exception! lost Leadership!", e);
            }

            if (selector.isLeader()) {
                LOGGER.info("I get Leadership! Begin to aggregate clickhouse data to mysql");
                etlService.auditSourceToMysql();
                return;
            }
        }
    }
}
