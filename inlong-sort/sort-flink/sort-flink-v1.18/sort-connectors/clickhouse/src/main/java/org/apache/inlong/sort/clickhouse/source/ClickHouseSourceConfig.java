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

package org.apache.inlong.sort.clickhouse.source;

import org.apache.inlong.sort.cdc.base.config.JdbcSourceConfig;
import com.ververica.cdc.connectors.base.options.StartupOptions;
import io.debezium.relational.RelationalDatabaseConnectorConfig;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

/**
 * ClickHouse source configuration for InLong Sort.
 */
public class ClickHouseSourceConfig extends JdbcSourceConfig {

    private final String queryTemplate;

    public ClickHouseSourceConfig(
            StartupOptions startupOptions,
            List<String> databaseList,
            List<String> tableList,
            int splitSize,
            int splitMetaGroupSize,
            double distributionFactorUpper,
            double distributionFactorLower,
            boolean includeSchemaChanges,
            Properties dbzProperties,
            String driverClassName,
            String hostname,
            int port,
            String username,
            String password,
            int fetchSize,
            String serverTimeZone,
            Duration connectTimeout,
            int connectMaxRetries,
            int connectionPoolSize,
            String chunkKeyColumn,
            String inlongMetric,
            String inlongAudit,
            String queryTemplate
    ) {
        super(startupOptions, databaseList, tableList,
                splitSize, splitMetaGroupSize,
                distributionFactorUpper, distributionFactorLower,
                includeSchemaChanges,
                dbzProperties, null,
                driverClassName, hostname, port,
                username, password,
                fetchSize, serverTimeZone,
                connectTimeout, connectMaxRetries, connectionPoolSize,
                chunkKeyColumn, inlongMetric, inlongAudit);
        this.queryTemplate = queryTemplate;
    }

    @Override
    public RelationalDatabaseConnectorConfig getDbzConnectorConfig() {
        return null;
    }

    @Override
    public List<String> getMetricLabelList() {
        return Collections.emptyList();
    }

    public String getJdbcUrl() {
        return String.format("jdbc:clickhouse://%s:%d/%s",
                getHostname(), getPort(),
                databaseList.isEmpty() ? "" : databaseList.get(0));
    }

    public String buildQuery(Object lastOffset) {
        return queryTemplate.replace("${last_offset}", String.valueOf(lastOffset));
    }
}
