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

package org.apache.inlong.sort.cdc.postgres.source.config;

import com.ververica.cdc.connectors.base.options.StartupOptions;
import io.debezium.connector.AbstractSourceInfo;
import org.apache.inlong.sort.cdc.base.config.JdbcSourceConfig;
import io.debezium.config.Configuration;
import io.debezium.connector.postgresql.PostgresConnectorConfig;
import io.debezium.relational.RelationalTableFilters;

import javax.annotation.Nullable;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

/** The configuration for Postgres CDC source. */
public class PostgresSourceConfig extends JdbcSourceConfig {

    private static final long serialVersionUID = 1L;

    public PostgresSourceConfig(
            StartupOptions startupOptions,
            List<String> databaseList,
            List<String> schemaList,
            List<String> tableList,
            int splitSize,
            int splitMetaGroupSize,
            double distributionFactorUpper,
            double distributionFactorLower,
            boolean includeSchemaChanges,
            Properties dbzProperties,
            Configuration dbzConfiguration,
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
            @Nullable String chunkKeyColumn,
            String inlongMetric,
            String inlongAudit) {
        super(
                startupOptions,
                databaseList,
                schemaList,
                tableList,
                splitSize,
                splitMetaGroupSize,
                distributionFactorUpper,
                distributionFactorLower,
                includeSchemaChanges,
                dbzProperties,
                dbzConfiguration,
                driverClassName,
                hostname,
                port,
                username,
                password,
                fetchSize,
                serverTimeZone,
                connectTimeout,
                connectMaxRetries,
                connectionPoolSize,
                chunkKeyColumn,
                inlongMetric,
                inlongAudit);
    }

    @Override
    public PostgresConnectorConfig getDbzConnectorConfig() {
        return new PostgresConnectorConfig(getDbzConfiguration());
    }

    public RelationalTableFilters getTableFilters() {
        return getDbzConnectorConfig().getTableFilters();
    }

    @Override
    public List<String> getMetricLabelList() {
        return Arrays.asList(AbstractSourceInfo.DATABASE_NAME_KEY,
                AbstractSourceInfo.SCHEMA_NAME_KEY, AbstractSourceInfo.TABLE_NAME_KEY);
    }
}
