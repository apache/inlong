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

package org.apache.inlong.sort.cdc.dm;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.inlong.sort.cdc.dm.source.DMRichSourceFunction;
import org.apache.inlong.sort.cdc.dm.table.DMDeserializationSchema;
import org.apache.inlong.sort.cdc.dm.table.StartupMode;

import java.time.Duration;
import java.util.Properties;

import static org.apache.flink.util.Preconditions.checkNotNull;

/** A builder to build a SourceFunction which can read snapshot and change events of DM. */
@PublicEvolving
public class DMSource {

    public static <T> Builder<T> builder() {
        return new Builder<>();
    }

    /** Builder class of {@link DMSource}. */
    public static class Builder<T> {

        // common config
        private StartupMode startupMode;
        private String username;
        private String password;
        private String databaseName;
        private String schemaName;
        private String tableName;
        private String tableList;
        private String serverTimeZone;
        private Duration connectTimeout;

        // snapshot reading config
        private String hostname;
        private Integer port;
        private Properties jdbcProperties;
        private DMDeserializationSchema<T> deserializer;
        private Long startupTimestamp;

        // metrics related
        private String inlongMetric;
        private String auditHostAndPorts;

        // does not need incremental reading config nor logmnr_dll
        // use DMBS_LOGMNR to directly access scn.

        public Builder<T> startupMode(StartupMode startupMode) {
            this.startupMode = startupMode;
            return this;
        }

        public Builder<T> username(String username) {
            this.username = username;
            return this;
        }

        public Builder<T> password(String password) {
            this.password = password;
            return this;
        }

        public Builder<T> databaseName(String databaseName) {
            this.databaseName = databaseName;
            return this;
        }

        public Builder<T> schemaName(String schemaName) {
            this.schemaName = schemaName;
            return this;
        }

        public Builder<T> tableName(String tableName) {
            this.tableName = tableName;
            return this;
        }

        public Builder<T> tableList(String tableList) {
            this.tableList = tableList;
            return this;
        }

        public Builder<T> serverTimeZone(String serverTimeZone) {
            this.serverTimeZone = serverTimeZone;
            return this;
        }

        public Builder<T> connectTimeout(Duration connectTimeout) {
            this.connectTimeout = connectTimeout;
            return this;
        }

        public Builder<T> hostname(String hostname) {
            this.hostname = hostname;
            return this;
        }

        public Builder<T> port(int port) {
            this.port = port;
            return this;
        }

        public Builder<T> jdbcProperties(Properties jdbcProperties) {
            this.jdbcProperties = jdbcProperties;
            return this;
        }

        public Builder<T> startupTimestamp(Long startupTimestamp) {
            this.startupTimestamp = startupTimestamp;
            return this;
        }

        public Builder<T> deserializer(DMDeserializationSchema<T> deserializer) {
            this.deserializer = deserializer;
            return this;
        }

        public Builder<T> inlongMetric(String inlongMetric) {
            this.inlongMetric = inlongMetric;
            return this;
        }

        public Builder<T> auditHostAndPorts(String auditHostAndPorts) {
            this.auditHostAndPorts = auditHostAndPorts;
            return this;
        }

        public SourceFunction<T> build() {
            switch (startupMode) {
                case INITIAL:
                    checkNotNull(hostname, "hostname shouldn't be null on startup mode 'initial'");
                    checkNotNull(port, "port shouldn't be null on startup mode 'initial'");
                    startupTimestamp = 0L;
                    break;
                case LATEST_OFFSET:
                    startupTimestamp = 0L;
                    break;
                case TIMESTAMP:
                    checkNotNull(
                            startupTimestamp,
                            "startupTimestamp shouldn't be null on startup mode 'timestamp'");
                    break;
                default:
                    throw new UnsupportedOperationException(
                            startupMode + " mode is not supported.");
            }

            if (StringUtils.isNotEmpty(databaseName) || StringUtils.isNotEmpty(tableName)) {
                if (StringUtils.isEmpty(databaseName) || StringUtils.isEmpty(tableName)) {
                    throw new IllegalArgumentException(
                            "'database-name' and 'table-name' should be configured at the same time");
                }
            } else {
                checkNotNull(
                        tableList,
                        "'database-name', 'table-name' or 'table-list' should be configured");
            }

            if (serverTimeZone == null) {
                serverTimeZone = "+00:00";
            }

            if (connectTimeout == null) {
                connectTimeout = Duration.ofSeconds(30);
            }

            return new DMRichSourceFunction<>(
                    StartupMode.INITIAL.equals(startupMode),
                    username,
                    password,
                    databaseName,
                    schemaName,
                    tableName,
                    tableList,
                    connectTimeout,
                    hostname,
                    port,
                    jdbcProperties,
                    deserializer,
                    inlongMetric,
                    auditHostAndPorts);
        }
    }
}
