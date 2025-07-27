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

package org.apache.inlong.sort.clickhouse.table;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

public class ClickHouseConnectorOptions {

    public static final ConfigOption<String> HOSTS =
            ConfigOptions.key("clickhouse.hosts")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("ClickHouse JDBC hosts, e.g. host1:8123,host2:8123");

    public static final ConfigOption<String> DATABASE =
            ConfigOptions.key("clickhouse.database")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("ClickHouse database name");

    public static final ConfigOption<String> TABLE_NAME =
            ConfigOptions.key("clickhouse.table-name")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("ClickHouse table name");

    public static final ConfigOption<String> USERNAME =
            ConfigOptions.key("clickhouse.username")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Username to connect to ClickHouse");

    public static final ConfigOption<String> PASSWORD =
            ConfigOptions.key("clickhouse.password")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Password to connect to ClickHouse");
}
