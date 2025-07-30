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

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

public class ClickHouseOptions {

    public static final ConfigOption<String> URL =
            ConfigOptions.key("clickhouse.url")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("ClickHouse JDBC URL");

    public static final ConfigOption<String> USERNAME =
            ConfigOptions.key("clickhouse.username")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("ClickHouse username");

    public static final ConfigOption<String> PASSWORD =
            ConfigOptions.key("clickhouse.password")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("ClickHouse password");

    public static final ConfigOption<String> QUERY =
            ConfigOptions.key("clickhouse.query")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Select query for source");

    public static final ConfigOption<String> SINK_SQL =
            ConfigOptions.key("clickhouse.sink.sql")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Insert SQL for sink");

    public static final ConfigOption<String> INSERT_SQL =
            ConfigOptions.key("clickhouse.insert-sql")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("ClickHouse insert SQL statement");
}
