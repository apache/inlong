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

package config;

/**
 * Config constants
 */
public class ConfigConstants {

    // Source config
    public static final String KEY_CLICKHOUSE_DRIVER = "clickhouse.driver";
    public static final String DEFAULT_CLICKHOUSE_DRIVER = "ru.yandex.clickhouse.ClickHouseDriver";
    public static final String KEY_CLICKHOUSE_URL = "clickhouse.url";
    public static final String KEY_CLICKHOUSE_USERNAME = "clickhouse.username";
    public static final String KEY_CLICKHOUSE_PASSWORD = "clickhouse.password";

    // DB config
    public static final String KEY_MYSQL_DRIVER = "mysql.driver";
    public static final String KEY_DEFAULT_MYSQL_DRIVER = "com.mysql.cj.jdbc.Driver";
    public static final String KEY_MYSQL_URL = "mysql.url";
    public static final String KEY_MYSQL_USERNAME = "mysql.username";
    public static final String KEY_MYSQL_PASSWORD = "mysql.password";

    // Time config
    public static final String KEY_QUERY_SQL_TIME_OUT = "query.sql.timeout";
    public static final int DEFAULT_QUERY_SQL_TIME_OUT = 300;
    public static final String KEY_JDBC_TIME_OUT = "jdbc.timeout.second";
    public static final int DEFAULT_JDBC_TIME_OUT = 300;
    public static final String KEY_DATASOURCE_CONNECTION_TIMEOUT = "datasource.connection.timeout.ms";
    public static final int DEFAULT_CONNECTION_TIMEOUT = 1000 * 60 * 5;
    public static final String KEY_API_RESPONSE_TIMEOUT = "api.response.timeout";
    public static final int DEFAULT_API_TIMEOUT = 30;
    public static final String KEY_QUEUE_PULL_TIMEOUT = "queue.pull.timeout.ms";
    public static final int DEFAULT_QUEUE_PULL_TIMEOUT = 1000;

    // Interval config
    public static final String KEY_SOURCE_CLICKHOUSE_STAT_INTERVAL = "source.clickhouse.stat.interval.minute";
    public static final int DEFAULT_SOURCE_CLICKHOUSE_STAT_INTERVAL = 1;
    public static final String KEY_SOURCE_DB_STAT_INTERVAL = "source.db.stat.interval.minute";
    public static final int DEFAULT_SOURCE_DB_STAT_INTERVAL = 1;
    public static final String KEY_SOURCE_DB_SINK_INTERVAL = "sink.db.interval.ms";
    public static final int DEFAULT_SOURCE_DB_SINK_INTERVAL = 100;
    public static final String KEY_SOURCE_DB_SINK_BATCH = "sink.db.batch";
    public static final int DEFAULT_SOURCE_DB_SINK_BATCH = 1000;
    public static final String KEY_SOURCE_DB_SINK_INTERNAL = "sink.db.internal.ms";
    public static final int DEFAULT_SOURCE_DB_SINK_INTERNAL = 100;

    // Api config
    public static final String KEY_HOUR_API_PATH = "hour.api.path";
    public static final String DEFAULT_HOUR_API_PATH = "/audit/query/hour";
    public static final String KEY_DAY_API_PATH = "day.api.path";
    public static final String DEFAULT_DAY_API_PATH = "/audit/query/day";
    public static final String KEY_DAY_API_TABLE = "day.api.table";
    public static final String DEFAULT_DAY_API_TABLE = "audit_data_day";
    public static final String KEY_MINUTE_API_TABLE = "minute.api.table";
    public static final String DEFAULT_MINUTE_API_TABLE = "audit_data_temp";
    public static final String KEY_MINUTE_10_API_PATH = "minute.10.api.path";
    public static final String DEFAULT_MINUTE_10_API_PATH = "/audit/query/minute/10";
    public static final String KEY_MINUTE_30_API_PATH = "minute.30.api.path";
    public static final String DEFAULT_MINUTE_30_API_PATH = "/audit/query/minute/30";
    public static final String KEY_API_POOL_SIZE = "api.pool.size";
    public static final int DEFAULT_POOL_SIZE = 10;
    public static final String KEY_API_BACKLOG_SIZE = "api.backlog.size";
    public static final int DEFAULT_API_BACKLOG_SIZE = 100;

    public static final String KEY_DATASOURCE_POOL_SIZE = "datasource.pool.size";
    public static final int DEFAULT_DATASOURCE_POOL_SIZE = 1000;

    public static final String KEY_DATA_QUEUE_SIZE = "data.queue.size";
    public static final int DEFAULT_DATA_QUEUE_SIZE = 1000000;
    public static final String KEY_AUDIT_IDS = "audit.ids";
    public static final String DEFAULT_AUDIT_IDS = "3;4;5;6";

    // Summary config
    public static final String KEY_REALTIME_SUMMARY_SOURCE_TABLE = "realtime.summary.source.table";
    public static final String DEFAULT_REALTIME_SUMMARY_SOURCE_TABLE = "audit_data";
    public static final String KEY_REALTIME_SUMMARY_SINK_TABLE = "realtime.summary.sink.table";
    public static final String DEFAULT_REALTIME_SUMMARY_SINK_TABLE = "audit_data_temp";
    public static final String KEY_REALTIME_SUMMARY_BEFORE_TIMES = "realtime.summary.before.times";
    public static final int DEFAULT_REALTIME_SUMMARY_BEFORE_TIMES = 6;

    public static final String KEY_DAILY_SUMMARY_SOURCE_TABLE = "daily.summary.source.table";
    public static final String DEFAULT_DAILY_SUMMARY_SOURCE_TABLE = "audit_data_temp";
    public static final String KEY_DAILY_SUMMARY_SINK_TABLE = "daily.summary.sink.table";
    public static final String DEFAULT_DAILY_SUMMARY_SINK_TABLE = "audit_data_day";
    public static final String KEY_DAILY_SUMMARY_BEFORE_TIMES = "daily.summary.before.times";
    public static final int DEFAULT_DAILY_SUMMARY_BEFORE_TIMES = 2;

}
