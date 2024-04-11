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

package org.apache.inlong.audit.config;

/**
 * Config constants
 */
public class ConfigConstants {

    // Source config
    public static final String KEY_CLICKHOUSE_DRIVER = "clickhouse.driver";
    public static final String DEFAULT_CLICKHOUSE_DRIVER = "ru.yandex.clickhouse.ClickHouseDriver";
    public static final String KEY_CLICKHOUSE_JDBC_URL = "clickhouse.jdbc.url";
    public static final String KEY_CLICKHOUSE_USERNAME = "clickhouse.username";
    public static final String KEY_CLICKHOUSE_PASSWORD = "clickhouse.password";

    // DB config
    public static final String KEY_MYSQL_DRIVER = "mysql.driver";
    public static final String KEY_DEFAULT_MYSQL_DRIVER = "com.mysql.cj.jdbc.Driver";
    public static final String KEY_MYSQL_JDBC_URL = "mysql.jdbc.url";
    public static final String KEY_MYSQL_USERNAME = "mysql.username";
    public static final String KEY_MYSQL_PASSWORD = "mysql.password";

    // Time config
    public static final String KEY_DATASOURCE_CONNECTION_TIMEOUT = "datasource.connection.timeout.ms";
    public static final int DEFAULT_CONNECTION_TIMEOUT = 1000 * 60 * 5;
    public static final String KEY_QUEUE_PULL_TIMEOUT = "queue.pull.timeout.ms";
    public static final int DEFAULT_QUEUE_PULL_TIMEOUT = 1000;

    // Interval config
    public static final String KEY_SOURCE_DB_STAT_INTERVAL = "source.db.stat.interval.minute";
    public static final int DEFAULT_SOURCE_DB_STAT_INTERVAL = 1;
    public static final String KEY_SOURCE_DB_SINK_INTERVAL = "sink.db.interval.ms";
    public static final int DEFAULT_SOURCE_DB_SINK_INTERVAL = 100;
    public static final String KEY_SOURCE_DB_SINK_BATCH = "sink.db.batch";
    public static final int DEFAULT_SOURCE_DB_SINK_BATCH = 1000;

    public static final String KEY_DATASOURCE_POOL_SIZE = "datasource.pool.size";
    public static final int DEFAULT_DATASOURCE_POOL_SIZE = 1000;

    public static final String KEY_DATA_QUEUE_SIZE = "data.queue.size";
    public static final int DEFAULT_DATA_QUEUE_SIZE = 1000000;
    public static final String KEY_AUDIT_IDS = "audit.ids";
    public static final String DEFAULT_AUDIT_IDS = "3;4;5;6";

    // Summary config
    public static final String KEY_REALTIME_SUMMARY_STAT_BACK_TIMES = "realtime.summary.stat.back.times";
    public static final int DEFAULT_REALTIME_SUMMARY_STAT_BACK_TIMES = 6;

    public static final String KEY_DAILY_SUMMARY_STAT_BACK_TIMES = "daily.summary.stat.back.times";
    public static final int DEFAULT_DAILY_SUMMARY_STAT_BACK_TIMES = 2;

    public static final String KEY_STAT_BACK_INITIAL_OFFSET = "stat.back.initial.offset";
    public static final int DEFAULT_STAT_BACK_INITIAL_OFFSET = 0;

    // HA selector config
    public static final String KEY_RELEASE_LEADER_INTERVAL = "release.leader.interval";
    public static final int DEFAULT_RELEASE_LEADER_INTERVAL = 40;
    public static final String KEY_SELECTOR_THREAD_POOL_SIZE = "selector.thread.pool.size";
    public static final int DEFAULT_SELECTOR_THREAD_POOL_SIZE = 3;

    public static final String KEY_SELECTOR_SERVICE_ID = "selector.service.id";
    public static final String DEFAULT_SELECTOR_SERVICE_ID = "audit-service";
    public static final String KEY_SELECTOR_FOLLOWER_LISTEN_CYCLE_MS = "selector.follower.listen.cycle.ms";
    public static final int DEFAULT_SELECTOR_FOLLOWER_LISTEN_CYCLE_MS = 2000;

    // HikariConfig
    public static final String CACHE_PREP_STMTS = "cachePrepStmts";
    public static final String PREP_STMT_CACHE_SIZE = "prepStmtCacheSize";
    public static final String PREP_STMT_CACHE_SQL_LIMIT = "prepStmtCacheSqlLimit";

    public static final String KEY_CACHE_PREP_STMTS = "cache.prep.stmts";
    public static final boolean DEFAULT_CACHE_PREP_STMTS = true;

    public static final String KEY_PREP_STMT_CACHE_SIZE = "prep.stmt.cache.size";
    public static final int DEFAULT_PREP_STMT_CACHE_SIZE = 250;

    public static final String KEY_PREP_STMT_CACHE_SQL_LIMIT = "prep.stmt.cache.sql.limit";
    public static final int DEFAULT_PREP_STMT_CACHE_SQL_LIMIT = 2048;

    public static final int MAX_INIT_COUNT = 2;
    public static final int RANDOM_BOUND = 10;

}
