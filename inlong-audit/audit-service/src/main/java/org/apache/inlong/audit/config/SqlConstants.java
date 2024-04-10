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
 * Sql constants
 */
public class SqlConstants {

    // HA selector sql
    public static final String SELECTOR_SQL =
            "insert ignore into {0} (service_id, leader_id, last_seen_active) values (''{1}'', ''{2}'', now()) on duplicate key update leader_id = if(last_seen_active < now() - interval # second, values(leader_id), leader_id),last_seen_active = if(leader_id = values(leader_id), values(last_seen_active), last_seen_active)";
    public static final String REPLACE_LEADER_SQL =
            "replace into {0} ( service_id, leader_id, last_seen_active ) values (''{1}'', ''#'', now())";
    public static final String RELEASE_SQL = "delete from {0} where service_id=''{1}'' and leader_id= ''{2}''";
    public static final String IS_LEADER_SQL =
            "select count(*) as is_leader from {0} where service_id=''{1}'' and leader_id=''{2}''";
    public static final String SEARCH_CURRENT_LEADER_SQL =
            "select leader_id as leader from {0} where service_id=''{1}''";
    public static final String SELECT_TEST_SQL = "SELECT 1 ";

    // ClickHouse query sql
    public static final String KEY_CLICKHOUSE_SOURCE_QUERY_SQL = "clickhouse.source.query.sql";
    public static final String DEFAULT_CLICKHOUSE_SOURCE_QUERY_SQL =
            "SELECT inlong_group_id, inlong_stream_id, audit_id, audit_tag\n" +
                    "    , sum(cnt) AS cnt, sum(size) AS size\n" +
                    "    , sum(delay) AS delay\n" +
                    "FROM (\n" +
                    "    SELECT max(audit_version), ip, docker_id, thread_id\n" +
                    "        , inlong_group_id, inlong_stream_id, audit_id, audit_tag, cnt\n" +
                    "        , size, delay\n" +
                    "    FROM (\n" +
                    "        SELECT audit_version, ip, docker_id, thread_id, inlong_group_id\n" +
                    "            , inlong_stream_id, audit_id, audit_tag, sum(count) AS cnt\n" +
                    "            , sum(size) AS size, sum(delay) AS delay\n" +
                    "        FROM (\n" +
                    "            SELECT audit_version, docker_id, thread_id, sdk_ts, packet_id\n" +
                    "                , log_ts, ip, inlong_group_id, inlong_stream_id, audit_id\n" +
                    "                , audit_tag, count, size, delay\n" +
                    "            FROM audit_data \n" +
                    "            WHERE log_ts BETWEEN ? AND ? \n" +
                    "                AND audit_id = ? \n" +
                    "            GROUP BY audit_version, docker_id, thread_id, sdk_ts, packet_id, log_ts, ip, inlong_group_id, inlong_stream_id, audit_id, audit_tag, count, size, delay\n"
                    +
                    "        ) t1\n" +
                    "        GROUP BY audit_version, ip, docker_id, thread_id, inlong_group_id, inlong_stream_id, audit_id, audit_tag\n"
                    +
                    "    ) t2\n" +
                    "    GROUP BY ip, docker_id, thread_id, inlong_group_id, inlong_stream_id, audit_id, audit_tag, cnt, size, delay\n"
                    +
                    ") t3\n" +
                    "GROUP BY inlong_group_id, inlong_stream_id, audit_id, audit_tag";

    // Mysql query sql
    public static final String KEY_MYSQL_SOURCE_QUERY_TEMP_SQL = "mysql.query.temp.sql";
    public static final String DEFAULT_MYSQL_SOURCE_QUERY_TEMP_SQL =
            "SELECT inlong_group_id, inlong_stream_id, audit_id, audit_tag\n" +
                    ", sum(count) AS cnt, sum(size) AS size\n" +
                    ", sum(delay) AS delay\n" +
                    "FROM audit_data_temp\n" +
                    "WHERE log_ts BETWEEN ?  AND ? \n" +
                    "AND audit_id = ? \n" +
                    "GROUP BY inlong_group_id, inlong_stream_id, audit_id, audit_tag";

    public static final String KEY_MYSQL_SOURCE_QUERY_DAY_SQL = "mysql.query.day.sql";
    public static final String DEFAULT_MYSQL_SOURCE_QUERY_DAY_SQL =
            "select log_ts,inlong_group_id,inlong_stream_id,audit_id,audit_tag,count,size,delay " +
                    "from audit_data_day where log_ts between ? and ? and inlong_group_id=? and inlong_stream_id=? and audit_id =? ";

    // Mysql insert sql
    public static final String KEY_MYSQL_SINK_INSERT_DAY_SQL = "mysql.sink.insert.day.sql";
    public static final String DEFAULT_MYSQL_SINK_INSERT_DAY_SQL =
            "replace into audit_data_day (log_ts,inlong_group_id, inlong_stream_id, audit_id,audit_tag,count, size, delay) "
                    + " values (?,?,?,?,?,?,?,?)";
    public static final String KEY_MYSQL_SINK_INSERT_TEMP_SQL = "mysql.sink.insert.temp.sql";
    public static final String DEFAULT_MYSQL_SINK_INSERT_TEMP_SQL =
            "replace into audit_data_temp (log_ts,inlong_group_id, inlong_stream_id, audit_id,audit_tag,count, size, delay) "
                    + " values (?,?,?,?,?,?,?,?)";

}
