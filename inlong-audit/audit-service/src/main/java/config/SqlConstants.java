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

}
