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

-- ----------------------------
-- Database for InLong Audit
-- ----------------------------
CREATE DATABASE IF NOT EXISTS apache_inlong_audit;

USE apache_inlong_audit;

-- ----------------------------
-- Table structure for audit_data
-- The table creation statement of the audit flow table is used to record the real-time flow data of the audit.
-- ----------------------------
CREATE TABLE IF NOT EXISTS `audit_data`
(
    `log_ts` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT "Log timestamp",
    `audit_id` varchar(100) NOT NULL DEFAULT "" COMMENT "Audit id",
    `audit_tag` varchar(100) NOT NULL DEFAULT "" COMMENT "Audit tag",
    `inlong_group_id` varchar(100) NOT NULL DEFAULT "" COMMENT "The target inlong group id",
    `inlong_stream_id` varchar(100) NOT NULL DEFAULT "" COMMENT "The target inlong stream id",
    `audit_version` bigint(20) NOT NULL DEFAULT "-1" COMMENT "Audit version",
    `ip` varchar(32) NOT NULL DEFAULT "" COMMENT "Client IP",
    `docker_id` varchar(100) NOT NULL DEFAULT "" COMMENT "Client docker id",
    `thread_id` varchar(50) NOT NULL DEFAULT "" COMMENT "Client thread id",
    `sdk_ts` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT "SDK timestamp",
    `packet_id` bigint(20) NOT NULL DEFAULT "0" COMMENT "Packet id",
    `count` bigint(20) NOT NULL DEFAULT "0" COMMENT "Message count",
    `size` bigint(20) NOT NULL DEFAULT "0" COMMENT "Message size",
    `delay` bigint(20) NOT NULL DEFAULT "0" COMMENT "Message delay count",
    `update_time` datetime NULL DEFAULT CURRENT_TIMESTAMP COMMENT "Update time"
) ENGINE=OLAP
PRIMARY KEY(`log_ts`,`audit_id`,`audit_tag`,`inlong_group_id`,`inlong_stream_id`,`audit_version`,`ip`,`docker_id`,`thread_id`,`sdk_ts`,`packet_id`)
COMMENT "Aduit data"
DISTRIBUTED BY HASH(`log_ts`,`audit_id`,`audit_tag`) BUCKETS 32;
