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

SET NAMES utf8;
SET FOREIGN_KEY_CHECKS = 0;
SET SQL_MODE = "NO_AUTO_VALUE_ON_ZERO";
SET time_zone = "+00:00";

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
    `id`               int(32)      NOT NULL PRIMARY KEY AUTO_INCREMENT COMMENT 'Incremental primary key',
    `ip`               varchar(32)  NOT NULL DEFAULT '' COMMENT 'Client IP',
    `docker_id`        varchar(100) NOT NULL DEFAULT '' COMMENT 'Client docker id',
    `thread_id`        varchar(50)  NOT NULL DEFAULT '' COMMENT 'Client thread id',
    `sdk_ts`           TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT 'SDK timestamp',
    `packet_id`        BIGINT       NOT NULL DEFAULT '0' COMMENT 'Packet id',
    `log_ts`           TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT 'Log timestamp',
    `inlong_group_id`  varchar(100) NOT NULL DEFAULT '' COMMENT 'The target inlong group id',
    `inlong_stream_id` varchar(100) NOT NULL DEFAULT '' COMMENT 'The target inlong stream id',
    `audit_id`         varchar(100) NOT NULL DEFAULT '' COMMENT 'Audit id',
    `audit_tag`        varchar(100) DEFAULT '' COMMENT 'Audit tag',
    `audit_version`    BIGINT       DEFAULT -1  COMMENT 'Audit version',
    `count`            BIGINT       NOT NULL DEFAULT '0' COMMENT 'Message count',
    `size`             BIGINT       NOT NULL DEFAULT '0' COMMENT 'Message size',
    `delay`            BIGINT       NOT NULL DEFAULT '0' COMMENT 'Message delay count',
    `update_time`      timestamp    NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT 'Update time',
    INDEX group_stream_audit_id (`inlong_group_id`, `inlong_stream_id`, `audit_id`, `log_ts`)
) ENGINE = InnoDB
DEFAULT CHARSET = UTF8 COMMENT ='Inlong audit data table';

-- ----------------------------
-- Table structure for audit_data_temp
-- You can create daily partitions or hourly partitions through the log_ts field.
-- The specific partition type is determined based on the actual data volume.
-- ----------------------------
CREATE TABLE IF NOT EXISTS `audit_data_temp` (
    `log_ts`            datetime NOT NULL DEFAULT '0000-00-00 00:00:00' COMMENT 'log timestamp',
    `audit_id`          varchar(100) NOT NULL DEFAULT '' COMMENT 'Audit id',
    `inlong_group_id`   varchar(100) NOT NULL DEFAULT '' COMMENT 'The target inlong group id',
    `inlong_stream_id`  varchar(100) NOT NULL DEFAULT '' COMMENT 'The target inlong stream id',
    `audit_tag`         varchar(100) DEFAULT '' COMMENT 'Audit tag',
    `count`             BIGINT NOT NULL DEFAULT '0' COMMENT 'Message count',
    `size`              BIGINT NOT NULL DEFAULT '0' COMMENT 'Message size',
    `delay`             BIGINT NOT NULL DEFAULT '0' COMMENT 'Message delay count',
    `update_time`       timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT 'Update time',
    PRIMARY KEY (`log_ts`,`audit_id`,`inlong_group_id`,`inlong_stream_id`,`audit_tag`)
) ENGINE = InnoDB
DEFAULT CHARSET = UTF8  COMMENT ='InLong audit data temp table'
PARTITION BY RANGE (to_days(`log_ts`))
(PARTITION pDefault VALUES LESS THAN (TO_DAYS('1970-01-01')));

-- ----------------------------
-- Table structure for audit_data_day
-- You can create daily partitions through the log_ts field.
-- The specific partition type is determined based on the actual data volume.
-- ----------------------------
CREATE TABLE IF NOT EXISTS `audit_data_day`
(
    `log_ts`           date NOT NULL DEFAULT '0000-00-00' COMMENT 'log timestamp',
    `inlong_group_id`  varchar(100) NOT NULL DEFAULT '' COMMENT 'The target inlong group id',
    `inlong_stream_id` varchar(100) NOT NULL DEFAULT '' COMMENT 'The target inlong stream id',
    `audit_id`         varchar(100) NOT NULL DEFAULT '' COMMENT 'Audit id',
    `audit_tag`        varchar(100) DEFAULT '' COMMENT 'Audit tag',
    `count`            BIGINT       NOT NULL DEFAULT '0' COMMENT 'Message count',
    `size`             BIGINT       NOT NULL DEFAULT '0' COMMENT 'Message size',
    `delay`            BIGINT       NOT NULL DEFAULT '0' COMMENT 'Message delay count',
    `update_time`      timestamp    NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT 'Update time',
    PRIMARY KEY (`log_ts`,`inlong_group_id`,`inlong_stream_id`,`audit_id`,`audit_tag`)
) ENGINE = InnoDB
DEFAULT CHARSET = utf8 COMMENT ='Inlong audit data day table'
PARTITION BY RANGE (to_days(`log_ts`))
(PARTITION pDefault VALUES LESS THAN (TO_DAYS('1970-01-01')));

-- ----------------------------
-- Table structure for selector
-- ----------------------------
CREATE TABLE IF NOT EXISTS `leader_selector`
(
   `service_id` varchar(128) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL,
   `leader_id` varchar(128) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL,
   `last_seen_active` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
   PRIMARY KEY (`service_id`)
) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4 COLLATE = utf8mb4_general_ci COMMENT = 'selector db';

-- ----------------------------
-- Table structure for audit id config
-- ----------------------------
CREATE TABLE IF NOT EXISTS `audit_id_config`
(
    `audit_id` varchar(128) NOT NULL DEFAULT '' COMMENT 'Audit id',
    `status` int(11) DEFAULT '1' COMMENT 'Audit source config status. 0:Offline,1:Online',
    `update_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT 'Update time',
    PRIMARY KEY (`audit_id`)
) ENGINE = InnoDB DEFAULT CHARSET = UTF8 COMMENT = 'Audit id config';


-- ----------------------------
-- Table structure for audit source config
-- ----------------------------
CREATE TABLE IF NOT EXISTS `audit_source_config`
(
     `source_name` varchar(32) NOT NULL DEFAULT '' COMMENT 'Source_name',
     `jdbc_url` varchar(256) NOT NULL DEFAULT '' COMMENT 'Jdbc url',
     `jdbc_driver_class` varchar(128) NOT NULL DEFAULT '' COMMENT 'Jdbc driver class',
     `jdbc_user_name` varchar(128) NOT NULL DEFAULT '' COMMENT 'Jdbc url',
     `jdbc_password` varchar(128) NOT NULL DEFAULT '' COMMENT 'Jdbc password',
     `service_id` varchar(128) NOT NULL DEFAULT '' COMMENT 'Service id',
     `status` int(11) DEFAULT '1' COMMENT 'Audit source config status. 0:Offline,1:Online',
     `update_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT 'Update time',
     PRIMARY KEY (`source_name`, `jdbc_url`)
) ENGINE = InnoDB DEFAULT CHARSET = UTF8 COMMENT = 'Audit source config';


-----------------------------
-- Table structure for audit route config
-- ----------------------------
CREATE TABLE IF NOT EXISTS `audit_route_config` (
    `id` INT NOT NULL AUTO_INCREMENT COMMENT 'Primary key ID',
    `source_name` VARCHAR(255) NOT NULL COMMENT 'Source name',
    `address` VARCHAR(255) NOT NULL COMMENT 'Host address',
    `audit_id_include` VARCHAR(255) COMMENT 'Included audit IDs',
    `inlong_group_id_include` VARCHAR(255) COMMENT 'Included Inlong group IDs (regular expression)',
    `inlong_group_id_exclude` VARCHAR(255) COMMENT 'Excluded Inlong group IDs (regular expression)',
    `status` TINYINT NOT NULL DEFAULT 1 COMMENT 'Status, 1=active, 0=inactive',
    `update_time` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT 'Update timestamp',
    PRIMARY KEY (`id`)
) ENGINE = InnoDB DEFAULT CHARSET = UTF8 COMMENT='Audit route configuration table';

