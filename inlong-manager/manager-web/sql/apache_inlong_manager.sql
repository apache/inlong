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

SET NAMES utf8mb4;
SET FOREIGN_KEY_CHECKS = 0;

-- ----------------------------
-- database for Manager Web
-- ----------------------------
CREATE DATABASE IF NOT EXISTS apache_inlong_manager;
USE apache_inlong_manager;

-- ----------------------------
-- Table structure for agent_heartbeat
-- ----------------------------
DROP TABLE IF EXISTS `agent_heartbeat`;
CREATE TABLE `agent_heartbeat`
(
    `ip`            varchar(64) NOT NULL COMMENT 'agent host ip',
    `version`       varchar(128)         DEFAULT NULL,
    `heartbeat_msg` text                 DEFAULT NULL COMMENT 'massage in heartbeat request',
    `modify_time`   timestamp   NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT 'Modify time',
    PRIMARY KEY (`ip`)
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8 COMMENT ='Agent heartbeat information table';

-- ----------------------------
-- Table structure for agent_sys_conf
-- ----------------------------
DROP TABLE IF EXISTS `agent_sys_conf`;
CREATE TABLE `agent_sys_conf`
(
    `ip`                            varchar(64) NOT NULL COMMENT 'ip',
    `max_retry_threads`             int(11)     NOT NULL DEFAULT '6' COMMENT 'Maximum number of retry threads',
    `min_retry_threads`             int(11)     NOT NULL DEFAULT '3' COMMENT 'Minimum number of retry threads',
    `db_path`                       varchar(64)          DEFAULT '../db' COMMENT 'The path where bd is located, use a relative path',
    `scan_interval_sec`             int(11)     NOT NULL DEFAULT '30' COMMENT 'Interval time to scan file directory',
    `batch_size`                    int(11)     NOT NULL DEFAULT '20' COMMENT 'The amount sent to data proxy in batch',
    `msg_size`                      int(11)     NOT NULL DEFAULT '100' COMMENT 'As many packages as possible at one time',
    `send_runnable_size`            int(11)     NOT NULL DEFAULT '5' COMMENT 'The number of sending threads corresponding to a data source',
    `msg_queue_size`                int(11)              DEFAULT '500',
    `max_reader_cnt`                int(11)     NOT NULL DEFAULT '18' COMMENT 'The maximum number of threads of an Agent',
    `thread_manager_sleep_interval` int(11)     NOT NULL DEFAULT '30000' COMMENT 'Interval time between manager thread to taskManager to fetch tasks',
    `oneline_size`                  int(11)     NOT NULL DEFAULT '1048576' COMMENT 'Maximum length of a row of data',
    `clear_day_offset`              int(11)     NOT NULL DEFAULT '11' COMMENT 'How many days ago to clear the data of BDB',
    `clear_interval_sec`            int(11)     NOT NULL DEFAULT '86400' COMMENT 'Interval time for clearing bdb data',
    `buffer_size_in_bytes`          int(16)     NOT NULL DEFAULT '268435456' COMMENT 'Maximum memory occupied by msg buffer',
    `agent_rpc_reconnect_time`      int(11)     NOT NULL DEFAULT '0' COMMENT 'The interval time to update the link, if it is 0, it will not be updated',
    `send_timeout_mill_sec`         int(11)     NOT NULL DEFAULT '60000' COMMENT 'The timeout period for sending a message (if the packet is not full within one minute, it will be sent out forcibly)',
    `flush_event_timeout_mill_sec`  int(11)     NOT NULL DEFAULT '16000',
    `stat_interval_sec`             int(11)     NOT NULL DEFAULT '60' COMMENT 'Statistical message sending frequency',
    `conf_refresh_interval_secs`    int(11)     NOT NULL DEFAULT '300' COMMENT 'The frequency at which the Agent regularly pulls the configuration from the InLongManager',
    `flow_size`                     int(11)              DEFAULT '1048576000',
    `bufferSize`                    int(11)              DEFAULT '1048576' COMMENT 'bufferSize, default 1048576',
    `compress`                      tinyint(2)           DEFAULT NULL COMMENT 'Whether to compress',
    `event_check_interval`          int(11)              DEFAULT NULL COMMENT 'File scanning period',
    `is_calMD5`                     tinyint(2)           DEFAULT NULL COMMENT 'Do you want to calculate the cumulative md5 of read characters',
    PRIMARY KEY (`ip`)
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8 COMMENT ='Agent system configuration table';

-- ----------------------------
-- Table structure for inlong_group
-- ----------------------------
DROP TABLE IF EXISTS `inlong_group`;
CREATE TABLE `inlong_group`
(
    `id`                  int(11)      NOT NULL AUTO_INCREMENT COMMENT 'Incremental primary key',
    `inlong_group_id`     varchar(256) NOT NULL COMMENT 'Inlong group id, filled in by the user, undeleted ones cannot be repeated',
    `name`                varchar(128)      DEFAULT '' COMMENT 'Inlong group name, English, numbers and underscore',
    `cn_name`             varchar(256)      DEFAULT NULL COMMENT 'Chinese display name',
    `description`         varchar(256)      DEFAULT '' COMMENT 'Inlong group Introduction',
    `middleware_type`     varchar(20)       DEFAULT 'TUBE' COMMENT 'The middleware type of message queue, high throughput: TUBE, high consistency: PULSAR',
    `queue_module`        VARCHAR(20)  NULL DEFAULT 'parallel' COMMENT 'Queue model of Pulsar, parallel: multiple partitions, high throughput, out-of-order messages; serial: single partition, low throughput, and orderly messages',
    `topic_partition_num` INT(4)       NULL DEFAULT '3' COMMENT 'The number of partitions of Pulsar Topic, 1-20',
    `mq_resource_obj`     varchar(128) NOT NULL COMMENT 'MQ resource object, for Tube, its Topic, for Pulsar, its Namespace',
    `daily_records`       int(11)           DEFAULT '10' COMMENT 'Number of access records per day, unit: 10,000 records per day',
    `daily_storage`       int(11)           DEFAULT '10' COMMENT 'Access size by day, unit: GB per day',
    `peak_records`        int(11)           DEFAULT '1000' COMMENT 'Access peak per second, unit: records per second',
    `max_length`          int(11)           DEFAULT '10240' COMMENT 'The maximum length of a single piece of data, unit: Byte',
    `schema_name`         varchar(128)      DEFAULT NULL COMMENT 'Data type, associated data_schema table',
    `in_charges`          varchar(512) NOT NULL COMMENT 'Name of responsible person, separated by commas',
    `followers`           varchar(512)      DEFAULT NULL COMMENT 'Name of followers, separated by commas',
    `status`              int(4)            DEFAULT '21' COMMENT 'Inlong group status',
    `is_deleted`          int(11)           DEFAULT '0' COMMENT 'Whether to delete, 0: not deleted, > 0: deleted',
    `creator`             varchar(64)  NOT NULL COMMENT 'Creator name',
    `modifier`            varchar(64)       DEFAULT NULL COMMENT 'Modifier name',
    `create_time`         timestamp    NULL DEFAULT CURRENT_TIMESTAMP COMMENT 'Create time',
    `modify_time`         timestamp    NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT 'Modify time',
    `temp_view`           text              DEFAULT NULL COMMENT 'Temporary view, used to save intermediate data that has not been submitted or approved after modification',
    `zookeeper_enabled`   int(4)            DEFAULT '1' COMMENT 'Need zookeeper support, 0: false, 1: true',
    `proxy_cluster_id`    int(11)           DEFAULT NULL COMMENT 'The id of dataproxy cluster',
    PRIMARY KEY (`id`),
    UNIQUE KEY `unique_inlong_group` (`inlong_group_id`, `is_deleted`, `modify_time`)
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8mb4 COMMENT ='Inlong group table';

-- ----------------------------
-- Table structure for inlong_group_pulsar
-- ----------------------------
DROP TABLE IF EXISTS `inlong_group_pulsar`;
CREATE TABLE `inlong_group_pulsar`
(
    `id`                  int(11)      NOT NULL AUTO_INCREMENT COMMENT 'Incremental primary key',
    `inlong_group_id`     varchar(256) NOT NULL COMMENT 'Inlong group id, filled in by the user, undeleted ones cannot be repeated',
    `ensemble`            int(3)            DEFAULT '3' COMMENT 'The writable nodes number of ledger',
    `write_quorum`        int(3)            DEFAULT '3' COMMENT 'The copies number of ledger',
    `ack_quorum`          int(3)            DEFAULT '2' COMMENT 'The number of requested acks',
    `retention_time`      int(11)           DEFAULT '72' COMMENT 'Message storage time',
    `retention_time_unit` char(20)          DEFAULT 'hours' COMMENT 'The unit of the message storage time',
    `ttl`                 int(11)           DEFAULT '24' COMMENT 'Message time-to-live duration',
    `ttl_unit`            varchar(20)       DEFAULT 'hours' COMMENT 'The unit of time-to-live duration',
    `retention_size`      int(11)           DEFAULT '-1' COMMENT 'Message size',
    `retention_size_unit` varchar(20)       DEFAULT 'MB' COMMENT 'The unit of message size',
    `is_deleted`          int(11)           DEFAULT '0' COMMENT 'Whether to delete, 0: not deleted, > 0: deleted',
    `create_time`         timestamp    NULL DEFAULT CURRENT_TIMESTAMP COMMENT 'Create time',
    `modify_time`         timestamp    NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT 'Modify time',
    PRIMARY KEY (`id`)
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8mb4 COMMENT ='Pulsar info table';

-- ----------------------------
-- Table structure for inlong_group_ext
-- ----------------------------
DROP TABLE IF EXISTS `inlong_group_ext`;
CREATE TABLE `inlong_group_ext`
(
    `id`              int(11)      NOT NULL AUTO_INCREMENT COMMENT 'Incremental primary key',
    `inlong_group_id` varchar(256) NOT NULL COMMENT 'Inlong group id',
    `key_name`        varchar(256) NOT NULL COMMENT 'Configuration item name',
    `key_value`       text              DEFAULT NULL COMMENT 'The value of the configuration item',
    `is_deleted`      int(11)           DEFAULT '0' COMMENT 'Whether to delete, 0: not deleted, > 0: deleted',
    `modify_time`     timestamp    NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT 'Modify time',
    PRIMARY KEY (`id`),
    KEY `index_group_id` (`inlong_group_id`),
    UNIQUE KEY `group_key_idx` (`inlong_group_id`, `key_name`)
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8mb4 COMMENT ='Inlong group extension table';

-- ----------------------------
-- Table structure for third_party_cluster
-- ----------------------------
DROP TABLE IF EXISTS `third_party_cluster`;
CREATE TABLE `third_party_cluster`
(
    `id`          int(11)      NOT NULL AUTO_INCREMENT COMMENT 'Incremental primary key',
    `name`        varchar(128) NOT NULL COMMENT 'Cluster name',
    `type`        varchar(32)  NOT NULL COMMENT 'Cluster type, including TUBE, PULSAR, etc.',
    `ip`          varchar(64)  NOT NULL COMMENT 'Cluster IP',
    `port`        int(11)      NOT NULL COMMENT 'Cluster port',
    `token`       varchar(128) COMMENT 'Cluster token',
    `url`         varchar(256)      DEFAULT NULL COMMENT 'Cluster URL address',
    `is_backup`   tinyint(1)        DEFAULT '0' COMMENT 'Whether it is a backup cluster, 0: no, 1: yes',
    `mq_set_name` varchar(128) NULL COMMENT 'MQ set name of this cluster',
    `ext_params`  text              DEFAULT NULL COMMENT 'Extended params',
    `in_charges`  varchar(512) NOT NULL COMMENT 'Name of responsible person, separated by commas',
    `status`      int(4)            DEFAULT '1' COMMENT 'Cluster status',
    `is_deleted`  int(11)           DEFAULT '0' COMMENT 'Whether to delete, 0: not deleted, > 0: deleted',
    `creator`     varchar(64)  NOT NULL COMMENT 'Creator name',
    `modifier`    varchar(64)       DEFAULT NULL COMMENT 'Modifier name',
    `create_time` timestamp    NULL DEFAULT CURRENT_TIMESTAMP COMMENT 'Create time',
    `modify_time` timestamp    NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT 'Modify time',
    PRIMARY KEY (`id`)
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8mb4 COMMENT ='MQ Cluster Information Table';

-- ----------------------------
-- Table structure for common_db_server
-- ----------------------------
DROP TABLE IF EXISTS `common_db_server`;
CREATE TABLE `common_db_server`
(
    `id`                  int(11)      NOT NULL AUTO_INCREMENT COMMENT 'Incremental primary key',
    `access_type`         varchar(20)  NOT NULL COMMENT 'Collection type, with Agent, DataProxy client, LoadProxy',
    `connection_name`     varchar(128) NOT NULL COMMENT 'The name of the database connection',
    `db_type`             varchar(128)      DEFAULT 'MySQL' COMMENT 'DB type, such as MySQL, Oracle',
    `db_server_ip`        varchar(64)  NOT NULL COMMENT 'DB Server IP',
    `port`                int(11)      NOT NULL COMMENT 'Port number',
    `db_name`             varchar(128)      DEFAULT NULL COMMENT 'Target database name',
    `username`            varchar(64)  NOT NULL COMMENT 'Username',
    `password`            varchar(64)  NOT NULL COMMENT 'The password corresponding to the above user name',
    `has_select`          tinyint(1)        DEFAULT '0' COMMENT 'Is there DB permission select, 0: No, 1: Yes',
    `has_insert`          tinyint(1)        DEFAULT '0' COMMENT 'Is there DB permission to insert, 0: No, 1: Yes',
    `has_update`          tinyint(1)        DEFAULT '0' COMMENT 'Is there a DB permission update, 0: No, 1: Yes',
    `has_delete`          tinyint(1)        DEFAULT '0' COMMENT 'Is there a DB permission to delete, 0: No, 1: Yes',
    `in_charges`          varchar(512) NOT NULL COMMENT 'DB person in charge, separated by a comma when there are multiple ones',
    `is_region_id`        tinyint(1)        DEFAULT '0' COMMENT 'Whether it contains a region ID, 0: No, 1: Yes',
    `db_description`      varchar(256)      DEFAULT NULL COMMENT 'DB description',
    `backup_db_server_ip` varchar(64)       DEFAULT NULL COMMENT 'Backup DB HOST',
    `backup_db_port`      int(11)           DEFAULT NULL COMMENT 'Backup DB port',
    `status`              int(4)            DEFAULT '0' COMMENT 'status',
    `is_deleted`          int(11)           DEFAULT '0' COMMENT 'Whether to delete, 0: not deleted, > 0: deleted',
    `creator`             varchar(64)  NOT NULL COMMENT 'Creator name',
    `modifier`            varchar(64)       DEFAULT NULL COMMENT 'Modifier name',
    `create_time`         timestamp    NULL DEFAULT CURRENT_TIMESTAMP COMMENT 'Create time',
    `modify_time`         timestamp    NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT 'Modify time',
    `visible_person`      varchar(1024)     DEFAULT NULL COMMENT 'List of visible persons, separated by commas',
    `visible_group`       varchar(1024)     DEFAULT NULL COMMENT 'List of visible groups, separated by commas',
    PRIMARY KEY (`id`)
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8mb4 COMMENT ='Common Database Server Table';

-- ----------------------------
-- Table structure for common_file_server
-- ----------------------------
DROP TABLE IF EXISTS `common_file_server`;
CREATE TABLE `common_file_server`
(
    `id`             int(11)     NOT NULL AUTO_INCREMENT COMMENT 'Incremental primary key',
    `access_type`    varchar(20) NOT NULL COMMENT 'Collection type, with Agent, DataProxy, LoadProxy',
    `ip`             varchar(64) NOT NULL COMMENT 'Data source IP',
    `port`           int(11)     NOT NULL COMMENT 'Port number',
    `is_inner_ip`    tinyint(1)           DEFAULT '0' COMMENT 'Whether it is intranet, 0: No, 1: Yes',
    `issue_type`     varchar(128)         DEFAULT NULL COMMENT 'Issuance method, such as SSH, TCS, etc.',
    `username`       varchar(64) NOT NULL COMMENT 'User name of the data source IP host',
    `password`       varchar(64) NOT NULL COMMENT 'The password corresponding to the above user name',
    `status`         int(4)               DEFAULT '0' COMMENT 'status',
    `is_deleted`     int(11)              DEFAULT '0' COMMENT 'Whether to delete, 0: not deleted, > 0: deleted',
    `creator`        varchar(64) NOT NULL COMMENT 'Creator name',
    `modifier`       varchar(64)          DEFAULT NULL COMMENT 'Modifier name',
    `create_time`    timestamp   NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT 'Create time',
    `modify_time`    timestamp   NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT 'Modify time',
    `visible_person` varchar(1024)        DEFAULT NULL COMMENT 'List of visible persons, separated by commas',
    `visible_group`  varchar(1024)        DEFAULT NULL COMMENT 'List of visible groups, separated by commas',
    PRIMARY KEY (`id`)
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8mb4 COMMENT ='Common File Server Table';

-- ----------------------------
-- Table structure for consumption
-- ----------------------------
DROP TABLE IF EXISTS `consumption`;
CREATE TABLE `consumption`
(
    `id`                  int(11)      NOT NULL AUTO_INCREMENT COMMENT 'Incremental primary key',
    `consumer_group_name` varchar(256)      DEFAULT NULL COMMENT 'consumer group name',
    `consumer_group_id`   varchar(256) NOT NULL COMMENT 'Consumer group ID',
    `in_charges`          varchar(512) NOT NULL COMMENT 'Person in charge of consumption',
    `inlong_group_id`     varchar(256) NOT NULL COMMENT 'Inlong group id',
    `middleware_type`     varchar(10)       DEFAULT 'TUBE' COMMENT 'The middleware type of message queue, high throughput: TUBE, high consistency: PULSAR',
    `topic`               varchar(256) NOT NULL COMMENT 'Consumption topic',
    `filter_enabled`      int(2)            DEFAULT '0' COMMENT 'Whether to filter, default 0, not filter consume',
    `inlong_stream_id`    varchar(256)      DEFAULT NULL COMMENT 'Inlong stream ID for consumption, if filter_enable is 1, it cannot empty',
    `status`              int(4)       NOT NULL COMMENT 'Status: draft, pending approval, approval rejected, approval passed',
    `is_deleted`          int(11)           DEFAULT '0' COMMENT 'Whether to delete, 0: not deleted, > 0: deleted',
    `creator`             varchar(64)  NOT NULL COMMENT 'creator',
    `modifier`            varchar(64)       DEFAULT NULL COMMENT 'modifier',
    `create_time`         timestamp    NULL DEFAULT CURRENT_TIMESTAMP COMMENT 'Create time',
    `modify_time`         timestamp    NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT 'Modify time',
    PRIMARY KEY (`id`)
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8mb4 COMMENT ='Data consumption configuration table';

-- ----------------------------
-- Table structure for consumption_pulsar
-- ----------------------------
DROP TABLE IF EXISTS `consumption_pulsar`;
CREATE TABLE `consumption_pulsar`
(
    `id`                  int(11)      NOT NULL AUTO_INCREMENT,
    `consumption_id`      int(11)      DEFAULT NULL COMMENT 'ID of the consumption information to which it belongs, guaranteed to be uniquely associated with consumption information',
    `consumer_group_id`   varchar(256) NOT NULL COMMENT 'Consumer group ID',
    `consumer_group_name` varchar(256) DEFAULT NULL COMMENT 'Consumer group name',
    `inlong_group_id`     varchar(256) NOT NULL COMMENT 'Inlong group ID',
    `is_rlq`              tinyint(1)   DEFAULT '0' COMMENT 'Whether to configure the retry letter topic, 0: no configuration, 1: configuration',
    `retry_letter_topic`  varchar(256) DEFAULT NULL COMMENT 'The name of the retry queue topic',
    `is_dlq`              tinyint(1)   DEFAULT '0' COMMENT 'Whether to configure dead letter topic, 0: no configuration, 1: means configuration',
    `dead_letter_topic`   varchar(256) DEFAULT NULL COMMENT 'dead letter topic name',
    `is_deleted`          int(11)      DEFAULT '0' COMMENT 'Whether to delete, 0: not deleted, > 0: deleted',
    PRIMARY KEY (`id`)
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8mb4 COMMENT ='Pulsar consumption table';

-- ----------------------------
-- Table structure for data_proxy_cluster
-- ----------------------------
DROP TABLE IF EXISTS `data_proxy_cluster`;
CREATE TABLE `data_proxy_cluster`
(
    `id`          int(11)      NOT NULL AUTO_INCREMENT COMMENT 'Incremental primary key',
    `name`        varchar(128) NOT NULL COMMENT 'Cluster name',
    `description` varchar(500)      DEFAULT NULL COMMENT 'Cluster description',
    `address`     varchar(128) NOT NULL COMMENT 'Cluster address',
    `port`        varchar(256)      DEFAULT '46801' COMMENT 'Access port number, multiple ports are separated by a comma',
    `is_backup`   tinyint(1)        DEFAULT '0' COMMENT 'Whether it is a backup cluster, 0: no, 1: yes',
    `mq_set_name` varchar(128) NULL COMMENT 'MQ set name of this cluster',
    `ext_params`  text              DEFAULT NULL COMMENT 'Extended params',
    `in_charges`  varchar(512)      DEFAULT NULL COMMENT 'Name of responsible person, separated by commas',
    `status`      int(4)            DEFAULT '1' COMMENT 'Cluster status',
    `is_deleted`  int(11)           DEFAULT '0' COMMENT 'Whether to delete, 0: not deleted, > 0: deleted',
    `creator`     varchar(64)  NOT NULL COMMENT 'Creator name',
    `modifier`    varchar(64)       DEFAULT NULL COMMENT 'Modifier name',
    `create_time` timestamp    NULL DEFAULT CURRENT_TIMESTAMP COMMENT 'Create time',
    `modify_time` timestamp    NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT 'Modify time',
    PRIMARY KEY (`id`),
    UNIQUE KEY `cluster_name` (`name`, `is_deleted`)
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8mb4 COMMENT ='DataProxy cluster table';
-- add default data proxy address
insert into data_proxy_cluster (name, address, port, status, is_deleted, creator, create_time, modify_time)
values ("default_dataproxy", "dataproxy", 46801, 0, 0, "admin", now(), now());

-- ----------------------------
-- Table structure for data_schema
-- ----------------------------
DROP TABLE IF EXISTS `data_schema`;
CREATE TABLE `data_schema`
(
    `id`                 int(11)      NOT NULL AUTO_INCREMENT COMMENT 'Incremental primary key',
    `name`               varchar(128) NOT NULL COMMENT 'Data format name, globally unique',
    `agent_type`         varchar(20)  NOT NULL COMMENT 'Agent type: file, db_incr, db_full',
    `data_generate_rule` varchar(32)  NOT NULL COMMENT 'Data file generation rules, including day and hour',
    `sort_type`          int(11)      NOT NULL COMMENT 'sort logic rules, 0, 5, 9, 10, 13, 15',
    `time_offset`        varchar(10)  NOT NULL COMMENT 'time offset',
    PRIMARY KEY (`id`),
    UNIQUE KEY `name` (`name`)
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8mb4 COMMENT ='Data format table';

-- create default data schema
INSERT INTO `data_schema` (name, agent_type, data_generate_rule, sort_type, time_offset)
values ('m0_day', 'file_agent', 'day', 0, '-0d');

-- ----------------------------
-- Table structure for stream_source_cmd_config
-- ----------------------------
DROP TABLE IF EXISTS `stream_source_cmd_config`;
CREATE TABLE `stream_source_cmd_config`
(
    `id`                  int(11)     NOT NULL AUTO_INCREMENT COMMENT 'cmd id',
    `cmd_type`            int(11)     NOT NULL,
    `task_id`             int(11)     NOT NULL,
    `specified_data_time` varchar(64) NOT NULL,
    `bSend`               tinyint(1)  NOT NULL,
    `create_time`         timestamp   NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT 'Create time',
    `modify_time`         timestamp   NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT 'Modify time',
    `result_info`         varchar(64)          DEFAULT NULL,
    PRIMARY KEY (`id`),
    KEY `index_1` (`task_id`, `bSend`, `specified_data_time`)
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8;

-- ----------------------------
-- Table structure for inlong_stream
-- ----------------------------
DROP TABLE IF EXISTS `inlong_stream`;
CREATE TABLE `inlong_stream`
(
    `id`                     int(11)      NOT NULL AUTO_INCREMENT COMMENT 'Incremental primary key',
    `inlong_stream_id`       varchar(256) NOT NULL COMMENT 'Inlong stream id, non-deleted globally unique',
    `inlong_group_id`        varchar(256) NOT NULL COMMENT 'Owning inlong group id',
    `name`                   varchar(64)       DEFAULT NULL COMMENT 'The name of the inlong stream page display, can be Chinese',
    `description`            varchar(256)      DEFAULT '' COMMENT 'Introduction to inlong stream',
    `mq_resource_obj`        varchar(128)      DEFAULT NULL COMMENT 'MQ resource object, in the inlong stream, Tube is inlong_stream_id, Pulsar is Topic',
    `data_source_type`       varchar(32)       DEFAULT 'FILE' COMMENT 'Data source type, including: FILE, DB, Auto-Push (DATA_PROXY_SDK, HTTP)',
    `storage_period`         int(11)           DEFAULT '1' COMMENT 'The storage period of data in MQ, unit: day',
    `data_type`              varchar(20)       DEFAULT 'TEXT' COMMENT 'Data type, there are: TEXT, KEY-VALUE, PB, BON, TEXT and BON should be treated differently',
    `data_encoding`          varchar(8)        DEFAULT 'UTF-8' COMMENT 'Data encoding format, including: UTF-8, GBK',
    `data_separator`         varchar(8)        DEFAULT NULL COMMENT 'The source data field separator, stored as ASCII code',
    `data_escape_char`       varchar(8)        DEFAULT NULL COMMENT 'Source data field escape character, the default is NULL (NULL), stored as 1 character',
    `have_predefined_fields` tinyint(1)        DEFAULT '0' COMMENT '(File, DB access) whether there are predefined fields, 0: none, 1: yes (save to inlong_stream_field)',
    `daily_records`          int(11)           DEFAULT '10' COMMENT 'Number of access records per day, unit: 10,000 records per day',
    `daily_storage`          int(11)           DEFAULT '10' COMMENT 'Access size by day, unit: GB per day',
    `peak_records`           int(11)           DEFAULT '1000' COMMENT 'Access peak per second, unit: records per second',
    `max_length`             int(11)           DEFAULT '10240' COMMENT 'The maximum length of a single piece of data, unit: Byte',
    `in_charges`             varchar(512)      DEFAULT NULL COMMENT 'Name of responsible person, separated by commas',
    `status`                 int(4)            DEFAULT '0' COMMENT 'Inlong stream status',
    `previous_status`        int(4)            DEFAULT '0' COMMENT 'Previous status',
    `is_deleted`             int(11)           DEFAULT '0' COMMENT 'Whether to delete, 0: not deleted, > 0: deleted',
    `creator`                varchar(64)       DEFAULT NULL COMMENT 'Creator name',
    `modifier`               varchar(64)       DEFAULT NULL COMMENT 'Modifier name',
    `create_time`            timestamp    NULL DEFAULT CURRENT_TIMESTAMP COMMENT 'Create time',
    `modify_time`            timestamp    NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT 'Modify time',
    `temp_view`              text              DEFAULT NULL COMMENT 'Temporary view, used to save intermediate data that has not been submitted or approved after modification',
    PRIMARY KEY (`id`),
    UNIQUE KEY `unique_inlong_stream` (`inlong_stream_id`, `inlong_group_id`, `is_deleted`, `modify_time`)
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8mb4 COMMENT ='Inlong stream table';

-- ----------------------------
-- Table structure for inlong_stream_ext
-- ----------------------------
DROP TABLE IF EXISTS `inlong_stream_ext`;
CREATE TABLE `inlong_stream_ext`
(
    `id`               int(11)      NOT NULL AUTO_INCREMENT COMMENT 'Incremental primary key',
    `inlong_group_id`  varchar(256) NOT NULL COMMENT 'Owning inlong group id',
    `inlong_stream_id` varchar(256) NOT NULL COMMENT 'Owning inlong stream id',
    `key_name`         varchar(256) NOT NULL COMMENT 'Configuration item name',
    `key_value`        text              DEFAULT NULL COMMENT 'The value of the configuration item',
    `is_deleted`       int(11)           DEFAULT '0' COMMENT 'Whether to delete, 0: not deleted, > 0: deleted',
    `modify_time`      timestamp    NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT 'Modify time',
    PRIMARY KEY (`id`),
    KEY `index_stream_id` (`inlong_stream_id`),
    UNIQUE KEY `group_stream_key_idx` (`inlong_group_id`, `inlong_stream_id`, `key_name`)
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8mb4 COMMENT ='Inlong stream extension table';

-- ----------------------------
-- Table structure for inlong_stream_field
-- ----------------------------
DROP TABLE IF EXISTS `inlong_stream_field`;
CREATE TABLE `inlong_stream_field`
(
    `id`                  int(11)      NOT NULL AUTO_INCREMENT COMMENT 'Incremental primary key',
    `inlong_group_id`     varchar(256) NOT NULL COMMENT 'Owning inlong group id',
    `inlong_stream_id`    varchar(256) NOT NULL COMMENT 'Owning inlong stream id',
    `is_predefined_field` tinyint(1)   DEFAULT '0' COMMENT 'Whether it is a predefined field, 0: no, 1: yes',
    `field_name`          varchar(20)  NOT NULL COMMENT 'field name',
    `field_value`         varchar(128) DEFAULT NULL COMMENT 'Field value, required if it is a predefined field',
    `pre_expression`      varchar(256) DEFAULT NULL COMMENT 'Pre-defined field value expression',
    `field_type`          varchar(20)  NOT NULL COMMENT 'field type',
    `field_comment`       varchar(50)  DEFAULT NULL COMMENT 'Field description',
    `rank_num`            smallint(6)  DEFAULT '0' COMMENT 'Field order (front-end display field order)',
    `is_deleted`          int(11)      DEFAULT '0' COMMENT 'Whether to delete, 0: not deleted, > 0: deleted',
    PRIMARY KEY (`id`),
    KEY `index_field_stream_id` (`inlong_stream_id`)
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8mb4 COMMENT ='File/DB data source field table';

-- ----------------------------
-- Table structure for operation_log
-- ----------------------------
DROP TABLE IF EXISTS `operation_log`;
CREATE TABLE `operation_log`
(
    `id`                  int(11)   NOT NULL AUTO_INCREMENT,
    `authentication_type` varchar(64)        DEFAULT NULL COMMENT 'Authentication type',
    `operation_type`      varchar(256)       DEFAULT NULL COMMENT 'operation type',
    `http_method`         varchar(64)        DEFAULT NULL COMMENT 'Request method',
    `invoke_method`       varchar(256)       DEFAULT NULL COMMENT 'invoke method',
    `operator`            varchar(256)       DEFAULT NULL COMMENT 'operator',
    `proxy`               varchar(256)       DEFAULT NULL COMMENT 'proxy',
    `request_url`         varchar(256)       DEFAULT NULL COMMENT 'Request URL',
    `remote_address`      varchar(256)       DEFAULT NULL COMMENT 'Request IP',
    `cost_time`           bigint(20)         DEFAULT NULL COMMENT 'time-consuming',
    `body`                text COMMENT 'Request body',
    `param`               text COMMENT 'parameter',
    `status`              int(4)             DEFAULT NULL COMMENT 'status',
    `request_time`        timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT 'request time',
    `err_msg`             text COMMENT 'Error message',
    PRIMARY KEY (`id`)
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8mb4;

-- ----------------------------
-- Table structure for role
-- ----------------------------
DROP TABLE IF EXISTS `role`;
CREATE TABLE `role`
(
    `id`          int(11)      NOT NULL AUTO_INCREMENT,
    `role_code`   varchar(100) NOT NULL COMMENT 'Role code',
    `role_name`   varchar(256) NOT NULL COMMENT 'Role Chinese name',
    `create_time` datetime     NOT NULL,
    `update_time` datetime     NOT NULL DEFAULT CURRENT_TIMESTAMP,
    `create_by`   varchar(256) NOT NULL,
    `update_by`   varchar(256) NOT NULL,
    `disabled`    tinyint(1)   NOT NULL DEFAULT '0' COMMENT 'Is it disabled?',
    PRIMARY KEY (`id`),
    UNIQUE KEY `unique_role_code_idx` (`role_code`),
    UNIQUE KEY `unique_role_name_idx` (`role_name`)
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8mb4 COMMENT ='Role Table';

-- ----------------------------
-- Table structure for source_db_basic
-- ----------------------------
DROP TABLE IF EXISTS `source_db_basic`;
CREATE TABLE `source_db_basic`
(
    `id`               int(11)      NOT NULL AUTO_INCREMENT COMMENT 'Incremental primary key',
    `inlong_group_id`  varchar(256) NOT NULL COMMENT 'Owning inlong group id',
    `inlong_stream_id` varchar(256) NOT NULL COMMENT 'Owning inlong stream id',
    `sync_type`        tinyint(1)        DEFAULT '0' COMMENT 'Data synchronization type, 0: FULL, full amount, 1: INCREMENTAL, incremental',
    `is_deleted`       int(11)           DEFAULT '0' COMMENT 'Whether to delete, 0: not deleted, > 0: deleted',
    `creator`          varchar(64)  NOT NULL COMMENT 'Creator name',
    `modifier`         varchar(64)       DEFAULT NULL COMMENT 'Modifier name',
    `create_time`      timestamp    NULL DEFAULT CURRENT_TIMESTAMP COMMENT 'Create time',
    `modify_time`      timestamp    NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT 'Modify time',
    `temp_view`        text              DEFAULT NULL COMMENT 'Temporary view, used to save intermediate data that has not been submitted or approved after modification',
    PRIMARY KEY (`id`)
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8mb4 COMMENT ='Basic configuration of DB data source';

-- ----------------------------
-- Table structure for source_db_detail
-- ----------------------------
DROP TABLE IF EXISTS `source_db_detail`;
CREATE TABLE `source_db_detail`
(
    `id`               int(11)      NOT NULL AUTO_INCREMENT COMMENT 'Incremental primary key',
    `inlong_group_id`  varchar(256) NOT NULL COMMENT 'Owning inlong group id',
    `inlong_stream_id` varchar(256) NOT NULL COMMENT 'Owning inlong stream id',
    `access_type`      varchar(20)  NOT NULL COMMENT 'Collection type, with Agent, DataProxy client, LoadProxy',
    `db_name`          varchar(128)      DEFAULT NULL COMMENT 'database name',
    `transfer_ip`      varchar(64)       DEFAULT NULL COMMENT 'Transfer IP',
    `connection_name`  varchar(128)      DEFAULT NULL COMMENT 'The name of the database connection',
    `table_name`       varchar(128)      DEFAULT NULL COMMENT 'Data table name, required for increment',
    `table_fields`     longtext COMMENT 'Data table fields, multiple are separated by half-width commas, required for increment',
    `data_sql`         longtext COMMENT 'SQL statement to collect source data, required for full amount',
    `crontab`          varchar(56)       DEFAULT NULL COMMENT 'Timed scheduling expression, required for full amount',
    `status`           int(4)            DEFAULT '0' COMMENT 'Data source status',
    `previous_status`  int(4)            DEFAULT '0' COMMENT 'Previous status',
    `is_deleted`       int(11)           DEFAULT '0' COMMENT 'Whether to delete, 0: not deleted, > 0: deleted',
    `creator`          varchar(64)  NOT NULL COMMENT 'Creator name',
    `modifier`         varchar(64)       DEFAULT NULL COMMENT 'Modifier name',
    `create_time`      timestamp    NULL DEFAULT CURRENT_TIMESTAMP COMMENT 'Create time',
    `modify_time`      timestamp    NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT 'Modify time',
    `temp_view`        text              DEFAULT NULL COMMENT 'Temporary view, used to save un-submitted and unapproved intermediate data after modification',
    PRIMARY KEY (`id`)
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8mb4 COMMENT ='DB data source details table';

-- ----------------------------
-- Table structure for source_file_basic
-- ----------------------------
DROP TABLE IF EXISTS `source_file_basic`;
CREATE TABLE `source_file_basic`
(
    `id`                int(11)      NOT NULL AUTO_INCREMENT COMMENT 'ID',
    `inlong_group_id`   varchar(256) NOT NULL COMMENT 'Inlong group id',
    `inlong_stream_id`  varchar(256) NOT NULL COMMENT 'Inlong stream id',
    `is_hybrid_source`  tinyint(1)        DEFAULT '0' COMMENT 'Whether to mix data sources',
    `is_table_mapping`  tinyint(1)        DEFAULT '0' COMMENT 'Is there a table name mapping',
    `date_offset`       int(4)            DEFAULT '0' COMMENT 'Time offset\n',
    `date_offset_unit`  varchar(2)        DEFAULT 'H' COMMENT 'Time offset unit',
    `file_rolling_type` varchar(2)        DEFAULT 'H' COMMENT 'File rolling type',
    `upload_max_size`   int(4)            DEFAULT '120' COMMENT 'Upload maximum size',
    `need_compress`     tinyint(1)        DEFAULT '0' COMMENT 'Whether need compress',
    `is_deleted`        int(11)           DEFAULT '0' COMMENT 'Whether to delete, 0: not deleted, > 0: deleted',
    `creator`           varchar(64)  NOT NULL COMMENT 'Creator',
    `modifier`          varchar(64)       DEFAULT NULL COMMENT 'Modifier',
    `create_time`       timestamp    NULL DEFAULT CURRENT_TIMESTAMP COMMENT 'Create time',
    `modify_time`       timestamp    NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT 'Modify time',
    `temp_view`         text              DEFAULT NULL COMMENT 'temp view',
    PRIMARY KEY (`id`)
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8mb4 COMMENT ='basic configuration of file data source';

-- ----------------------------
-- Table structure for source_file_detail
-- ----------------------------
DROP TABLE IF EXISTS `source_file_detail`;
CREATE TABLE `source_file_detail`
(
    `id`               int(11)      NOT NULL AUTO_INCREMENT COMMENT 'Incremental primary key',
    `inlong_group_id`  varchar(256) NOT NULL COMMENT 'Owning inlong group id',
    `inlong_stream_id` varchar(256) NOT NULL COMMENT 'Owning inlong stream id',
    `access_type`      varchar(20)       DEFAULT 'Agent' COMMENT 'Collection type, there are Agent, DataProxy client, LoadProxy, the file can only be Agent temporarily',
    `server_name`      varchar(64)       DEFAULT NULL COMMENT 'The name of the data source service. If it is empty, add configuration through the following fields',
    `ip`               varchar(128) NOT NULL COMMENT 'Data source IP address',
    `port`             int(11)      NOT NULL COMMENT 'Data source port number',
    `is_inner_ip`      tinyint(1)        DEFAULT '0' COMMENT 'Whether it is intranet, 0: no, 1: yes',
    `issue_type`       varchar(10)       DEFAULT 'SSH' COMMENT 'Issuing method, there are SSH, TCS',
    `username`         varchar(32)       DEFAULT NULL COMMENT 'User name of the data source IP host',
    `password`         varchar(64)       DEFAULT NULL COMMENT 'The password corresponding to the above user name',
    `file_path`        varchar(256) NOT NULL COMMENT 'File path, supports regular matching',
    `status`           int(4)            DEFAULT '0' COMMENT 'Data source status',
    `previous_status`  int(4)            DEFAULT '0' COMMENT 'Previous status',
    `is_deleted`       int(11)           DEFAULT '0' COMMENT 'Whether to delete, 0: not deleted, > 0: deleted',
    `creator`          varchar(64)  NOT NULL COMMENT 'Creator name',
    `modifier`         varchar(64)       DEFAULT NULL COMMENT 'Modifier name',
    `create_time`      timestamp    NULL DEFAULT CURRENT_TIMESTAMP COMMENT 'Create time',
    `modify_time`      timestamp    NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT 'Modify time',
    `temp_view`        text              DEFAULT NULL COMMENT 'Temporary view, used to save un-submitted and unapproved intermediate data after modification',
    PRIMARY KEY (`id`)
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8mb4 COMMENT ='Detailed table of file data source';

-- ----------------------------
-- Table structure for stream_source
-- ----------------------------
DROP TABLE IF EXISTS `stream_source`;
CREATE TABLE `stream_source`
(
    `id`               int(11)      NOT NULL AUTO_INCREMENT COMMENT 'ID',
    `inlong_group_id`  varchar(256) NOT NULL COMMENT 'Inlong group id',
    `inlong_stream_id` varchar(256) NOT NULL COMMENT 'Inlong stream id',
    `source_type`      varchar(20)       DEFAULT '0' COMMENT 'Source type, including: FILE, DB, etc',
    `agent_ip`         varchar(40)       DEFAULT NULL COMMENT 'Ip of the agent running the task',
    `uuid`             varchar(30)       DEFAULT NULL COMMENT 'Mac uuid of the agent running the task',
    `server_id`        int(11)           DEFAULT NULL COMMENT 'Id of the source server',
    `server_name`      varchar(50)       DEFAULT '' COMMENT 'Name of the source server',
    `cluster_id`       int(11)           DEFAULT NULL COMMENT 'Id of the cluster that collected this source',
    `cluster_name`     varchar(50)       DEFAULT '' COMMENT 'Name of the cluster that collected this source',
    `snapshot`         text              DEFAULT NULL COMMENT 'Snapshot of this source task',
    `report_time`      timestamp    NULL COMMENT 'Snapshot time',
    `ext_params`       text              DEFAULT NULL COMMENT 'Another fields will saved as JSON string, such as filePath, dbName, tableName, etc',
    `status`           int(4)            DEFAULT '0' COMMENT 'Data source status',
    `previous_status`  int(4)            DEFAULT '0' COMMENT 'Previous status',
    `is_deleted`       int(11)           DEFAULT '0' COMMENT 'Whether to delete, 0: not deleted, > 0: deleted',
    `creator`          varchar(64)  NOT NULL COMMENT 'Creator name',
    `modifier`         varchar(64)       DEFAULT NULL COMMENT 'Modifier name',
    `create_time`      timestamp    NULL DEFAULT CURRENT_TIMESTAMP COMMENT 'Create time',
    `modify_time`      timestamp    NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT 'Modify time',
    PRIMARY KEY (`id`)
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8mb4 COMMENT ='Stream source table';

-- ----------------------------
-- Table structure for stream_sink
-- ----------------------------
DROP TABLE IF EXISTS `stream_sink`;
CREATE TABLE `stream_sink`
(
    `id`                     int(11)      NOT NULL AUTO_INCREMENT COMMENT 'Incremental primary key',
    `inlong_group_id`        varchar(256) NOT NULL COMMENT 'Owning inlong group id',
    `inlong_stream_id`       varchar(256) NOT NULL COMMENT 'Owning inlong stream id',
    `sink_type`              varchar(15)           DEFAULT 'HIVE' COMMENT 'Sink type, including: HIVE, ES, etc',
    `storage_period`         int(11)               DEFAULT '10' COMMENT 'Data storage period, unit: day',
    `enable_create_resource` tinyint(1)            DEFAULT '1' COMMENT 'Whether to enable create sink resource? 0: disable, 1: enable. default is 1',
    `ext_params`             text COMMENT 'Another fields, will saved as JSON type',
    `operate_log`            varchar(5000)         DEFAULT NULL COMMENT 'Background operate log',
    `status`                 int(11)               DEFAULT '0' COMMENT 'Status',
    `previous_status`        int(11)               DEFAULT '0' COMMENT 'Previous status',
    `is_deleted`             int(11)               DEFAULT '0' COMMENT 'Whether to delete, 0: not deleted, > 0: deleted',
    `creator`                varchar(64)  NOT NULL COMMENT 'Creator name',
    `modifier`               varchar(64)           DEFAULT NULL COMMENT 'Modifier name',
    `create_time`            timestamp    NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT 'Create time',
    `modify_time`            timestamp    NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT 'Modify time',
    PRIMARY KEY (`id`)
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8mb4 COMMENT ='Stream sink table';

-- ----------------------------
-- Table structure for stream_sink_ext
-- ----------------------------
DROP TABLE IF EXISTS `stream_sink_ext`;
CREATE TABLE `stream_sink_ext`
(
    `id`          int(11)      NOT NULL AUTO_INCREMENT COMMENT 'Incremental primary key',
    `sink_type`   varchar(20)  NOT NULL COMMENT 'Sink type, including: HDFS, HIVE, etc.',
    `sink_id`     int(11)      NOT NULL COMMENT 'Sink id',
    `key_name`    varchar(256) NOT NULL COMMENT 'Configuration item name',
    `key_value`   text                  DEFAULT NULL COMMENT 'The value of the configuration item',
    `is_deleted`  int(11)               DEFAULT '0' COMMENT 'Whether to delete, 0: not deleted, > 0: deleted',
    `modify_time` timestamp    NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT 'Modify time',
    PRIMARY KEY (`id`),
    KEY `index_sink_id` (`sink_id`)
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8mb4 COMMENT ='Stream sink extension table';

-- ----------------------------
-- Table structure for stream_sink_field
-- ----------------------------
DROP TABLE IF EXISTS `stream_sink_field`;
CREATE TABLE `stream_sink_field`
(
    `id`                int(11)      NOT NULL AUTO_INCREMENT COMMENT 'Incremental primary key',
    `inlong_group_id`   varchar(256) NOT NULL COMMENT 'Inlong group id',
    `inlong_stream_id`  varchar(256) NOT NULL COMMENT 'Inlong stream id',
    `sink_id`           int(11)      NOT NULL COMMENT 'Sink id',
    `sink_type`         varchar(15)  NOT NULL COMMENT 'Sink type',
    `source_field_name` varchar(50)   DEFAULT NULL COMMENT 'Source field name',
    `source_field_type` varchar(50)   DEFAULT NULL COMMENT 'Source field type',
    `field_name`        varchar(50)  NOT NULL COMMENT 'Field name',
    `field_type`        varchar(50)  NOT NULL COMMENT 'Field type',
    `field_comment`     varchar(2000) DEFAULT NULL COMMENT 'Field description',
    `rank_num`          smallint(6)   DEFAULT '0' COMMENT 'Field order (front-end display field order)',
    `is_deleted`        int(11)       DEFAULT '0' COMMENT 'Whether to delete, 0: not deleted, > 0: deleted',
    PRIMARY KEY (`id`)
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8mb4 COMMENT ='Stream sink field table';

-- ----------------------------
-- Table structure for user
-- ----------------------------
DROP TABLE IF EXISTS `user`;
CREATE TABLE `user`
(
    `id`           int(11)      NOT NULL AUTO_INCREMENT,
    `name`         varchar(256) NOT NULL COMMENT 'account name',
    `password`     varchar(64)  NOT NULL COMMENT 'password md5',
    `account_type` int(11)      NOT NULL DEFAULT '1' COMMENT 'account type, 0-manager 1-normal',
    `due_date`     datetime              DEFAULT NULL COMMENT 'due date for account',
    `create_time`  datetime     NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT 'Create time',
    `update_time`  datetime              DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT 'update time',
    `create_by`    varchar(256) NOT NULL COMMENT 'create by sb.',
    `update_by`    varchar(256)          DEFAULT NULL COMMENT 'update by sb.',
    PRIMARY KEY (`id`),
    UNIQUE KEY `unique_user_name_idx` (`name`)
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8mb4 COMMENT ='User table';

-- create default admin user, username is 'admin', password is 'inlong'
INSERT INTO `user` (name, password, account_type, due_date, create_time, update_time, create_by, update_by)
VALUES ('admin', '628ed559bff5ae36bd2184d4216973cf', 0, '2099-12-31 23:59:59',
        CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, 'inlong_init', 'inlong_init');

-- ----------------------------
-- Table structure for user_role
-- ----------------------------
DROP TABLE IF EXISTS `user_role`;
CREATE TABLE `user_role`
(
    `id`          int(11)      NOT NULL AUTO_INCREMENT,
    `user_name`   varchar(256) NOT NULL COMMENT 'username rtx',
    `role_code`   varchar(256) NOT NULL COMMENT 'role',
    `create_time` datetime     NOT NULL,
    `update_time` datetime     NOT NULL DEFAULT CURRENT_TIMESTAMP,
    `create_by`   varchar(256) NOT NULL,
    `update_by`   varchar(256) NOT NULL,
    `disabled`    tinyint(1)   NOT NULL DEFAULT '0' COMMENT 'Is it disabled?',
    PRIMARY KEY (`id`)
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8mb4 COMMENT ='User Role Table';

-- ----------------------------
-- Table structure for workflow_approver
-- ----------------------------
DROP TABLE IF EXISTS `workflow_approver`;
CREATE TABLE `workflow_approver`
(
    `id`                int(11)       NOT NULL AUTO_INCREMENT,
    `process_name`      varchar(256)  NOT NULL COMMENT 'Process name',
    `task_name`         varchar(256)  NOT NULL COMMENT 'Approval task name',
    `filter_key`        varchar(64)   NOT NULL COMMENT 'Filter condition KEY',
    `filter_value`      varchar(256)           DEFAULT NULL COMMENT 'Filter matching value',
    `filter_value_desc` varchar(256)           DEFAULT NULL COMMENT 'Filter value description',
    `approvers`         varchar(1024) NOT NULL COMMENT 'Approvers, separated by commas',
    `creator`           varchar(64)   NOT NULL COMMENT 'Creator',
    `modifier`          varchar(64)   NOT NULL COMMENT 'Modifier',
    `create_time`       timestamp     NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT 'Create time',
    `modify_time`       timestamp     NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT 'Modify time',
    `is_deleted`        int(11)                DEFAULT '0' COMMENT 'Whether to delete, 0 is not deleted, if greater than 0, delete',
    PRIMARY KEY (`id`),
    KEY `process_name_task_name_index` (`process_name`, `task_name`)
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8mb4 COMMENT ='Workflow approver table';

-- create default approver for new consumption and new inlong group
INSERT INTO `workflow_approver`(`process_name`, `task_name`, `filter_key`, `filter_value`, `approvers`,
                                `creator`, `modifier`, `create_time`, `modify_time`, `is_deleted`)
VALUES ('NEW_CONSUMPTION_PROCESS', 'ut_admin', 'DEFAULT', NULL, 'admin',
        'inlong_init', 'inlong_init', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, 0),
       ('NEW_GROUP_PROCESS', 'ut_admin', 'DEFAULT', NULL, 'admin',
        'inlong_init', 'inlong_init', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, 0);

-- ----------------------------
-- Table structure for workflow_event_log
-- ----------------------------
DROP TABLE IF EXISTS `workflow_event_log`;
CREATE TABLE `workflow_event_log`
(
    `id`                   int(11)      NOT NULL AUTO_INCREMENT,
    `process_id`           int(11)      NOT NULL,
    `process_name`         varchar(256)  DEFAULT NULL COMMENT 'Process name',
    `process_display_name` varchar(256) NOT NULL COMMENT 'Process name',
    `inlong_group_id`      varchar(256)  DEFAULT NULL COMMENT 'Inlong group id',
    `task_id`              int(11)       DEFAULT NULL COMMENT 'Task ID',
    `element_name`         varchar(256) NOT NULL COMMENT 'Name of the component that triggered the event',
    `element_display_name` varchar(256) NOT NULL COMMENT 'Display name of the component that triggered the event',
    `event_type`           varchar(64)  NOT NULL COMMENT 'Event type: process / task ',
    `event`                varchar(64)  NOT NULL COMMENT 'Event name',
    `listener`             varchar(1024) DEFAULT NULL COMMENT 'Event listener name',
    `status`               int(11)      NOT NULL COMMENT 'Status',
    `async`                tinyint(1)   NOT NULL COMMENT 'Asynchronous or not',
    `ip`                   varchar(64)   DEFAULT NULL COMMENT 'IP address executed by listener',
    `start_time`           datetime     NOT NULL COMMENT 'Monitor start execution time',
    `end_time`             datetime      DEFAULT NULL COMMENT 'Listener end time',
    `remark`               text COMMENT 'Execution result remark information',
    `exception`            text COMMENT 'Exception information',
    PRIMARY KEY (`id`)
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8mb4 COMMENT ='Workflow event log table';

-- ----------------------------
-- Table structure for workflow_process
-- ----------------------------
DROP TABLE IF EXISTS `workflow_process`;
CREATE TABLE `workflow_process`
(
    `id`              int(11)      NOT NULL AUTO_INCREMENT,
    `name`            varchar(256) NOT NULL COMMENT 'process name',
    `display_name`    varchar(256) NOT NULL COMMENT 'WorkflowProcess display name',
    `type`            varchar(256)          DEFAULT NULL COMMENT 'WorkflowProcess classification',
    `title`           varchar(256)          DEFAULT NULL COMMENT 'WorkflowProcess title',
    `inlong_group_id` varchar(256)          DEFAULT NULL COMMENT 'Inlong group id: to facilitate related inlong group',
    `applicant`       varchar(256) NOT NULL COMMENT 'applicant',
    `status`          varchar(64)  NOT NULL COMMENT 'status',
    `form_data`       text COMMENT 'form information',
    `start_time`      datetime     NOT NULL COMMENT 'start time',
    `end_time`        datetime              DEFAULT NULL COMMENT 'End event',
    `ext_params`      text COMMENT 'Extended information-json',
    `hidden`          tinyint(1)   NOT NULL DEFAULT '0' COMMENT 'Whether to hidden, 0: not hidden, 1: hidden',
    PRIMARY KEY (`id`)
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8mb4 COMMENT ='Workflow process table';

-- ----------------------------
-- Table structure for workflow_task
-- ----------------------------
DROP TABLE IF EXISTS `workflow_task`;
CREATE TABLE `workflow_task`
(
    `id`                   int(11)       NOT NULL AUTO_INCREMENT,
    `type`                 varchar(64)   NOT NULL COMMENT 'Task type: UserTask / ServiceTask',
    `process_id`           int(11)       NOT NULL COMMENT 'Process ID',
    `process_name`         varchar(256)  NOT NULL COMMENT 'Process name',
    `process_display_name` varchar(256)  NOT NULL COMMENT 'Process name',
    `name`                 varchar(256)  NOT NULL COMMENT 'Task name',
    `display_name`         varchar(256)  NOT NULL COMMENT 'Task display name',
    `applicant`            varchar(64)   DEFAULT NULL COMMENT 'Applicant',
    `approvers`            varchar(1024) NOT NULL COMMENT 'Approvers',
    `status`               varchar(64)   NOT NULL COMMENT 'Status',
    `operator`             varchar(256)  DEFAULT NULL COMMENT 'Actual operator',
    `remark`               varchar(1024) DEFAULT NULL COMMENT 'Remark information',
    `form_data`            text COMMENT 'Form information submitted by the current task',
    `start_time`           datetime      NOT NULL COMMENT 'Start time',
    `end_time`             datetime      DEFAULT NULL COMMENT 'End time',
    `ext_params`           text COMMENT 'Extended information-json',
    PRIMARY KEY (`id`)
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8mb4 COMMENT ='Workflow task table';

-- ----------------------------
-- Table structure for cluster_set
-- ----------------------------
DROP TABLE IF EXISTS `cluster_set`;
CREATE TABLE `cluster_set`
(
    `id`              int(11)      NOT NULL AUTO_INCREMENT COMMENT 'Incremental primary key',
    `set_name`        varchar(128) NOT NULL COMMENT 'ClusterSet name, English, numbers and underscore',
    `cn_name`         varchar(256) COMMENT 'Chinese display name',
    `description`     varchar(256) COMMENT 'ClusterSet Introduction',
    `middleware_type` varchar(10)       DEFAULT 'TUBE' COMMENT 'The middleware type of message queue, high throughput: TUBE, high consistency: PULSAR',
    `in_charges`      varchar(512) COMMENT 'Name of responsible person, separated by commas',
    `followers`       varchar(512) COMMENT 'Name of followers, separated by commas',
    `status`          int(4)            DEFAULT '21' COMMENT 'ClusterSet status',
    `is_deleted`      int(11)           DEFAULT '0' COMMENT 'Whether to delete, 0: not deleted, 1: deleted',
    `creator`         varchar(64)  NOT NULL COMMENT 'Creator name',
    `modifier`        varchar(64)  NULL COMMENT 'Modifier name',
    `create_time`     timestamp    NULL DEFAULT CURRENT_TIMESTAMP COMMENT 'Create time',
    `modify_time`     timestamp    NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT 'Modify time',
    PRIMARY KEY (`id`),
    UNIQUE KEY `unique_cluster_set` (`set_name`)
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8mb4 COMMENT ='ClusterSet table';

-- ----------------------------
-- Table structure for cluster_set_inlongid
-- ----------------------------
DROP TABLE IF EXISTS `cluster_set_inlongid`;
CREATE TABLE `cluster_set_inlongid`
(
    `id`              int(11)      NOT NULL AUTO_INCREMENT COMMENT 'Incremental primary key',
    `set_name`        varchar(256) NOT NULL COMMENT 'ClusterSet name, English, numbers and underscore',
    `inlong_group_id` varchar(256) NOT NULL COMMENT 'Inlong group id, filled in by the user, undeleted ones cannot be repeated',
    PRIMARY KEY (`id`),
    UNIQUE KEY `unique_cluster_set_inlongid` (`set_name`, `inlong_group_id`)
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8mb4 COMMENT ='InlongId table';

-- ----------------------------
-- Table structure for cache_cluster
-- ----------------------------
DROP TABLE IF EXISTS `cache_cluster`;
CREATE TABLE `cache_cluster`
(
    `id`           int(11)      NOT NULL AUTO_INCREMENT COMMENT 'Incremental primary key',
    `cluster_name` varchar(128) NOT NULL COMMENT 'CacheCluster name, English, numbers and underscore',
    `set_name`     varchar(128) NOT NULL COMMENT 'ClusterSet name, English, numbers and underscore',
    `zone`         varchar(128) NOT NULL COMMENT 'Zone, sz/sh/tj',
    PRIMARY KEY (`id`),
    UNIQUE KEY `unique_cache_cluster` (`cluster_name`)
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8mb4 COMMENT ='CacheCluster table';

-- ----------------------------
-- Table structure for cache_cluster_ext
-- ----------------------------
DROP TABLE IF EXISTS `cache_cluster_ext`;
CREATE TABLE `cache_cluster_ext`
(
    `id`           int(11)      NOT NULL AUTO_INCREMENT COMMENT 'Incremental primary key',
    `cluster_name` varchar(128) NOT NULL COMMENT 'CacheCluster name, English, numbers and underscore',
    `key_name`     varchar(256) NOT NULL COMMENT 'Configuration item name',
    `key_value`    text         NULL COMMENT 'The value of the configuration item',
    `is_deleted`   int(11)           DEFAULT '0' COMMENT 'Whether to delete, 0: not deleted, 1: deleted',
    `modify_time`  timestamp    NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT 'Modify time',
    PRIMARY KEY (`id`),
    KEY `index_cache_cluster` (`cluster_name`)
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8mb4 COMMENT ='CacheCluster extension table';

-- ----------------------------
-- Table structure for cache_topic
-- ----------------------------
DROP TABLE IF EXISTS `cache_topic`;
CREATE TABLE `cache_topic`
(
    `id`            int(11)      NOT NULL AUTO_INCREMENT COMMENT 'Incremental primary key',
    `topic_name`    varchar(128) NOT NULL COMMENT 'Topic name, English, numbers and underscore',
    `set_name`      varchar(128) NOT NULL COMMENT 'ClusterSet name, English, numbers and underscore',
    `partition_num` int(11)      NOT NULL COMMENT 'Partition number',
    PRIMARY KEY (`id`),
    UNIQUE KEY `unique_cache_topic` (`topic_name`, `set_name`)
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8mb4 COMMENT ='CacheTopic table';

-- ----------------------------
-- Table structure for proxy_cluster
-- ----------------------------
DROP TABLE IF EXISTS `proxy_cluster`;
CREATE TABLE `proxy_cluster`
(
    `id`           int(11)      NOT NULL AUTO_INCREMENT COMMENT 'Incremental primary key',
    `cluster_name` varchar(128) NOT NULL COMMENT 'ProxyCluster name, English, numbers and underscore',
    `set_name`     varchar(128) NOT NULL COMMENT 'ClusterSet name, English, numbers and underscore',
    `zone`         varchar(128) NOT NULL COMMENT 'Zone, sz/sh/tj',
    PRIMARY KEY (`id`),
    UNIQUE KEY `unique_proxy_cluster` (`cluster_name`, `set_name`)
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8mb4 COMMENT ='ProxyCluster table';

-- ----------------------------
-- Table structure for proxy_cluster_to_cache_cluster
-- ----------------------------
DROP TABLE IF EXISTS `proxy_cluster_to_cache_cluster`;
CREATE TABLE `proxy_cluster_to_cache_cluster`
(
    `id`                 int(11)      NOT NULL AUTO_INCREMENT COMMENT 'Incremental primary key',
    `proxy_cluster_name` varchar(128) NOT NULL COMMENT 'ProxyCluster name, English, numbers and underscore',
    `cache_cluster_name` varchar(128) NOT NULL COMMENT 'CacheCluster name, English, numbers and underscore',
    PRIMARY KEY (`id`),
    UNIQUE KEY `unique_proxy_cluster_to_cache_cluster` (`proxy_cluster_name`, `cache_cluster_name`)
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8mb4 COMMENT ='The relation table of ProxyCluster and CacheCluster';

-- ----------------------------
-- Table structure for flume_source
-- ----------------------------
DROP TABLE IF EXISTS `flume_source`;
CREATE TABLE `flume_source`
(
    `id`            int(11)      NOT NULL AUTO_INCREMENT COMMENT 'Incremental primary key',
    `source_name`   varchar(128) NOT NULL COMMENT 'FlumeSource name, English, numbers and underscore',
    `set_name`      varchar(128) NOT NULL COMMENT 'ClusterSet name, English, numbers and underscore',
    `type`          varchar(128) NOT NULL COMMENT 'FlumeSource classname',
    `channels`      varchar(128) NOT NULL COMMENT 'The channels of FlumeSource, separated by space',
    `selector_type` varchar(128) NOT NULL COMMENT 'FlumeSource channel selector classname',
    PRIMARY KEY (`id`),
    UNIQUE KEY `unique_flume_source` (`source_name`, `set_name`)
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8mb4 COMMENT ='FlumeSource table';

-- ----------------------------
-- Table structure for flume_source_ext
-- ----------------------------
DROP TABLE IF EXISTS `flume_source_ext`;
CREATE TABLE `flume_source_ext`
(
    `id`          int(11)      NOT NULL AUTO_INCREMENT COMMENT 'Incremental primary key',
    `parent_name` varchar(128) NOT NULL COMMENT 'FlumeSource name, English, numbers and underscore',
    `set_name`    varchar(128) NOT NULL COMMENT 'ClusterSet name, English, numbers and underscore',
    `key_name`    varchar(256) NOT NULL COMMENT 'Configuration item name',
    `key_value`   text         NULL COMMENT 'The value of the configuration item',
    `is_deleted`  int(11)           DEFAULT '0' COMMENT 'Whether to delete, 0: not deleted, 1: deleted',
    `modify_time` timestamp    NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT 'Modify time',
    PRIMARY KEY (`id`),
    KEY `index_flume_source_ext` (`parent_name`)
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8mb4 COMMENT ='FlumeSource extension table';

-- ----------------------------
-- Table structure for flume_channel
-- ----------------------------
DROP TABLE IF EXISTS `flume_channel`;
CREATE TABLE `flume_channel`
(
    `id`           int(11)      NOT NULL AUTO_INCREMENT COMMENT 'Incremental primary key',
    `channel_name` varchar(128) NOT NULL COMMENT 'FlumeChannel name, English, numbers and underscore',
    `set_name`     varchar(128) NOT NULL COMMENT 'ClusterSet name, English, numbers and underscore',
    `type`         varchar(128) NOT NULL COMMENT 'FlumeChannel classname',
    PRIMARY KEY (`id`),
    UNIQUE KEY `unique_flume_channel` (`channel_name`, `set_name`)
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8mb4 COMMENT ='FlumeChannel table';

-- ----------------------------
-- Table structure for flume_channel_ext
-- ----------------------------
DROP TABLE IF EXISTS `flume_channel_ext`;
CREATE TABLE `flume_channel_ext`
(
    `id`          int(11)      NOT NULL AUTO_INCREMENT COMMENT 'Incremental primary key',
    `parent_name` varchar(128) NOT NULL COMMENT 'FlumeChannel name, English, numbers and underscore',
    `set_name`    varchar(128) NOT NULL COMMENT 'ClusterSet name, English, numbers and underscore',
    `key_name`    varchar(256) NOT NULL COMMENT 'Configuration item name',
    `key_value`   text         NULL COMMENT 'The value of the configuration item',
    `is_deleted`  int(11)           DEFAULT '0' COMMENT 'Whether to delete, 0: not deleted, 1: deleted',
    `modify_time` timestamp    NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT 'Modify time',
    PRIMARY KEY (`id`),
    KEY `index_flume_channel_ext` (`parent_name`)
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8mb4 COMMENT ='FlumeChannel extension table';

-- ----------------------------
-- Table structure for flume_sink
-- ----------------------------
DROP TABLE IF EXISTS `flume_sink`;
CREATE TABLE `flume_sink`
(
    `id`        int(11)      NOT NULL AUTO_INCREMENT COMMENT 'Incremental primary key',
    `sink_name` varchar(128) NOT NULL COMMENT 'FlumeSink name, English, numbers and underscore',
    `set_name`  varchar(128) NOT NULL COMMENT 'ClusterSet name, English, numbers and underscore',
    `type`      varchar(128) NOT NULL COMMENT 'FlumeSink classname',
    `channel`   varchar(128) NOT NULL COMMENT 'FlumeSink channel',
    PRIMARY KEY (`id`),
    UNIQUE KEY `unique_flume_sink` (`sink_name`, `set_name`)
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8mb4 COMMENT ='FlumeSink table';

-- ----------------------------
-- Table structure for flume_sink_ext
-- ----------------------------
DROP TABLE IF EXISTS `flume_sink_ext`;
CREATE TABLE `flume_sink_ext`
(
    `id`          int(11)      NOT NULL AUTO_INCREMENT COMMENT 'Incremental primary key',
    `parent_name` varchar(128) NOT NULL COMMENT 'FlumeSink name, English, numbers and underscore',
    `set_name`    varchar(128) NOT NULL COMMENT 'ClusterSet name, English, numbers and underscore',
    `key_name`    varchar(256) NOT NULL COMMENT 'Configuration item name',
    `key_value`   text         NULL COMMENT 'The value of the configuration item',
    `is_deleted`  int(11)           DEFAULT '0' COMMENT 'Whether to delete, 0: not deleted, > 0: deleted',
    `modify_time` timestamp    NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT 'Modify time',
    PRIMARY KEY (`id`),
    KEY `index_flume_sink_ext` (`parent_name`)
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8mb4 COMMENT ='FlumeSink extension table';

-- ----------------------------
-- Table structure for db_collector_detail_task
-- ----------------------------
DROP TABLE IF EXISTS `db_collector_detail_task`;
CREATE TABLE `db_collector_detail_task`
(
    `id`            int(11)      NOT NULL AUTO_INCREMENT COMMENT 'Incremental primary key',
    `main_id`       varchar(128) NOT NULL COMMENT 'main task id',
    `type`          int(11)      NOT NULL COMMENT 'task type',
    `time_var`      varchar(64)  NOT NULL COMMENT 'time variable',
    `db_type`       int(11)      NOT NULL COMMENT 'db type',
    `ip`            varchar(64)  NOT NULL COMMENT 'db ip',
    `port`          int(11)      NOT NULL COMMENT 'db port',
    `db_name`       varchar(64)  NULL COMMENT 'db name',
    `user`          varchar(64)  NULL COMMENT 'user name',
    `password`      varchar(64)  NULL COMMENT 'password',
    `sql_statement` varchar(256) NULL COMMENT 'sql statement',
    `offset`        int(11)      NOT NULL COMMENT 'offset for the data source',
    `total_limit`   int(11)      NOT NULL COMMENT 'total limit in a task',
    `once_limit`    int(11)      NOT NULL COMMENT 'limit for one query',
    `time_limit`    int(11)      NOT NULL COMMENT 'time limit for task',
    `retry_times`   int(11)      NOT NULL COMMENT 'max retry times if task failes',
    `group_id`      varchar(64)  NULL COMMENT 'group id',
    `stream_id`     varchar(64)  NULL COMMENT 'stream id',
    `state`         int(11)      NOT NULL COMMENT 'task state',
    `create_time`   timestamp    NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT 'create time',
    `modify_time`   timestamp    NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT 'modify time',
    PRIMARY KEY (`id`)
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8mb4 COMMENT ='db collector detail task table';

-- ----------------------------
-- Table structure for sort_cluster_config
-- ----------------------------
DROP TABLE IF EXISTS `sort_cluster_config`;
CREATE TABLE `sort_cluster_config`
(
    `id`           int(11)      NOT NULL AUTO_INCREMENT COMMENT 'Incremental primary key',
    `cluster_name` varchar(128) NOT NULL COMMENT 'Cluster name',
    `task_name`    varchar(128) NOT NULL COMMENT 'Task name',
    `sink_type`    varchar(128) NOT NULL COMMENT 'Type of sink',
    PRIMARY KEY (`id`),
    KEY `index_sort_cluster_config` (`cluster_name`)
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8mb4 COMMENT ='Sort cluster config table';

-- ----------------------------
-- Table structure for sort_task_id_param
-- ----------------------------
DROP TABLE IF EXISTS `sort_task_id_param`;
CREATE TABLE `sort_task_id_param`
(
    `id`          int(11)       NOT NULL AUTO_INCREMENT COMMENT 'Incremental primary key',
    `task_name`   varchar(128)  NOT NULL COMMENT 'Task name',
    `group_id`    varchar(128)  NOT NULL COMMENT 'Inlong group id',
    `stream_id`   varchar(128)  NULL COMMENT 'Inlong stream id',
    `param_key`   varchar(128)  NOT NULL COMMENT 'Key of param',
    `param_value` varchar(1024) NOT NULL COMMENT 'Value of param',
    PRIMARY KEY (`id`),
    KEY `index_sort_task_id_param` (`task_name`)
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8mb4 COMMENT ='Sort task id params table';

-- ----------------------------
-- Table structure for sort_task_sink_param
-- ----------------------------
DROP TABLE IF EXISTS `sort_task_sink_param`;
CREATE TABLE `sort_task_sink_param`
(
    `id`          int(11)       NOT NULL AUTO_INCREMENT COMMENT 'Incremental primary key',
    `task_name`   varchar(128)  NOT NULL COMMENT 'Task name',
    `sink_type`   varchar(128)  NOT NULL COMMENT 'Type of sink',
    `param_key`   varchar(128)  NOT NULL COMMENT 'Key of param',
    `param_value` varchar(1024) NOT NULL COMMENT 'Value of param',
    PRIMARY KEY (`id`),
    KEY `index_sort_task_sink_params` (`task_name`, `sink_type`)
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8mb4 COMMENT ='Sort task sink params table';

-- ----------------------------
-- Table structure for sort_source_config
-- ----------------------------
DROP TABLE IF EXISTS `sort_source_config`;
CREATE TABLE `sort_source_config`
(
    `id`            int(11)       NOT NULL AUTO_INCREMENT COMMENT 'Incremental primary key',
    `cluster_name`  varchar(128)  NOT NULL COMMENT 'Cluster name',
    `task_name`     varchar(128)  NOT NULL COMMENT 'Task name',
    `zone_name`     varchar(128)  NOT NULL COMMENT 'Cache zone name',
    `topic`         varchar(128)  NOT NULL COMMENT 'Topic',
    `param_key`     varchar(128)  NOT NULL COMMENT 'Key of param',
    `param_value`   varchar(1024) NOT NULL COMMENT 'Value of param',
    PRIMARY KEY (`id`),
    KEY `index_sort_source_config` (`cluster_name`, `task_name`)
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8mb4 COMMENT ='Sort source config table';

-- ----------------------------
-- Table structure for config log report
-- ----------------------------
DROP TABLE IF EXISTS `stream_config_log`;
CREATE TABLE `stream_config_log`
(
    `id`               int(11)      NOT NULL AUTO_INCREMENT COMMENT 'Incremental primary key',
    `ip`               varchar(64)  NOT NULL COMMENT 'client host ip',
    `version`          varchar(128)          DEFAULT NULL COMMENT 'client version',
    `inlong_stream_id` varchar(256)          DEFAULT NULL COMMENT 'Inlong stream ID for consumption',
    `inlong_group_id`  varchar(256) NOT NULL COMMENT 'Inlong group id',
    `component_name`   varchar(64)  NOT NULL COMMENT 'current report info component name',
    `config_name`      varchar(64)           DEFAULT NULL COMMENT 'massage in heartbeat request',
    `log_type`         int(1)                DEFAULT NULL COMMENT '0 normal, 1 error',
    `log_info`         text                  DEFAULT NULL COMMENT 'massage in heartbeat request',
    `report_time`      timestamp    NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT 'report time',
    `modify_time`      timestamp    NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT 'Modify time',
    PRIMARY KEY (`id`),
    KEY `index_config_log_report` (`component_name`, `config_name`, `report_time`)
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8 COMMENT ='stream config log report information table';

-- ----------------------------

-- Table structure for client metric report
-- ----------------------------
DROP TABLE IF EXISTS `stream_metric`;
CREATE TABLE `stream_metric`
(
    `id`               int(11)      NOT NULL AUTO_INCREMENT COMMENT 'Incremental primary key',
    `ip`               varchar(64)  NOT NULL COMMENT 'agent host ip',
    `version`          varchar(128)          DEFAULT NULL COMMENT 'client version',
    `inlong_stream_id` varchar(256)          DEFAULT NULL COMMENT 'Inlong stream ID for consumption',
    `inlong_group_id`  varchar(256) NOT NULL COMMENT 'Inlong group id',
    `component_name`   varchar(64)  NOT NULL COMMENT 'current report info component name',
    `metric_name`      varchar(64)  NOT NULL COMMENT 'current report info component name',
    `metric_info`      text                  DEFAULT NULL COMMENT 'massage in heartbeat request',
    `report_time`      timestamp    NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT 'report time',
    `modify_time`      timestamp    NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT 'Modify time',
    PRIMARY KEY (`id`),
    KEY `index_metric_report` (`component_name`, `metric_name`, `report_time`)
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8 COMMENT ='stream metric report information table';

SET FOREIGN_KEY_CHECKS = 1;
