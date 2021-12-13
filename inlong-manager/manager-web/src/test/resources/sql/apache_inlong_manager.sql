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
-- Table structure for agent_heartbeat_log
-- ----------------------------
DROP TABLE IF EXISTS `agent_heartbeat_log`;
CREATE TABLE `agent_heartbeat_log`
(
    `ip`            varchar(64) NOT NULL COMMENT 'agent host ip',
    `version`       varchar(128)         DEFAULT NULL,
    `heartbeat_msg` text                 DEFAULT NULL COMMENT 'massage in heartbeat request',
    `modify_time`   timestamp   NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT 'Modify time',
    PRIMARY KEY (`ip`)
);

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
);

-- ----------------------------
-- Table structure for business
-- ----------------------------
DROP TABLE IF EXISTS `business`;
CREATE TABLE `business`
(
    `id`                  int(11)      NOT NULL AUTO_INCREMENT COMMENT 'Incremental primary key',
    `inlong_group_id`     varchar(128) NOT NULL COMMENT 'Business group id, filled in by the user, undeleted ones cannot be repeated',
    `name`                varchar(128)          DEFAULT '' COMMENT 'Business name, English, numbers and underscore',
    `cn_name`             varchar(256)          DEFAULT NULL COMMENT 'Chinese display name',
    `description`         varchar(256)          DEFAULT '' COMMENT 'Business Introduction',
    `middleware_type`     varchar(10)           DEFAULT 'TUBE' COMMENT 'The middleware type of message queue, high throughput: TUBE, high consistency: PULSAR',
    `queue_module`        VARCHAR(20)  NULL     DEFAULT 'parallel' COMMENT 'Queue model of Pulsar, parallel: multiple partitions, high throughput, out-of-order messages; serial: single partition, low throughput, and orderly messages',
    `topic_partition_num` INT(4)       NULL     DEFAULT '3' COMMENT 'The number of partitions of Pulsar Topic, 1-20',
    `mq_resource_obj`     varchar(128) NOT NULL COMMENT 'MQ resource object, for Tube, its Topic, for Pulsar, its Namespace',
    `daily_records`       int(11)               DEFAULT '10' COMMENT 'Number of access records per day, unit: 10,000 records per day',
    `daily_storage`       int(11)               DEFAULT '10' COMMENT 'Access size by day, unit: GB per day',
    `peak_records`        int(11)               DEFAULT '1000' COMMENT 'Access peak per second, unit: records per second',
    `max_length`          int(11)               DEFAULT '10240' COMMENT 'The maximum length of a single piece of data, unit: Byte',
    `schema_name`         varchar(128)          DEFAULT NULL COMMENT 'Data type, associated data_schema table',
    `in_charges`          varchar(512) NOT NULL COMMENT 'Name of responsible person, separated by commas',
    `followers`           varchar(512)          DEFAULT NULL COMMENT 'List of names of business followers, separated by commas',
    `status`              int(4)                DEFAULT '21' COMMENT 'Business status',
    `is_deleted`          tinyint(1)            DEFAULT '0' COMMENT 'Whether to delete, 0: not deleted, 1: deleted',
    `creator`             varchar(64)  NOT NULL COMMENT 'Creator name',
    `modifier`            varchar(64)           DEFAULT NULL COMMENT 'Modifier name',
    `create_time`         timestamp    NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT 'Create time',
    `modify_time`         timestamp    NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT 'Modify time',
    `temp_view`           json                  DEFAULT NULL COMMENT 'Temporary view, used to save intermediate data that has not been submitted or approved after modification',
    PRIMARY KEY (`id`),
    UNIQUE KEY `unique_business` (`inlong_group_id`, `is_deleted`, `modify_time`)
);

-- ----------------------------
-- Table structure for business_pulsar
-- ----------------------------
DROP TABLE IF EXISTS `business_pulsar`;
CREATE TABLE `business_pulsar`
(
    `id`                  int(11)      NOT NULL AUTO_INCREMENT COMMENT 'Incremental primary key',
    `inlong_group_id`     varchar(128) NOT NULL COMMENT 'Business group id, filled in by the user, undeleted ones cannot be repeated',
    `ensemble`            int(3)                DEFAULT '3' COMMENT 'The writable nodes number of ledger',
    `write_quorum`        int(3)                DEFAULT '3' COMMENT 'The copies number of ledger',
    `ack_quorum`          int(3)                DEFAULT '2' COMMENT 'The number of requested acks',
    `retention_time`      int(11)               DEFAULT '72' COMMENT 'Message storage time',
    `retention_time_unit` char(20)              DEFAULT 'hours' COMMENT 'The unit of the message storage time',
    `ttl`                 int(11)               DEFAULT '24' COMMENT 'Message time-to-live duration',
    `ttl_unit`            varchar(20)           DEFAULT 'hours' COMMENT 'The unit of time-to-live duration',
    `retention_size`      int(11)               DEFAULT '-1' COMMENT 'Message size',
    `retention_size_unit` varchar(20)           DEFAULT 'MB' COMMENT 'The unit of message size',
    `is_deleted`          tinyint(1)            DEFAULT '0' COMMENT 'Whether to delete, 0: not deleted, 1: deleted',
    `create_time`         timestamp    NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT 'Create time',
    `modify_time`         timestamp    NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT 'Modify time',
    PRIMARY KEY (`id`)
);

-- ----------------------------
-- Table structure for business_ext
-- ----------------------------
DROP TABLE IF EXISTS `business_ext`;
CREATE TABLE `business_ext`
(
    `id`              int(11)      NOT NULL AUTO_INCREMENT COMMENT 'Incremental primary key',
    `inlong_group_id` varchar(128) NOT NULL COMMENT 'Business group id',
    `key_name`        varchar(64)  NOT NULL COMMENT 'Configuration item name',
    `key_value`       varchar(256)          DEFAULT NULL COMMENT 'The value of the configuration item',
    `is_deleted`      tinyint(1)            DEFAULT '0' COMMENT 'Whether to delete, 0: not deleted, 1: deleted',
    `modify_time`     timestamp    NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT 'Modify time',
    PRIMARY KEY (`id`),
    KEY `index_group_id` (`inlong_group_id`)
);

-- ----------------------------
-- Table structure for cluster_info
-- ----------------------------
DROP TABLE IF EXISTS `cluster_info`;
CREATE TABLE `cluster_info`
(
    `id`          int(11)      NOT NULL AUTO_INCREMENT COMMENT 'Incremental primary key',
    `name`        varchar(128) NOT NULL COMMENT 'cluster name',
    `type`        varchar(32)  NOT NULL COMMENT 'Cluster type, including TUBE, PULSAR, etc.',
    `ip`          varchar(64)  NOT NULL COMMENT 'Cluster IP address',
    `port`        int(11)      NOT NULL COMMENT 'Cluster port',
    `in_charges`  varchar(512) NOT NULL COMMENT 'Name of responsible person, separated by commas',
    `url`         varchar(256)          DEFAULT NULL COMMENT 'Cluster URL address',
    `is_backup`   tinyint(1)            DEFAULT '0' COMMENT 'Whether it is a backup cluster, 0: no, 1: yes',
    `ext_props`   text                  DEFAULT NULL COMMENT 'extended properties',
    `status`      int(4)                DEFAULT '1' COMMENT 'cluster status',
    `is_deleted`  tinyint(1)            DEFAULT '0' COMMENT 'Whether to delete, 0: not deleted, 1: deleted',
    `creator`     varchar(64)  NOT NULL COMMENT 'Creator name',
    `modifier`    varchar(64)           DEFAULT NULL COMMENT 'Modifier name',
    `create_time` timestamp    NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT 'Create time',
    `modify_time` timestamp    NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT 'Modify time',
    PRIMARY KEY (`id`)
);

-- ----------------------------
-- Table structure for common_db_server
-- ----------------------------
DROP TABLE IF EXISTS `common_db_server`;
CREATE TABLE `common_db_server`
(
    `id`                  int(11)      NOT NULL AUTO_INCREMENT COMMENT 'Incremental primary key',
    `access_type`         varchar(20)  NOT NULL COMMENT 'Collection type, with Agent, DataProxy client, LoadProxy',
    `connection_name`     varchar(128) NOT NULL COMMENT 'The name of the database connection',
    `db_type`             varchar(128)          DEFAULT 'MySQL' COMMENT 'DB type, such as MySQL, Oracle',
    `db_server_ip`        varchar(64)  NOT NULL COMMENT 'DB Server IP',
    `port`                int(11)      NOT NULL COMMENT 'Port number',
    `db_name`             varchar(128)          DEFAULT NULL COMMENT 'Target database name',
    `username`            varchar(64)  NOT NULL COMMENT 'Username',
    `password`            varchar(64)  NOT NULL COMMENT 'The password corresponding to the above user name',
    `has_select`          tinyint(1)            DEFAULT '0' COMMENT 'Is there DB permission select, 0: No, 1: Yes',
    `has_insert`          tinyint(1)            DEFAULT '0' COMMENT 'Is there DB permission to insert, 0: No, 1: Yes',
    `has_update`          tinyint(1)            DEFAULT '0' COMMENT 'Is there a DB permission update, 0: No, 1: Yes',
    `has_delete`          tinyint(1)            DEFAULT '0' COMMENT 'Is there a DB permission to delete, 0: No, 1: Yes',
    `in_charges`          varchar(512) NOT NULL COMMENT 'DB person in charge, separated by a comma when there are multiple ones',
    `is_region_id`        tinyint(1)            DEFAULT '0' COMMENT 'Whether it contains a region ID, 0: No, 1: Yes',
    `db_description`      varchar(256)          DEFAULT NULL COMMENT 'DB description',
    `backup_db_server_ip` varchar(64)           DEFAULT NULL COMMENT 'Backup DB HOST',
    `backup_db_port`      int(11)               DEFAULT NULL COMMENT 'Backup DB port',
    `status`              int(4)                DEFAULT '0' COMMENT 'status',
    `is_deleted`          tinyint(1)            DEFAULT '0' COMMENT 'Whether to delete, 0: not deleted, 1: deleted',
    `creator`             varchar(64)  NOT NULL COMMENT 'Creator name',
    `modifier`            varchar(64)           DEFAULT NULL COMMENT 'Modifier name',
    `create_time`         timestamp    NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT 'Create time',
    `modify_time`         timestamp    NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT 'Modify time',
    `visible_person`      varchar(1024)         DEFAULT NULL COMMENT 'List of visible persons, separated by commas',
    `visible_group`       varchar(1024)         DEFAULT NULL COMMENT 'List of visible groups, separated by commas',
    PRIMARY KEY (`id`)
);

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
    `is_deleted`     tinyint(1)           DEFAULT '0' COMMENT 'Whether to delete, 0: not deleted, 1: deleted',
    `creator`        varchar(64) NOT NULL COMMENT 'Creator name',
    `modifier`       varchar(64)          DEFAULT NULL COMMENT 'Modifier name',
    `create_time`    timestamp   NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT 'Create time',
    `modify_time`    timestamp   NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT 'Modify time',
    `visible_person` varchar(1024)        DEFAULT NULL COMMENT 'List of visible persons, separated by commas',
    `visible_group`  varchar(1024)        DEFAULT NULL COMMENT 'List of visible groups, separated by commas',
    PRIMARY KEY (`id`)
);

-- ----------------------------
-- Table structure for consumption
-- ----------------------------
DROP TABLE IF EXISTS `consumption`;
CREATE TABLE `consumption`
(
    `id`                  int(11)      NOT NULL AUTO_INCREMENT COMMENT 'Incremental primary key',
    `consumer_group_name` varchar(255) DEFAULT NULL COMMENT 'consumer group name',
    `consumer_group_id`   varchar(255) NOT NULL COMMENT 'Consumer group ID',
    `in_charges`          varchar(512) NOT NULL COMMENT 'Person in charge of consumption',
    `inlong_group_id`     varchar(255) NOT NULL COMMENT 'Business group id',
    `middleware_type`     varchar(10)           DEFAULT 'TUBE' COMMENT 'The middleware type of message queue, high throughput: TUBE, high consistency: PULSAR',
    `topic`               varchar(255) NOT NULL COMMENT 'Consumption topic',
    `filter_enabled`      int(2)                DEFAULT '0' COMMENT 'Whether to filter, default 0, not filter consume',
    `inlong_stream_id`    varchar(1024)         DEFAULT NULL COMMENT 'Data stream ID for consumption, if filter_enable is 1, it cannot empty',
    `status`              int(4)       NOT NULL COMMENT 'Status: draft, pending approval, approval rejected, approval passed',
    `is_deleted`          tinyint(1)            DEFAULT '0' COMMENT 'Whether to delete, 0: not deleted, 1: deleted',
    `creator`             varchar(64)  NOT NULL COMMENT 'creator',
    `modifier`            varchar(64)           DEFAULT NULL COMMENT 'modifier',
    `create_time`         timestamp    NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT 'Create time',
    `modify_time`         timestamp    NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT 'Modify time',
    PRIMARY KEY (`id`)
);

-- ----------------------------
-- Table structure for consumption_pulsar
-- ----------------------------
DROP TABLE IF EXISTS `consumption_pulsar`;
CREATE TABLE `consumption_pulsar`
(
    `id`                  int(11)      NOT NULL AUTO_INCREMENT,
    `consumption_id`      int(11)      DEFAULT NULL COMMENT 'ID of the consumption information to which it belongs, guaranteed to be uniquely associated with consumption information',
    `consumer_group_id`   varchar(255) NOT NULL COMMENT 'Consumer group ID',
    `consumer_group_name` varchar(255) DEFAULT NULL COMMENT 'Consumer group name',
    `inlong_group_id`     varchar(255) NOT NULL COMMENT 'Business group ID',
    `is_rlq`              tinyint(1)   DEFAULT '0' COMMENT 'Whether to configure the retry letter topic, 0: no configuration, 1: configuration',
    `retry_letter_topic`  varchar(255) DEFAULT NULL COMMENT 'The name of the retry queue topic',
    `is_dlq`              tinyint(1)   DEFAULT '0' COMMENT 'Whether to configure dead letter topic, 0: no configuration, 1: means configuration',
    `dead_letter_topic`   varchar(255) DEFAULT NULL COMMENT 'dead letter topic name',
    `is_deleted`          tinyint(1)   DEFAULT '0' COMMENT 'Whether to delete',
    PRIMARY KEY (`id`)
) COMMENT ='Pulsar consumption table';

-- ----------------------------
-- Table structure for data_proxy_cluster
-- ----------------------------
DROP TABLE IF EXISTS `data_proxy_cluster`;
CREATE TABLE `data_proxy_cluster`
(
    `id`          int(11)      NOT NULL AUTO_INCREMENT COMMENT 'Incremental primary key',
    `name`        varchar(128) NOT NULL COMMENT 'cluster name',
    `description` varchar(500)          DEFAULT NULL COMMENT 'cluster description',
    `address`     varchar(128) NOT NULL COMMENT 'cluster address',
    `port`        varchar(256)          DEFAULT '46801' COMMENT 'Access port number, multiple ports are separated by a comma',
    `is_backup`   tinyint(1)            DEFAULT '0' COMMENT 'Whether it is a backup cluster, 0: no, 1: yes',
    `is_inner_ip` tinyint(1)            DEFAULT '0' COMMENT 'Whether it is intranet, 0: no, 1: yes',
    `net_type`    varchar(20)           DEFAULT NULL COMMENT 'Cluster network type, internal, or public',
    `in_charges`  varchar(512)          DEFAULT NULL COMMENT 'Name of responsible person, separated by commas',
    `ext_props`   text                  DEFAULT NULL COMMENT 'Extended properties',
    `status`      int(4)                DEFAULT '1' COMMENT 'Cluster status',
    `is_deleted`  tinyint(1)            DEFAULT '0' COMMENT 'Whether to delete, 0: not deleted, 1: deleted',
    `creator`     varchar(64)  NOT NULL COMMENT 'Creator name',
    `modifier`    varchar(64)           DEFAULT NULL COMMENT 'Modifier name',
    `create_time` timestamp    NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT 'Create time',
    `modify_time` timestamp             DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT 'Modify time',
    PRIMARY KEY (`id`)
);

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
);

-- create default data schema
INSERT INTO `data_schema` (name, agent_type, data_generate_rule, sort_type, time_offset)
values ('m0_day', 'file_agent', 'day', 0, '-0d');

-- ----------------------------
-- Table structure for data_source_cmd_config
-- ----------------------------
DROP TABLE IF EXISTS `data_source_cmd_config`;
CREATE TABLE `data_source_cmd_config`
(
    `id`                  int(11)     NOT NULL AUTO_INCREMENT COMMENT 'cmd id',
    `cmd_type`            int(11)     NOT NULL,
    `task_id`             int(11)     NOT NULL,
    `specified_data_time` varchar(64) NOT NULL,
    `bSend`               tinyint(1)  NOT NULL,
    `modify_time`         timestamp   NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT 'Last update time ',
    `create_time`         timestamp   NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT 'Create time',
    `result_info`         varchar(64)          DEFAULT NULL,
    PRIMARY KEY (`id`),
    KEY `index_1` (`task_id`, `bSend`, `specified_data_time`)
);

-- ----------------------------
-- Table structure for data_stream
-- ----------------------------
DROP TABLE IF EXISTS `data_stream`;
CREATE TABLE `data_stream`
(
    `id`                     int(11)      NOT NULL AUTO_INCREMENT COMMENT 'Incremental primary key',
    `inlong_stream_id`       varchar(128) NOT NULL COMMENT 'Data stream id, non-deleted globally unique',
    `inlong_group_id`        varchar(128) NOT NULL COMMENT 'Owning business group id',
    `name`                   varchar(64)  NOT NULL COMMENT 'The name of the data stream page display, can be Chinese',
    `description`            varchar(256)      DEFAULT '' COMMENT 'Introduction to data stream',
    `mq_resource_obj`        varchar(128)      DEFAULT NULL COMMENT 'MQ resource object, in the data stream, Tube is data_stream_id, Pulsar is Topic',
    `data_source_type`       varchar(32)       DEFAULT 'FILE' COMMENT 'Data source type, including: FILE, DB, Auto-Push (DATA_PROXY_SDK, HTTP)',
    `storage_period`         int(11)           DEFAULT '1' COMMENT 'The storage period of data in MQ, unit: day',
    `data_type`              varchar(20)       DEFAULT 'TEXT' COMMENT 'Data type, there are: TEXT, KEY-VALUE, PB, BON, TEXT and BON should be treated differently',
    `data_encoding`          varchar(8)        DEFAULT 'UTF-8' COMMENT 'Data encoding format, including: UTF-8, GBK',
    `data_separator`         varchar(8)        DEFAULT NULL COMMENT 'The source data field separator, stored as ASCII code',
    `data_escape_char`       varchar(8)        DEFAULT NULL COMMENT 'Source data field escape character, the default is NULL (NULL), stored as 1 character',
    `have_predefined_fields` tinyint(1)        DEFAULT '0' COMMENT '(File, DB access) whether there are predefined fields, 0: none, 1: yes (save to data_stream_field)',
    `daily_records`          int(11)           DEFAULT '10' COMMENT 'Number of access records per day, unit: 10,000 records per day',
    `daily_storage`          int(11)           DEFAULT '10' COMMENT 'Access size by day, unit: GB per day',
    `peak_records`           int(11)           DEFAULT '1000' COMMENT 'Access peak per second, unit: records per second',
    `max_length`             int(11)           DEFAULT '10240' COMMENT 'The maximum length of a single piece of data, unit: Byte',
    `in_charges`             varchar(512)      DEFAULT NULL COMMENT 'Name of responsible person, separated by commas',
    `status`                 int(4)            DEFAULT '0' COMMENT 'Data stream status',
    `previous_status`        int(4)            DEFAULT '0' COMMENT 'Previous status',
    `is_deleted`             tinyint(1)        DEFAULT '0' COMMENT 'Whether to delete, 0: not deleted, 1: deleted',
    `creator`                varchar(64)       DEFAULT NULL COMMENT 'Creator name',
    `modifier`               varchar(64)       DEFAULT NULL COMMENT 'Modifier name',
    `create_time`            timestamp    NULL DEFAULT CURRENT_TIMESTAMP COMMENT 'Create time',
    `modify_time`            timestamp    NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT 'Modify time',
    `temp_view`              text              DEFAULT NULL COMMENT 'Temporary view, used to save intermediate data that has not been submitted or approved after modification',
    PRIMARY KEY (`id`),
    UNIQUE KEY `unique_data_stream` (`inlong_stream_id`, `inlong_group_id`, `is_deleted`, `modify_time`)
);

-- ----------------------------
-- Table structure for data_stream_ext
-- ----------------------------
DROP TABLE IF EXISTS `data_stream_ext`;
CREATE TABLE `data_stream_ext`
(
    `id`               int(11)      NOT NULL AUTO_INCREMENT COMMENT 'Incremental primary key',
    `inlong_group_id`  varchar(128) NOT NULL COMMENT 'Owning business group id',
    `inlong_stream_id` varchar(128) NOT NULL COMMENT 'Owning data stream id',
    `key_name`         varchar(64)  NOT NULL COMMENT 'Configuration item name',
    `key_value`        varchar(256)          DEFAULT NULL COMMENT 'The value of the configuration item',
    `is_deleted`       tinyint(1)            DEFAULT '0' COMMENT 'Whether to delete, 0: not deleted, 1: deleted',
    `modify_time`      timestamp    NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT 'Modify time',
    PRIMARY KEY (`id`),
    KEY `index_stream_id` (`inlong_stream_id`)
);

-- ----------------------------
-- Table structure for data_stream_field
-- ----------------------------
DROP TABLE IF EXISTS `data_stream_field`;
CREATE TABLE `data_stream_field`
(
    `id`                  int(11)      NOT NULL AUTO_INCREMENT COMMENT 'Incremental primary key',
    `inlong_group_id`     varchar(128) NOT NULL COMMENT 'Owning business group id',
    `inlong_stream_id`    varchar(256) NOT NULL COMMENT 'Owning data stream id',
    `is_predefined_field` tinyint(1)   DEFAULT '0' COMMENT 'Whether it is a predefined field, 0: no, 1: yes',
    `field_name`          varchar(20)  NOT NULL COMMENT 'field name',
    `field_value`         varchar(128) DEFAULT NULL COMMENT 'Field value, required if it is a predefined field',
    `pre_expression`      varchar(256) DEFAULT NULL COMMENT 'Pre-defined field value expression',
    `field_type`          varchar(20)  NOT NULL COMMENT 'field type',
    `field_comment`       varchar(50)  DEFAULT NULL COMMENT 'Field description',
    `rank_num`            smallint(6)  DEFAULT '0' COMMENT 'Field order (front-end display field order)',
    `is_deleted`          tinyint(1)   DEFAULT '0' COMMENT 'Whether to delete, 0: not deleted, 1: deleted',
    `is_exist`            tinyint(1)   DEFAULT '0' COMMENT 'Does it exist, 0: does not exist, 1: exists',
    `bon_field_path`      varchar(256) DEFAULT NULL COMMENT 'BON field path',
    `bon_field_type`      varchar(64)  DEFAULT NULL COMMENT 'BON field type',
    `encrypt_level`       varchar(20)  DEFAULT NULL COMMENT 'Encryption level',
    PRIMARY KEY (`id`),
    KEY `index_stream_id` (`inlong_stream_id`)
);

-- ----------------------------
-- Table structure for operation_log
-- ----------------------------
DROP TABLE IF EXISTS `operation_log`;
CREATE TABLE `operation_log`
(
    `id`                  int(11)   NOT NULL AUTO_INCREMENT,
    `authentication_type` varchar(64)        DEFAULT NULL COMMENT 'Authentication type',
    `operation_type`      varchar(255)       DEFAULT NULL COMMENT 'operation type',
    `http_method`         varchar(64)        DEFAULT NULL COMMENT 'Request method',
    `invoke_method`       varchar(255)       DEFAULT NULL COMMENT 'invoke method',
    `operator`            varchar(255)       DEFAULT NULL COMMENT 'operator',
    `proxy`               varchar(255)       DEFAULT NULL COMMENT 'proxy',
    `request_url`         varchar(255)       DEFAULT NULL COMMENT 'Request URL',
    `remote_address`      varchar(255)       DEFAULT NULL COMMENT 'Request IP',
    `cost_time`           bigint(20)         DEFAULT NULL COMMENT 'time-consuming',
    `body`                text COMMENT 'Request body',
    `param`               text COMMENT 'parameter',
    `status`              int(4)             DEFAULT NULL COMMENT 'status',
    `request_time`        timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT 'request time',
    `err_msg`             text COMMENT 'Error message',
    PRIMARY KEY (`id`)
);

-- ----------------------------
-- Table structure for role
-- ----------------------------
DROP TABLE IF EXISTS `role`;
CREATE TABLE `role`
(
    `id`          int(11)      NOT NULL AUTO_INCREMENT,
    `role_code`   varchar(100) NOT NULL COMMENT 'Role code',
    `role_name`   varchar(255) NOT NULL COMMENT 'Role Chinese name',
    `create_time` datetime     NOT NULL,
    `update_time` datetime     NOT NULL DEFAULT CURRENT_TIMESTAMP,
    `create_by`   varchar(255) NOT NULL,
    `update_by`   varchar(255) NOT NULL,
    `disabled`    tinyint(1)   NOT NULL DEFAULT '0' COMMENT 'Is it disabled?',
    PRIMARY KEY (`id`),
    UNIQUE KEY `role_role_code_uindex` (`role_code`),
    UNIQUE KEY `role_role_name_uindex` (`role_name`)
);

-- ----------------------------
-- Table structure for source_db_basic
-- ----------------------------
DROP TABLE IF EXISTS `source_db_basic`;
CREATE TABLE `source_db_basic`
(
    `id`               int(11)      NOT NULL AUTO_INCREMENT COMMENT 'Incremental primary key',
    `inlong_group_id`  varchar(128) NOT NULL COMMENT 'Owning business group id',
    `inlong_stream_id` varchar(256) NOT NULL COMMENT 'Owning data stream id',
    `sync_type`        tinyint(1)            DEFAULT '0' COMMENT 'Data synchronization type, 0: FULL, full amount, 1: INCREMENTAL, incremental',
    `is_deleted`       tinyint(1)            DEFAULT '0' COMMENT 'Whether to delete, 0: not deleted, 1: deleted',
    `creator`          varchar(64)  NOT NULL COMMENT 'Creator name',
    `modifier`         varchar(64)           DEFAULT NULL COMMENT 'Modifier name',
    `create_time`      timestamp    NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT 'Create time',
    `modify_time`      timestamp    NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT 'Modify time',
    `temp_view`        json                  DEFAULT NULL COMMENT 'Temporary view, used to save intermediate data that has not been submitted or approved after modification',
    PRIMARY KEY (`id`)
);

-- ----------------------------
-- Table structure for source_db_detail
-- ----------------------------
DROP TABLE IF EXISTS `source_db_detail`;
CREATE TABLE `source_db_detail`
(
    `id`               int(11)      NOT NULL AUTO_INCREMENT COMMENT 'Incremental primary key',
    `inlong_group_id`  varchar(128) NOT NULL COMMENT 'Owning business group id',
    `inlong_stream_id` varchar(128) NOT NULL COMMENT 'Owning data stream id',
    `access_type`      varchar(20)  NOT NULL COMMENT 'Collection type, with Agent, DataProxy client, LoadProxy',
    `db_name`          varchar(128)          DEFAULT NULL COMMENT 'database name',
    `transfer_ip`      varchar(64)           DEFAULT NULL COMMENT 'Transfer IP',
    `connection_name`  varchar(128)          DEFAULT NULL COMMENT 'The name of the database connection',
    `table_name`       varchar(128)          DEFAULT NULL COMMENT 'Data table name, required for increment',
    `table_fields`     longtext COMMENT 'Data table fields, multiple are separated by half-width commas, required for increment',
    `data_sql`         longtext COMMENT 'SQL statement to collect source data, required for full amount',
    `crontab`          varchar(56)           DEFAULT NULL COMMENT 'Timed scheduling expression, required for full amount',
    `status`           int(4)                DEFAULT '0' COMMENT 'Data source status',
    `previous_status`  int(4)                DEFAULT '0' COMMENT 'Previous status',
    `is_deleted`       tinyint(1)            DEFAULT '0' COMMENT 'Whether to delete, 0: not deleted, 1: deleted',
    `creator`          varchar(64)  NOT NULL COMMENT 'Creator name',
    `modifier`         varchar(64)           DEFAULT NULL COMMENT 'Modifier name',
    `create_time`      timestamp    NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT 'Create time',
    `modify_time`      timestamp    NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT 'Modify time',
    `temp_view`        text                  DEFAULT NULL COMMENT 'Temporary view, used to save un-submitted and unapproved intermediate data after modification',
    PRIMARY KEY (`id`)
);

-- ----------------------------
-- Table structure for source_file_basic
-- ----------------------------
DROP TABLE IF EXISTS `source_file_basic`;
CREATE TABLE `source_file_basic`
(
    `id`                int(11)      NOT NULL AUTO_INCREMENT COMMENT 'ID',
    `inlong_group_id`   varchar(128) NOT NULL COMMENT 'Business group id',
    `inlong_stream_id`  varchar(128) NOT NULL COMMENT 'Data stream id',
    `is_hybrid_source`  tinyint(1)            DEFAULT '0' COMMENT 'Whether to mix data sources',
    `is_table_mapping`  tinyint(1)            DEFAULT '0' COMMENT 'Is there a table name mapping',
    `date_offset`       int(4)                DEFAULT '0' COMMENT 'Time offset\n',
    `date_offset_unit`  varchar(2)            DEFAULT 'H' COMMENT 'Time offset unit',
    `file_rolling_type` varchar(2)            DEFAULT 'H' COMMENT 'File rolling type',
    `upload_max_size`   int(4)                DEFAULT '120' COMMENT 'Upload maximum size',
    `need_compress`     tinyint(1)            DEFAULT '0' COMMENT 'Whether need compress',
    `is_deleted`        tinyint(1)            DEFAULT '0' COMMENT 'Delete switch',
    `creator`           varchar(64)  NOT NULL COMMENT 'Creator',
    `modifier`          varchar(64)           DEFAULT NULL COMMENT 'Modifier',
    `create_time`       timestamp    NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT 'Create time',
    `modify_time`       timestamp    NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT 'Modify time',
    `temp_view`         text                  DEFAULT NULL COMMENT 'temp view',
    PRIMARY KEY (`id`)
);

-- ----------------------------
-- Table structure for source_file_detail
-- ----------------------------
DROP TABLE IF EXISTS `source_file_detail`;
CREATE TABLE `source_file_detail`
(
    `id`               int(11)      NOT NULL AUTO_INCREMENT COMMENT 'Incremental primary key',
    `inlong_group_id`  varchar(128) NOT NULL COMMENT 'Owning business group id',
    `inlong_stream_id` varchar(128) NOT NULL COMMENT 'Owning data stream id',
    `access_type`      varchar(20)           DEFAULT 'Agent' COMMENT 'Collection type, there are Agent, DataProxy client, LoadProxy, the file can only be Agent temporarily',
    `server_name`      varchar(64)           DEFAULT NULL COMMENT 'The name of the data source service. If it is empty, add configuration through the following fields',
    `ip`               varchar(128) NOT NULL COMMENT 'Data source IP address',
    `port`             int(11)      NOT NULL COMMENT 'Data source port number',
    `is_inner_ip`      tinyint(1)            DEFAULT '0' COMMENT 'Whether it is intranet, 0: no, 1: yes',
    `issue_type`       varchar(10)           DEFAULT 'SSH' COMMENT 'Issuing method, there are SSH, TCS',
    `username`         varchar(32)           DEFAULT NULL COMMENT 'User name of the data source IP host',
    `password`         varchar(64)           DEFAULT NULL COMMENT 'The password corresponding to the above user name',
    `file_path`        varchar(256) NOT NULL COMMENT 'File path, supports regular matching',
    `status`           int(4)                DEFAULT '0' COMMENT 'Data source status',
    `previous_status`  int(4)                DEFAULT '0' COMMENT 'Previous status',
    `is_deleted`       tinyint(1)            DEFAULT '0' COMMENT 'Whether to delete, 0: not deleted, 1: deleted',
    `creator`          varchar(64)  NOT NULL COMMENT 'Creator name',
    `modifier`         varchar(64)           DEFAULT NULL COMMENT 'Modifier name',
    `create_time`      timestamp    NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT 'Create time',
    `modify_time`      timestamp    NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT 'Modify time',
    `temp_view`        text                  DEFAULT NULL COMMENT 'Temporary view, used to save un-submitted and unapproved intermediate data after modification',
    PRIMARY KEY (`id`)
);

-- ----------------------------
-- Table structure for storage_ext
-- ----------------------------
DROP TABLE IF EXISTS `storage_ext`;
CREATE TABLE `storage_ext`
(
    `id`           int(11)     NOT NULL AUTO_INCREMENT COMMENT 'Incremental primary key',
    `storage_type` varchar(20) NOT NULL COMMENT 'Storage type, including: HDFS, HIVE, etc.',
    `storage_id`   int(11)     NOT NULL COMMENT 'data storage id',
    `key_name`     varchar(64) NOT NULL COMMENT 'Configuration item name',
    `key_value`    varchar(256)         DEFAULT NULL COMMENT 'The value of the configuration item',
    `is_deleted`   tinyint(1)           DEFAULT '0' COMMENT 'Whether to delete, 0: not deleted, 1: deleted',
    `modify_time`  timestamp   NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT 'Modify time',
    PRIMARY KEY (`id`),
    KEY `index_storage_id` (`storage_id`)
);

-- ----------------------------
-- Table structure for storage_hive
-- ----------------------------
DROP TABLE IF EXISTS `storage_hive`;
CREATE TABLE `storage_hive`
(
    `id`                          int(11)      NOT NULL AUTO_INCREMENT COMMENT 'Incremental primary key',
    `inlong_group_id`             varchar(128) NOT NULL COMMENT 'Owning business group id',
    `inlong_stream_id`            varchar(128) NOT NULL COMMENT 'Owning data stream id',
    `jdbc_url`                    varchar(255)          DEFAULT NULL COMMENT 'Hive JDBC connection URL, such as "jdbc:hive2://127.0.0.1:10000"',
    `username`                    varchar(128)          DEFAULT NULL COMMENT 'Username',
    `password`                    varchar(255)          DEFAULT NULL COMMENT 'User password',
    `db_name`                     varchar(128)          DEFAULT NULL COMMENT 'Target database name',
    `table_name`                  varchar(128)          DEFAULT NULL COMMENT 'Target data table name',
    `hdfs_default_fs`             varchar(255)          DEFAULT NULL COMMENT 'HDFS defaultFS, such as "hdfs://127.0.0.1:9000"',
    `warehouse_dir`               varchar(250)          DEFAULT '/user/hive/warehouse' COMMENT 'Hive table storage path on HDFS, such as "/user/hive/warehouse"',
    `partition_interval`          int(5)                DEFAULT NULL COMMENT 'Partition interval, support: 1(D / H), 10 I, 30 I',
    `partition_unit`              varchar(10)           DEFAULT 'D' COMMENT 'Partition type, support: D-day, H-hour, I-minute',
    `primary_partition`           varchar(255)          DEFAULT 'dt' COMMENT 'primary partition field',
    `secondary_partition`         varchar(256)          DEFAULT NULL COMMENT 'secondary partition field',
    `partition_creation_strategy` varchar(50)           DEFAULT 'COMPLETED' COMMENT 'Partition creation strategy, support: ARRIVED, COMPLETED',
    `file_format`                 varchar(15)           DEFAULT 'TextFile' COMMENT 'The stored table format, TextFile, RCFile, SequenceFile, Avro',
    `data_encoding`               varchar(20)           DEFAULT 'UTF-8' COMMENT 'data encoding type',
    `data_separator`              varchar(10)           DEFAULT NULL COMMENT 'data field separator',
    `storage_period`              int(5)                DEFAULT '10' COMMENT 'Data storage period, unit: day',
    `opt_log`                     varchar(5000)         DEFAULT NULL COMMENT 'Background operation log',
    `status`                      int(4)                DEFAULT '0' COMMENT 'status',
    `previous_status`             int(4)                DEFAULT '0' COMMENT 'Previous status',
    `is_deleted`                  tinyint(1)            DEFAULT '0' COMMENT 'Whether to delete, 0: not deleted, 1: deleted',
    `creator`                     varchar(64)           DEFAULT NULL COMMENT 'creator name',
    `modifier`                    varchar(64)           DEFAULT NULL COMMENT 'modifier name',
    `create_time`                 timestamp    NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT 'create time',
    `modify_time`                 timestamp    NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT 'modify time',
    `temp_view`                   text                  DEFAULT NULL COMMENT 'Temporary view, used to save un-submitted and unapproved intermediate data after modification',
    PRIMARY KEY (`id`)
);

-- ----------------------------
-- Table structure for storage_hive_field
-- ----------------------------
DROP TABLE IF EXISTS `storage_hive_field`;
CREATE TABLE `storage_hive_field`
(
    `id`                int(11)     NOT NULL AUTO_INCREMENT COMMENT 'Incremental primary key',
    `storage_id`        int(11)     NOT NULL COMMENT 'Hive data storage id',
    `source_field_name` varchar(20) NOT NULL COMMENT 'source field name',
    `source_field_type` varchar(20) NOT NULL COMMENT 'source field type',
    `field_name`        varchar(20) NOT NULL COMMENT 'field name',
    `field_type`        varchar(20) NOT NULL COMMENT 'field type',
    `field_comment`     varchar(2000) DEFAULT '' COMMENT 'Field description',
    `is_required`       tinyint(1)    DEFAULT NULL COMMENT 'Is it required, 0: not necessary, 1: required',
    `bon_field_path`    varchar(256)  DEFAULT NULL COMMENT 'BON field path',
    `bon_field_type`    varchar(64)   DEFAULT NULL COMMENT 'BON field type',
    `encrypt_level`     varchar(20)   DEFAULT NULL COMMENT 'Encryption level',
    `is_exist`          tinyint(1)    DEFAULT '0' COMMENT 'Does it exist, 0: does not exist, 1: exists',
    `rank_num`          smallint(6)   DEFAULT '0' COMMENT 'Field order (front-end display field order)',
    `is_deleted`        tinyint(1)    DEFAULT '0' COMMENT 'Whether to delete, 0: not deleted, 1: deleted',
    PRIMARY KEY (`id`),
    KEY `index_storage_id` (`storage_id`)
);

-- ----------------------------
-- Table structure for task
-- ----------------------------
DROP TABLE IF EXISTS `task`;
CREATE TABLE `task`
(
    `id`          bigint(20)   NOT NULL,
    `taskflow_id` bigint(20)   NOT NULL COMMENT 'Owning task flow id',
    `task_def_id` bigint(20)    DEFAULT NULL COMMENT 'task definition id',
    `task_name`   varchar(255) NOT NULL COMMENT 'task name',
    `status`      varchar(255)  DEFAULT NULL COMMENT 'task status',
    `post_param`  varchar(255)  DEFAULT NULL COMMENT 'Task parameters',
    `resultmsg`   varchar(1000) DEFAULT NULL COMMENT 'Execution result log',
    `create_time` datetime     NOT NULL COMMENT 'Create time',
    `create_by`   varchar(255) NOT NULL COMMENT 'creator',
    `update_time` datetime      DEFAULT NULL COMMENT 'last modified time',
    `update_by`   varchar(0)    DEFAULT NULL COMMENT 'last modified person',
    PRIMARY KEY (`id`)
);

-- ----------------------------
-- Table structure for task_def
-- ----------------------------
DROP TABLE IF EXISTS `task_def`;
CREATE TABLE `task_def`
(
    `id`              bigint(20)   NOT NULL,
    `taskflow_def_id` bigint(20)   NOT NULL COMMENT 'Task flow definition id',
    `parent_id`       bigint(20)   DEFAULT NULL COMMENT 'parent task id',
    `implclass`       varchar(255) DEFAULT NULL COMMENT 'task processing flow class',
    `task_name`       varchar(255) DEFAULT NULL COMMENT 'task name',
    `create_time`     datetime     NOT NULL COMMENT 'Create time',
    `create_by`       varchar(255) NOT NULL COMMENT 'creator',
    `update_time`     datetime     DEFAULT NULL COMMENT 'last modified time',
    `update_by`       datetime     DEFAULT NULL COMMENT 'last modified person',
    `delivery_id`     bigint(20)   DEFAULT NULL COMMENT 'Task push method',
    PRIMARY KEY (`id`)
);

-- ----------------------------
-- Table structure for taskflow
-- ----------------------------
DROP TABLE IF EXISTS `taskflow`;
CREATE TABLE `taskflow`
(
    `id`              bigint(20)   NOT NULL AUTO_INCREMENT,
    `taskflow_def_id` bigint(20)   NOT NULL COMMENT 'Taskflow definition id',
    `status`          varchar(255) DEFAULT NULL COMMENT 'status',
    `create_by`       varchar(255) NOT NULL COMMENT 'creator',
    `create_time`     datetime     DEFAULT NULL COMMENT 'Create time',
    `update_time`     datetime     DEFAULT NULL COMMENT 'last modified time',
    `update_by`       varchar(255) DEFAULT NULL COMMENT 'last modified person',
    `event`           varchar(255) DEFAULT NULL COMMENT 'trigger event',
    PRIMARY KEY (`id`)
);

-- ----------------------------
-- Table structure for taskflow_def
-- ----------------------------
DROP TABLE IF EXISTS `taskflow_def`;
CREATE TABLE `taskflow_def`
(
    `id`            bigint(20)   NOT NULL AUTO_INCREMENT,
    `name`          varchar(255) NOT NULL COMMENT 'Workflow definition name',
    `descrip`       varchar(255) DEFAULT NULL COMMENT 'Workflow function description',
    `create_time`   datetime     NOT NULL COMMENT 'Create time',
    `create_by`     varchar(255) NOT NULL COMMENT 'creator',
    `isValid`       int(11)      DEFAULT NULL COMMENT 'logical deletion',
    `trigger_event` varchar(255) DEFAULT NULL COMMENT 'trigger event',
    PRIMARY KEY (`id`)
);

-- ----------------------------
-- Table structure for user
-- ----------------------------
DROP TABLE IF EXISTS `user`;
CREATE TABLE `user`
(
    `id`           int(11)      NOT NULL AUTO_INCREMENT,
    `name`         varchar(255) NOT NULL COMMENT 'account name',
    `password`     varchar(64)  NOT NULL COMMENT 'password md5',
    `account_type` int(11)      NOT NULL DEFAULT '1' COMMENT 'account type, 0-manager 1-normal',
    `due_date`     datetime              DEFAULT NULL COMMENT 'due date for account',
    `create_time`  datetime     NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT 'Create time',
    `update_time`  datetime              DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT 'update time',
    `create_by`    varchar(255) NOT NULL COMMENT 'create by sb.',
    `update_by`    varchar(255)          DEFAULT NULL COMMENT 'update by sb.',
    PRIMARY KEY (`id`),
    UNIQUE KEY `user_name_uindex` (`name`)
);

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
    `user_name`   varchar(255) NOT NULL COMMENT 'username rtx',
    `role_code`   varchar(255) NOT NULL COMMENT 'role',
    `create_time` datetime     NOT NULL,
    `update_time` datetime     NOT NULL DEFAULT CURRENT_TIMESTAMP,
    `create_by`   varchar(255) NOT NULL,
    `update_by`   varchar(255) NOT NULL,
    `disabled`    tinyint(1)   NOT NULL DEFAULT '0' COMMENT 'Is it disabled?',
    PRIMARY KEY (`id`)
);

-- ----------------------------
-- Table structure for wf_approver
-- ----------------------------
DROP TABLE IF EXISTS `wf_approver`;
CREATE TABLE `wf_approver`
(
    `id`                int(11)       NOT NULL AUTO_INCREMENT,
    `process_name`      varchar(255)  NOT NULL COMMENT 'process definition name',
    `task_name`         varchar(255)  NOT NULL COMMENT 'Approval task name',
    `filter_key`        varchar(64)   NOT NULL COMMENT 'filter condition KEY',
    `filter_value`      varchar(255)           DEFAULT NULL COMMENT 'Filter matching value',
    `filter_value_desc` varchar(255)           DEFAULT NULL COMMENT 'Filter value description',
    `approvers`         varchar(1024) NOT NULL COMMENT 'Approvers, separated by commas',
    `creator`           varchar(64)   NOT NULL COMMENT 'creator',
    `modifier`          varchar(64)   NOT NULL COMMENT 'modifier',
    `create_time`       timestamp     NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT 'Create time',
    `modify_time`       timestamp     NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT 'update time',
    `is_deleted`        int(11)                DEFAULT '0' COMMENT 'Whether to delete, 0 is not deleted, if greater than 0, delete',
    PRIMARY KEY (`id`),
    KEY `process_name_task_name_index` (`process_name`, `task_name`)
);

-- create default approver for new consumption and new business
INSERT INTO `wf_approver`(`process_name`, `task_name`, `filter_key`, `filter_value`, `approvers`,
                          `creator`, `modifier`, `create_time`, `modify_time`, `is_deleted`)
VALUES ('NEW_CONSUMPTION_WORKFLOW', 'ut_admin', 'DEFAULT', NULL, 'admin',
        'inlong_init', 'inlong_init', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, 0),
       ('NEW_BUSINESS_WORKFLOW', 'ut_admin', 'DEFAULT', NULL, 'admin',
        'inlong_init', 'inlong_init', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, 0);

-- ----------------------------
-- Table structure for wf_event_log
-- ----------------------------
DROP TABLE IF EXISTS `wf_event_log`;
CREATE TABLE `wf_event_log`
(
    `id`                   int(11)      NOT NULL AUTO_INCREMENT,
    `process_inst_id`      int(11)      NOT NULL,
    `process_name`         varchar(255)  DEFAULT NULL COMMENT 'Process name',
    `process_display_name` varchar(255) NOT NULL COMMENT 'Process name',
    `inlong_group_id`      varchar(128)  DEFAULT NULL COMMENT 'Business group id',
    `task_inst_id`         int(11)       DEFAULT NULL COMMENT 'Task ID',
    `element_name`         varchar(255) NOT NULL COMMENT 'The name of the component that triggered the event',
    `element_display_name` varchar(255) NOT NULL COMMENT 'Chinese name of the component that triggered the event',
    `event_type`           varchar(64)  NOT NULL COMMENT 'Event type: process event/task event',
    `event`                varchar(64)  NOT NULL COMMENT 'Event name',
    `listener`             varchar(1024) DEFAULT NULL COMMENT 'Event listener name',
    `state`                int(11)      NOT NULL COMMENT 'state',
    `async`                tinyint(1)   NOT NULL COMMENT 'Asynchronous or not',
    `ip`                   varchar(64)   DEFAULT NULL COMMENT 'IP address executed by listener',
    `start_time`           datetime     NOT NULL COMMENT 'Monitor start execution time',
    `end_time`             datetime      DEFAULT NULL COMMENT 'Listener end time',
    `remark`               text COMMENT 'Execution result remark information',
    `exception`            text COMMENT 'Exception information',
    PRIMARY KEY (`id`)
);

-- ----------------------------
-- Table structure for wf_process_instance
-- ----------------------------
DROP TABLE IF EXISTS `wf_process_instance`;
CREATE TABLE `wf_process_instance`
(
    `id`              int(11)      NOT NULL AUTO_INCREMENT,
    `name`            varchar(255) NOT NULL COMMENT 'process name',
    `display_name`    varchar(255) NOT NULL COMMENT 'Process display name',
    `type`            varchar(255)          DEFAULT NULL COMMENT 'Process classification',
    `title`           varchar(255)          DEFAULT NULL COMMENT 'Process title',
    `inlong_group_id` varchar(128)          DEFAULT NULL COMMENT 'Business group id: to facilitate related business',
    `applicant`       varchar(255) NOT NULL COMMENT 'applicant',
    `state`           varchar(64)  NOT NULL COMMENT 'state',
    `form_data`       mediumtext COMMENT 'form information',
    `start_time`      datetime     NOT NULL COMMENT 'start time',
    `end_time`        datetime              DEFAULT NULL COMMENT 'End event',
    `ext`             text COMMENT 'Extended information-json',
    `hidden`          tinyint(1)   NOT NULL DEFAULT '0' COMMENT 'Is it hidden',
    PRIMARY KEY (`id`)
);

-- ----------------------------
-- Table structure for wf_task_instance
-- ----------------------------
DROP TABLE IF EXISTS `wf_task_instance`;
CREATE TABLE `wf_task_instance`
(
    `id`                   int(11)       NOT NULL AUTO_INCREMENT,
    `type`                 varchar(64)   NOT NULL COMMENT 'Task type: UserTask user task/ServiceTask system task',
    `process_inst_id`      int(11)       NOT NULL COMMENT 'process ID',
    `process_name`         varchar(255)  NOT NULL COMMENT 'process name',
    `process_display_name` varchar(255)  NOT NULL COMMENT 'process name',
    `name`                 varchar(255)  NOT NULL COMMENT 'task name',
    `display_name`         varchar(255)  NOT NULL COMMENT 'Task display name',
    `applicant`            varchar(64)   DEFAULT NULL COMMENT 'applicant',
    `approvers`            varchar(1024) NOT NULL COMMENT 'approvers',
    `state`                varchar(64)   NOT NULL COMMENT 'state',
    `operator`             varchar(255)  DEFAULT NULL COMMENT 'actual operator',
    `remark`               varchar(1024) DEFAULT NULL COMMENT 'Remark information',
    `form_data`            mediumtext COMMENT 'form information submitted by the current task',
    `start_time`           datetime      NOT NULL COMMENT 'start time',
    `end_time`             datetime      DEFAULT NULL COMMENT 'End time',
    `ext`                  text COMMENT 'Extended information-json',
    PRIMARY KEY (`id`)
);

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
    `middleware_type` varchar(10)           DEFAULT 'TUBE' COMMENT 'The middleware type of message queue, high throughput: TUBE, high consistency: PULSAR',
    `in_charges`      varchar(512) COMMENT 'Name of responsible person, separated by commas',
    `followers`       varchar(512) COMMENT 'List of names of business followers, separated by commas',
    `status`          int(4)                DEFAULT '21' COMMENT 'ClusterSet status',
    `is_deleted`      tinyint(1)            DEFAULT '0' COMMENT 'Whether to delete, 0: not deleted, 1: deleted',
    `creator`         varchar(64)  NOT NULL COMMENT 'Creator name',
    `modifier`        varchar(64)  NULL COMMENT 'Modifier name',
    `create_time`     timestamp    NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT 'Create time',
    `modify_time`     timestamp    NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT 'Modify time',
    PRIMARY KEY (`id`),
    UNIQUE KEY `unique_cluster_set` (`set_name`)
);

-- ----------------------------
-- Table structure for cluster_set_inlongid
-- ----------------------------
DROP TABLE IF EXISTS `cluster_set_inlongid`;
CREATE TABLE `cluster_set_inlongid`
(
    `id`              int(11)      NOT NULL AUTO_INCREMENT COMMENT 'Incremental primary key',
    `set_name`        varchar(128) NOT NULL COMMENT 'ClusterSet name, English, numbers and underscore',
    `inlong_group_id` varchar(128) NOT NULL COMMENT 'Business group id, filled in by the user, undeleted ones cannot be repeated',
    PRIMARY KEY (`id`),
    UNIQUE KEY `unique_cluster_set_inlongid` (`set_name`, `inlong_group_id`)
);

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
);

-- ----------------------------
-- Table structure for cache_cluster_ext
-- ----------------------------
DROP TABLE IF EXISTS `cache_cluster_ext`;
CREATE TABLE `cache_cluster_ext`
(
    `id`           int(11)      NOT NULL AUTO_INCREMENT COMMENT 'Incremental primary key',
    `cluster_name` varchar(128) NOT NULL COMMENT 'CacheCluster name, English, numbers and underscore',
    `key_name`     varchar(64)  NOT NULL COMMENT 'Configuration item name',
    `key_value`    varchar(256) NULL COMMENT 'The value of the configuration item',
    `is_deleted`   tinyint(1)            DEFAULT '0' COMMENT 'Whether to delete, 0: not deleted, 1: deleted',
    `modify_time`  timestamp    NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT 'Modify time',
    PRIMARY KEY (`id`),
    KEY `index_cache_cluster` (`cluster_name`)
);

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
);

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
);

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
);

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
);

-- ----------------------------
-- Table structure for flume_source_ext
-- ----------------------------
DROP TABLE IF EXISTS `flume_source_ext`;
CREATE TABLE `flume_source_ext`
(
    `id`          int(11)      NOT NULL AUTO_INCREMENT COMMENT 'Incremental primary key',
    `parent_name` varchar(128) NOT NULL COMMENT 'FlumeSource name, English, numbers and underscore',
    `set_name`    varchar(128) NOT NULL COMMENT 'ClusterSet name, English, numbers and underscore',
    `key_name`    varchar(64)  NOT NULL COMMENT 'Configuration item name',
    `key_value`   varchar(256) NULL COMMENT 'The value of the configuration item',
    `is_deleted`  tinyint(1)            DEFAULT '0' COMMENT 'Whether to delete, 0: not deleted, 1: deleted',
    `modify_time` timestamp    NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT 'Modify time',
    PRIMARY KEY (`id`),
    KEY `index_flume_source_ext` (`parent_name`)
);

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
);

-- ----------------------------
-- Table structure for flume_channel_ext
-- ----------------------------
DROP TABLE IF EXISTS `flume_channel_ext`;
CREATE TABLE `flume_channel_ext`
(
    `id`          int(11)      NOT NULL AUTO_INCREMENT COMMENT 'Incremental primary key',
    `parent_name` varchar(128) NOT NULL COMMENT 'FlumeChannel name, English, numbers and underscore',
    `set_name`    varchar(128) NOT NULL COMMENT 'ClusterSet name, English, numbers and underscore',
    `key_name`    varchar(64)  NOT NULL COMMENT 'Configuration item name',
    `key_value`   varchar(256) NULL COMMENT 'The value of the configuration item',
    `is_deleted`  tinyint(1)            DEFAULT '0' COMMENT 'Whether to delete, 0: not deleted, 1: deleted',
    `modify_time` timestamp    NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT 'Modify time',
    PRIMARY KEY (`id`),
    KEY `index_flume_channel_ext` (`parent_name`)
);

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
);

-- ----------------------------
-- Table structure for flume_sink_ext
-- ----------------------------
DROP TABLE IF EXISTS `flume_sink_ext`;
CREATE TABLE `flume_sink_ext`
(
    `id`          int(11)      NOT NULL AUTO_INCREMENT COMMENT 'Incremental primary key',
    `parent_name` varchar(128) NOT NULL COMMENT 'FlumeSink name, English, numbers and underscore',
    `set_name`    varchar(128) NOT NULL COMMENT 'ClusterSet name, English, numbers and underscore',
    `key_name`    varchar(64)  NOT NULL COMMENT 'Configuration item name',
    `key_value`   varchar(256) NULL COMMENT 'The value of the configuration item',
    `is_deleted`  tinyint(1)            DEFAULT '0' COMMENT 'Whether to delete, 0: not deleted, 1: deleted',
    `modify_time` timestamp    NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT 'Modify time',
    PRIMARY KEY (`id`),
    KEY `index_flume_sink_ext` (`parent_name`)
);

SET FOREIGN_KEY_CHECKS = 1;
