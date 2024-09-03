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

-- This is the SQL change file from version 1.11.0 to the current version 1.12.0.
-- When upgrading to version 1.12.0, please execute those SQLs in the DB (such as MySQL) used by the Manager module.

SET NAMES utf8mb4;
SET FOREIGN_KEY_CHECKS = 0;

USE `apache_inlong_manager`;

ALTER TABLE `stream_source` ADD COLUMN  `data_time_zone` varchar(256) DEFAULT NULL COMMENT 'Data time zone';
DROP INDEX `source_template_id_index` ON `stream_source`;
CREATE INDEX source_task_map_id_index ON `stream_source` (`task_map_id`);

ALTER TABLE `stream_source` CHANGE template_id task_map_id int(11) DEFAULT NULL COMMENT 'Id of the task this agent belongs to';

-- ----------------------------
-- Table structure for module_config
-- ----------------------------
CREATE TABLE IF NOT EXISTS `module_config`
(
    `id`          int(11)      NOT NULL AUTO_INCREMENT COMMENT 'Incremental primary key',
    `name`        varchar(256) NOT NULL COMMENT 'Module name',
    `type`        varchar(255) DEFAULT NULL COMMENT 'Module type',
    `package_id`  int(11)      NOT NULL COMMENT 'Package id',
    `ext_params`  text                  COMMENT 'Extended params, will be saved as JSON string',
    `version`     varchar(20)  NOT NULL COMMENT 'Version',
    `is_deleted`  int(11)               DEFAULT '0' COMMENT 'Whether to delete, 0: not deleted, > 0: deleted',
    `creator`     varchar(64)  NOT NULL COMMENT 'Creator name',
    `modifier`    varchar(64)           DEFAULT NULL COMMENT 'Modifier name',
    `create_time` timestamp    NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT 'Create time',
    `modify_time` timestamp    NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT 'Modify time',
    PRIMARY KEY (`id`)
) ENGINE = InnoDB
    DEFAULT CHARSET = utf8mb4 COMMENT = 'Module config table';

-- ----------------------------
-- Table structure for package_config
-- ----------------------------
CREATE TABLE IF NOT EXISTS `package_config` (
    `id`           int(11)      NOT NULL AUTO_INCREMENT COMMENT 'Incremental primary key',
    `md5`          varchar(256) NOT NULL COMMENT 'Md5 of package',
    `file_name`    varchar(256) NOT NULL COMMENT 'File name',
    `type`         varchar(255) DEFAULT NULL COMMENT 'Package type',
    `download_url` varchar(256) NOT NULL COMMENT 'Download url for package',
    `storage_path` varchar(256) NOT NULL COMMENT 'Storage path for package',
    `is_deleted`   int(11)               DEFAULT '0' COMMENT 'Whether to delete, 0: not deleted, > 0: deleted',
    `creator`      varchar(64)  NOT NULL COMMENT 'Creator name',
    `modifier`     varchar(64)           DEFAULT NULL COMMENT 'Modifier name',
    `create_time`  timestamp    NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT 'Create time',
    `modify_time`  timestamp    NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT 'Modify time',
    PRIMARY KEY (`id`)
)  ENGINE = InnoDB
    DEFAULT CHARSET = utf8mb4 COMMENT = 'Package config table';

DROP INDEX `unique_audit_base_type` ON `audit_base`;
ALTER TABLE `audit_base` CHANGE is_sent indicator_type int(4) DEFAULT NULL COMMENT 'Indicator type for audit';
ALTER TABLE `audit_base` ADD UNIQUE KEY unique_audit_base_type (`indicator_type`,`type`);

CREATE TABLE IF NOT EXISTS `sort_config`
(
    `id`                  int(11)       NOT NULL AUTO_INCREMENT COMMENT 'Incremental primary key',
    `sink_id`             int(11)       NOT NULL COMMENT 'Sink id',
    `config_params`       text          NOT NULL COMMENT 'The config params',
    `sink_type`           varchar(128)  NOT NULL COMMENT 'Sink type',
    `inlong_cluster_name` varchar(128)  NOT NULL COMMENT 'Inlong cluster name',
    `inlong_cluster_tag`  varchar(128)  NOT NULL COMMENT 'Inlong cluster tag',
    `sort_task_name`      varchar(128)  NOT NULL COMMENT 'Sort task name',
    `data_node_name`      varchar(128)  NOT NULL COMMENT 'Data node name',
    `creator`             varchar(128)  DEFAULT NULL COMMENT 'Creator',
    `modifier`            varchar(128)  DEFAULT NULL COMMENT 'Modifier name',
    `create_time`         datetime      NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT 'Create time',
    `modify_time`         datetime      NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT 'Modify time',
    `is_deleted`          int(11)       DEFAULT '0' COMMENT 'Whether to delete, 0 is not deleted, if greater than 0, delete',
    `version`             int(11)       NOT NULL DEFAULT '1' COMMENT 'Version number, which will be incremented by 1 after modification',
    PRIMARY KEY (`id`),
    UNIQUE KEY `unique_sort_config_sink_id` (`sink_id`, `is_deleted`)
    ) ENGINE = InnoDB
    DEFAULT CHARSET = utf8mb4 COMMENT = 'sort_config';

CREATE TABLE IF NOT EXISTS `cluster_config`
(
    `id`                  int(11)       NOT NULL AUTO_INCREMENT COMMENT 'Incremental primary key',
    `cluster_tag`         varchar(128)  NOT NULL COMMENT 'Inlong cluster tag',
    `cluster_type`        varchar(128)  NOT NULL COMMENT 'Inlong cluster type',
    `config_params`       text          NOT NULL COMMENT 'The cluster params',
    `creator`             varchar(128)  DEFAULT NULL COMMENT 'Creator',
    `modifier`            varchar(128)  DEFAULT NULL COMMENT 'Modifier name',
    `create_time`         datetime      NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT 'Create time',
    `modify_time`         datetime      NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT 'Modify time',
    `is_deleted`          int(11)       DEFAULT '0' COMMENT 'Whether to delete, 0 is not deleted, if greater than 0, delete',
    `version`             int(11)       NOT NULL DEFAULT '1' COMMENT 'Version number, which will be incremented by 1 after modification',
    PRIMARY KEY (`id`),
    UNIQUE KEY `unique_clustert_config_sink_id` (`cluster_tag`, `is_deleted`)
    ) ENGINE = InnoDB
    DEFAULT CHARSET = utf8mb4 COMMENT = 'cluster_config';
