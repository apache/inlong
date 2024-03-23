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

-- This is the SQL change file from version 1.9.0 to the current version 1.10.0.
-- When upgrading to version 1.10.0, please execute those SQLs in the DB (such as MySQL) used by the Manager module.

SET NAMES utf8mb4;
SET FOREIGN_KEY_CHECKS = 0;

USE `apache_inlong_manager`;

ALTER TABLE `stream_source` ADD COLUMN  `data_time_zone` varchar(256) DEFAULT NULL COMMENT 'Data time zone';

CREATE TABLE IF NOT EXISTS `sort_config`
(
    `id`                  int(11)       NOT NULL AUTO_INCREMENT COMMENT 'Incremental primary key',
    `sink_id`             int(11)       NOT NULL COMMENT 'Sink id',
    `source_params`       text          NOT NULL COMMENT 'The source params of sort',
    `cluster_params`      text          NOT NULL COMMENT 'The cluster params of sort',
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


