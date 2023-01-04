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

-- This is the SQL change file from version 1.4.0 to the current version 1.5.0.
-- When upgrading to version 1.5.0, please execute those SQLs in the DB (such as MySQL) used by the Manager module.

SET NAMES utf8mb4;
SET FOREIGN_KEY_CHECKS = 0;

USE `apache_inlong_manager`;


ALTER TABLE `inlong_group`
    ADD COLUMN `data_report_type` int(4) DEFAULT '0' COMMENT 'Data report type. 0: report to DataProxy and respond when the DataProxy received data. 1: report to DataProxy and respond after DataProxy sends data. 2: report to MQ and respond when the MQ received data';


ALTER TABLE `inlong_cluster_node`
    ADD COLUMN `node_load` int(11) DEFAULT '-1' COMMENT 'Current load value of the node';


ALTER TABLE `stream_source`
    ADD COLUMN `inlong_cluster_node_label` varchar(512) DEFAULT NULL COMMENT 'Cluster node label';

CREATE TABLE IF NOT EXISTS `inlong_label`
(
    `id`           int(11)      NOT NULL AUTO_INCREMENT COMMENT 'Incremental primary key',
    `label_name`   varchar(128) NOT NULL COMMENT 'label name',
    `ext_params`   mediumtext            DEFAULT NULL COMMENT 'Extended params, will be saved as JSON string',
    `description`  varchar(256)          DEFAULT '' COMMENT 'Description of cluster tag',
    `in_charges`   varchar(512) NOT NULL COMMENT 'Name of responsible person, separated by commas',
    `is_deleted`   int(11)               DEFAULT '0' COMMENT 'Whether to delete, 0: not deleted, > 0: deleted',
    `creator`      varchar(64)  NOT NULL COMMENT 'Creator name',
    `modifier`     varchar(64)           DEFAULT NULL COMMENT 'Modifier name',
    `create_time`  timestamp    NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT 'Create time',
    `modify_time`  timestamp    NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT 'Modify time',
    `version`      int(11)      NOT NULL DEFAULT '1' COMMENT 'Version number, which will be incremented by 1 after modification',
    PRIMARY KEY (`id`),
    UNIQUE KEY (`label_name`)
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8mb4 COMMENT ='Inlong collect task label table';

CREATE TABLE IF NOT EXISTS `inlong_label_node_relation`
(
    `id`            int(11)      NOT NULL AUTO_INCREMENT COMMENT 'Incremental primary key',
    `label_id`      int(11)      NOT NULL COMMENT 'Id of the label id',
    `node_id`       int(11)      NOT NULL COMMENT 'Id of the cluster node id.',
    PRIMARY KEY (`id`),
    UNIQUE KEY `unique_label_kv` (`label_id`, `node_id`)
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8mb4 COMMENT ='Inlong collect task label table';