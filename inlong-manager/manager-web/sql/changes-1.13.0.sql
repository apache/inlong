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

-- This is the SQL change file from version 1.12.0 to the current version 1.13.0.
-- When upgrading to version 1.13.0, please execute those SQLs in the DB (such as MySQL) used by the Manager module.

SET NAMES utf8mb4;
SET FOREIGN_KEY_CHECKS = 0;

USE `apache_inlong_manager`;

ALTER TABLE `inlong_cluster_node` ADD COLUMN  `username` varchar(256) DEFAULT NULL COMMENT 'username for ssh';
ALTER TABLE `inlong_cluster_node` ADD COLUMN  `password` varchar(256) DEFAULT NULL COMMENT 'password for ssh';
ALTER TABLE `inlong_cluster_node` ADD COLUMN  `ssh_port` int(11) DEFAULT NULL COMMENT 'ssh port';

-- ----------------------------
-- Table structure for template
-- ----------------------------
CREATE TABLE IF NOT EXISTS `template`
(
    `id`                  int(11)       NOT NULL AUTO_INCREMENT COMMENT 'Incremental primary key',
    `name`                varchar(128)  NOT NULL COMMENT 'Inlong cluster tag',
    `in_charges`          varchar(512)  NOT NULL COMMENT 'Name of responsible person, separated by commas',
    `visible_range`       text          NOT NULL COMMENT 'Visible range of template',
    `creator`             varchar(128)  DEFAULT NULL COMMENT 'Creator',
    `modifier`            varchar(128)  DEFAULT NULL COMMENT 'Modifier name',
    `create_time`         datetime      NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT 'Create time',
    `modify_time`         datetime      NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT 'Modify time',
    `is_deleted`          int(11)       DEFAULT '0' COMMENT 'Whether to delete, 0 is not deleted, if greater than 0, delete',
    `version`             int(11)       NOT NULL DEFAULT '1' COMMENT 'Version number, which will be incremented by 1 after modification',
    PRIMARY KEY (`id`),
    UNIQUE KEY `unique_template_name` (`name`, `is_deleted`)
) ENGINE = InnoDB
    DEFAULT CHARSET = utf8mb4 COMMENT = 'template';

-- ----------------------------
-- Table structure for template field
-- ----------------------------
CREATE TABLE IF NOT EXISTS `template_field`
(
    `id`                  int(11)      NOT NULL AUTO_INCREMENT COMMENT 'Incremental primary key',
    `template_id`         int(11)      NOT NULL COMMENT 'Owning template id',
    `is_predefined_field` tinyint(1)   DEFAULT '0' COMMENT 'Whether it is a predefined field, 0: no, 1: yes',
    `field_name`          varchar(120) NOT NULL COMMENT 'field name',
    `field_value`         varchar(128) DEFAULT NULL COMMENT 'Field value, required if it is a predefined field',
    `pre_expression`      varchar(256) DEFAULT NULL COMMENT 'Pre-defined field value expression',
    `field_type`          varchar(20)  NOT NULL COMMENT 'field type',
    `field_comment`       varchar(50)  DEFAULT NULL COMMENT 'Field description',
    `is_meta_field`       smallint(3)  DEFAULT '0' COMMENT 'Is this field a meta field? 0: no, 1: yes',
    `meta_field_name`     varchar(120) DEFAULT NULL COMMENT 'Meta field name',
    `field_format`        text         DEFAULT NULL COMMENT 'Field format, including: MICROSECONDS, MILLISECONDS, SECONDS, custom such as yyyy-MM-dd HH:mm:ss, and serialize format of complex type or decimal precision, etc.',
    `rank_num`            smallint(6)  DEFAULT '0' COMMENT 'Field order (front-end display field order)',
    `is_deleted`          int(11)      DEFAULT '0' COMMENT 'Whether to delete, 0: not deleted, > 0: deleted',
    PRIMARY KEY (`id`),
    INDEX `template_field_index` (`template_id`)
) ENGINE = InnoDB
    DEFAULT CHARSET = utf8mb4 COMMENT ='Template field table';

-- ----------------------------
-- Table structure for tenant_template
-- ----------------------------
CREATE TABLE IF NOT EXISTS `tenant_template`
(
    `id`            int(11)      NOT NULL AUTO_INCREMENT COMMENT 'Incremental primary key',
    `tenant`        varchar(256) NOT NULL COMMENT 'Inlong tenant',
    `template_name` varchar(128) NOT NULL COMMENT 'template name',
    `is_deleted`    int(11)               DEFAULT '0' COMMENT 'Whether to delete, 0: not deleted, > 0: deleted',
    `creator`       varchar(64)  NOT NULL COMMENT 'Creator name',
    `modifier`      varchar(64)           DEFAULT NULL COMMENT 'Modifier name',
    `create_time`   timestamp    NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT 'Create time',
    `modify_time`   timestamp    NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT 'Modify time',
    `version`       int(11)      NOT NULL DEFAULT '1' COMMENT 'Version number, which will be incremented by 1 after modification',
    PRIMARY KEY (`id`),
    UNIQUE KEY `unique_tenant_inlong_template` (`tenant`, `template_name`, `is_deleted`)
) ENGINE = InnoDB
    DEFAULT CHARSET = utf8mb4 COMMENT ='Tenant template table';

-- ----------------------------
-- Table structure for schedule_config
-- ----------------------------
CREATE TABLE IF NOT EXISTS `schedule_config`
(
    `id`                     int(11)      NOT NULL AUTO_INCREMENT COMMENT 'Incremental primary key',
    `inlong_group_id`               varchar(256) NOT NULL COMMENT 'Inlong group id, undeleted ones cannot be repeated',
    `schedule_type`          int(4)       NOT NULL DEFAULT '0' COMMENT 'Schedule type, 0 for normal, 1 for crontab',
    `schedule_unit`          varchar(64)  DEFAULT NULL COMMENT 'Schedule unit, Y=year, M=month, W=week, D=day, H=hour, I=minute, O=oneround',
    `schedule_interval`      int(11)      DEFAULT '1' COMMENT 'Schedule interval',
    `start_time`             timestamp    NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT 'Start time for schedule',
    `end_time`               timestamp    NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT 'End time for schedule',
    `delay_time`             int(11)      DEFAULT '0' COMMENT 'Delay time in minutes to schedule',
    `self_depend`            int(11)      DEFAULT NULL COMMENT 'Self depend info',
    `task_parallelism`       int(11)      DEFAULT NULL COMMENT 'Task parallelism',
    `crontab_expression`     varchar(256) DEFAULT NULL COMMENT 'Crontab expression if schedule type is crontab',
    `status`                 int(4)       DEFAULT '100' COMMENT 'Schedule status',
    `previous_status`        int(4)       DEFAULT '100' COMMENT 'Previous schedule status',
    `is_deleted`             int(11)      DEFAULT '0' COMMENT 'Whether to delete, 0: not deleted, > 0: deleted',
    `creator`                varchar(64)  NOT NULL COMMENT 'Creator name',
    `modifier`               varchar(64)  DEFAULT NULL COMMENT 'Modifier name',
    `create_time`            timestamp    NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT 'Create time',
    `modify_time`            timestamp    NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT 'Modify time',
    `version`                int(11)      NOT NULL DEFAULT '1' COMMENT 'Version number, which will be incremented by 1 after modification',
    PRIMARY KEY (`id`),
    UNIQUE KEY `unique_group_schedule_config` (`inlong_group_id`, `is_deleted`)
    ) ENGINE = InnoDB
    DEFAULT CHARSET = utf8mb4 COMMENT = 'schedule_config';
-- ----------------------------

-- ----------------------------
-- Table structure for agent_task_config
-- ----------------------------
CREATE TABLE IF NOT EXISTS `agent_task_config`
(
    `id`                  int(11)       NOT NULL AUTO_INCREMENT COMMENT 'Incremental primary key',
    `config_params`       text          DEFAULT NULL COMMENT 'The agent config params',
    `task_params`         text          DEFAULT NULL COMMENT 'The agent task config params',
    `module_params`       text          DEFAULT NULL COMMENT 'The module config params',
    `agent_ip`            varchar(128)  NOT NULL COMMENT 'agent ip',
    `cluster_name`        varchar(128)  NOT NULL COMMENT 'Inlong cluster name',
    `creator`             varchar(128)  DEFAULT NULL COMMENT 'Creator',
    `modifier`            varchar(128)  DEFAULT NULL COMMENT 'Modifier name',
    `create_time`         datetime      NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT 'Create time',
    `modify_time`         datetime      NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT 'Modify time',
    `is_deleted`          int(11)       DEFAULT '0' COMMENT 'Whether to delete, 0 is not deleted, if greater than 0, delete',
    `version`             int(11)       NOT NULL DEFAULT '1' COMMENT 'Version number, which will be incremented by 1 after modification',
    PRIMARY KEY (`id`),
    UNIQUE KEY `unique_agent_task_config_ip_cluster_name` (`agent_ip`, `cluster_name`, `is_deleted`)
) ENGINE = InnoDB
    DEFAULT CHARSET = utf8mb4 COMMENT = 'agent_task_config';
