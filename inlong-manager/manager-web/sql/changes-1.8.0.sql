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

-- This is the SQL change file from version 1.7.0 to the current version 1.8.0.
-- When upgrading to version 1.8.0, please execute those SQLs in the DB (such as MySQL) used by the Manager module.

SET NAMES utf8mb4;
SET FOREIGN_KEY_CHECKS = 0;

USE `apache_inlong_manager`;

ALTER TABLE inlong_group
    CHANGE lightweight inlong_group_mode tinyint(1) DEFAULT 0 NULL COMMENT 'Inlong group mode, Standard mode: 0, DataSync mode: 1';

-- To support multi-tenant management in InLong, see https://github.com/apache/inlong/issues/7914
CREATE TABLE IF NOT EXISTS `inlong_tenant`
(
    `id`           int(11)      NOT NULL AUTO_INCREMENT,
    `name`         varchar(256) NOT NULL COMMENT 'Tenant name, not support modification',
    `description`  varchar(256) DEFAULT '' COMMENT 'Description of tenant',
    `is_deleted`   int(11)      DEFAULT '0' COMMENT 'Whether to delete, 0 is not deleted, if greater than 0, delete',
    `creator`      varchar(256) NOT NULL COMMENT 'Creator name',
    `modifier`     varchar(256) DEFAULT NULL COMMENT 'Modifier name',
    `create_time`  datetime     NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT 'Create time',
    `modify_time`  datetime     NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT 'Modify time',
    `version`      int(11)      NOT NULL DEFAULT '1' COMMENT 'Version number, which will be incremented by 1 after modification',
    PRIMARY KEY (`id`),
    UNIQUE KEY `unique_tenant_key` (`name`, `is_deleted`)
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8 COMMENT ='Inlong tenant table';

INSERT INTO `inlong_tenant`(`name`, `description`, `creator`, `modifier`)
VALUES ('public', 'Default tenant', 'inlong_init', 'inlong_init');

-- To support distinguish inlong user permission and tenant permission control, please see https://github.com/apache/inlong/issues/8098
CREATE TABLE IF NOT EXISTS `inlong_user_role`
(
    `id`          int(11)      NOT NULL AUTO_INCREMENT,
    `username`   varchar(256)  NOT NULL COMMENT 'Username',
    `role_code`   varchar(256) NOT NULL COMMENT 'User role code',
    `disabled`    tinyint(1)   NOT NULL DEFAULT '0' COMMENT 'Whether to disabled, 0: enabled, 1: disabled',
    `is_deleted`  int(11)               DEFAULT '0' COMMENT 'Whether to delete, 0 is not deleted, if greater than 0, delete',
    `creator`     varchar(256) NOT NULL COMMENT 'Creator name',
    `modifier`    varchar(256)          DEFAULT NULL COMMENT 'Modifier name',
    `create_time` datetime     NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT 'Create time',
    `modify_time` datetime     NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT 'Modify time',
    `version`     int(11)      NOT NULL DEFAULT '1' COMMENT 'Version number, which will be incremented by 1 after modification',
    PRIMARY KEY (`id`),
    UNIQUE KEY `unique_inlong_user_role` (`username`, `role_code`, `is_deleted`)
    ) ENGINE = InnoDB
    DEFAULT CHARSET = utf8mb4 COMMENT ='Inlong User Role Table';

INSERT INTO `inlong_user_role` (`username`, `role_code`, `creator`)
VALUES ('admin', 'INLONG_ADMIN', 'inlong_init');

RENAME TABLE user_role TO tenant_user_role;
ALTER TABLE tenant_user_role
    ADD tenant VARCHAR(256) DEFAULT 'public' NOT NULL comment 'User tenant';
ALTER TABLE tenant_user_role
    ADD CONSTRAINT unique_tenant_user
        UNIQUE (username, tenant, is_deleted);
CREATE INDEX index_tenant
    ON tenant_user_role (tenant, is_deleted);

-- To avoid the ambiguity, rename "tenant" in PulsarGroup & PulsarCluster to "pulsarTenant"
UPDATE inlong_group SET ext_params = replace(ext_params, '"tenant"', '"pulsarTenant"');
UPDATE inlong_cluster SET ext_params = replace(ext_params, '"tenant"', '"pulsarTenant"');

ALTER TABLE `inlong_stream` MODIFY COLUMN `name` varchar(256) DEFAULT NULL COMMENT 'The name of the inlong stream page display, can be Chinese';