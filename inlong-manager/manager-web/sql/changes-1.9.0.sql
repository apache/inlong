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

-- This is the SQL change file from version 1.8.0 to the current version 1.9.0.
-- When upgrading to version 1.9.0, please execute those SQLs in the DB (such as MySQL) used by the Manager module.

SET NAMES utf8mb4;
SET FOREIGN_KEY_CHECKS = 0;

USE `apache_inlong_manager`;

-- To improve the ability of resources sharing. More details please see #8590
DROP INDEX cluster_tenant_index ON inlong_cluster;
ALTER TABLE `inlong_cluster` DROP COLUMN `tenant`;

ALTER TABLE `inlong_cluster_tag` DROP COLUMN `tenant`;

CREATE TABLE IF NOT EXISTS `tenant_cluster_tag`
(
    `id`          int(11)      NOT NULL AUTO_INCREMENT COMMENT 'Incremental primary key',
    `tenant`      varchar(256) NOT NULL COMMENT 'Inlong tenant',
    `cluster_tag` varchar(128) NOT NULL COMMENT 'Cluster tag',
    `is_deleted`  int(11)               DEFAULT '0' COMMENT 'Whether to delete, 0: not deleted, > 0: deleted',
    `creator`     varchar(64)  NOT NULL COMMENT 'Creator name',
    `modifier`    varchar(64)           DEFAULT NULL COMMENT 'Modifier name',
    `create_time` timestamp    NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT 'Create time',
    `modify_time` timestamp    NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT 'Modify time',
    `version`     int(11)      NOT NULL DEFAULT '1' COMMENT 'Version number, which will be incremented by 1 after modification',
    PRIMARY KEY (`id`),
    UNIQUE KEY `unique_tenant_inlong_cluster_tag` (`tenant`, `cluster_tag`, `is_deleted`)
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8mb4 COMMENT ='Tenant cluster tag table';



