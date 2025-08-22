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

-- This is the SQL change file from version 2.1.0 to the current version 2.2.0.
-- When upgrading to version 2.2.0, please execute those SQLs in the DB (such as MySQL) used by the Manager module.

SET NAMES utf8mb4;
SET FOREIGN_KEY_CHECKS = 0;

USE `apache_inlong_manager`;
CREATE TABLE `audit_alert_rule` (
  `id` int(11) NOT NULL AUTO_INCREMENT COMMENT 'Primary Key ID',
  `inlong_group_id` varchar(256) NOT NULL COMMENT 'InLong Group ID',
  `inlong_stream_id` varchar(256) DEFAULT NULL COMMENT 'InLong Stream ID',
  `audit_id` varchar(256) NOT NULL COMMENT 'Audit item ID, e.g., 3, 4, 5, 6',
  `alert_name` varchar(256) NOT NULL COMMENT 'Alert Name',
  `condition` text NOT NULL COMMENT 'Alert condition (JSON format), e.g., {"type": "data_loss", "operator": ">", "value": 5}',
  `level` varchar(50) NOT NULL COMMENT 'Alert level, e.g., HIGH, MEDIUM, LOW',
  `notify_type` varchar(50) NOT NULL COMMENT 'Notification type, e.g., EMAIL, SMS',
  `receivers` text NOT NULL COMMENT 'List of receivers, comma-separated',
  `enabled` tinyint(1) DEFAULT '1' COMMENT 'Whether to enable, 1-enabled, 0-disabled',
  `create_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT 'Create Time',
  `update_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT 'Update Time',
  PRIMARY KEY (`id`),
  KEY `idx_group_stream` (`inlong_group_id`, `inlong_stream_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='Audit Alert Rule Table';