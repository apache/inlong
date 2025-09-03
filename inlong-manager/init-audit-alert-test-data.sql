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

-- Audit Alert Rule database initialization and test script
-- Ensure the database contains audit alert rule tables and test data

USE apache_inlong_manager;

-- 1. Check if table exists, create if not
CREATE TABLE IF NOT EXISTS `audit_alert_rule` (
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
  `creator` varchar(256) DEFAULT NULL COMMENT 'Creator',
  `modifier` varchar(256) DEFAULT NULL COMMENT 'Modifier',
  `create_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT 'Create Time',
  `update_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT 'Update Time',
  PRIMARY KEY (`id`),
  KEY `idx_group_stream` (`inlong_group_id`, `inlong_stream_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='Audit Alert Rule Table';

-- 2. Insert some test data (if not exists)
INSERT IGNORE INTO `audit_alert_rule` 
(`id`, `inlong_group_id`, `inlong_stream_id`, `audit_id`, `alert_name`, `condition`, `level`, `notify_type`, `receivers`, `enabled`, `creator`, `modifier`)
VALUES 
(1, 'demo_group', 'demo_stream_001', '3', 'Data Send Failure Alert', 'count < 1000', 'ERROR', 'EMAIL', 'admin@example.com', 1, 'system', 'system'),
(2, 'demo_group', 'demo_stream_002', '4', 'Data Receive Delay Alert', 'delay > 60000', 'WARN', 'SMS', 'ops@example.com', 1, 'system', 'system'),
(3, 'prod_group', 'prod_stream_001', '5', 'Production Data Loss Alert', 'count == 0', 'CRITICAL', 'EMAIL', 'admin@example.com,ops@example.com', 1, 'system', 'system'),
(4, 'test_group', 'test_stream_001', '6', 'Test Environment Data Quality Alert', 'count > 100000', 'INFO', 'HTTP', 'http://webhook.example.com/alert', 0, 'system', 'system');

-- 3. Verify data insertion
SELECT 
    id,
    inlong_group_id,
    inlong_stream_id,
    audit_id,
    alert_name,
    `condition`,
    `level`,
    notify_type,
    receivers,
    enabled,
    creator,
    create_time
FROM audit_alert_rule
ORDER BY create_time DESC;

-- 4. Statistical information
SELECT 
    COUNT(*) as total_rules,
    SUM(CASE WHEN enabled = 1 THEN 1 ELSE 0 END) as enabled_rules,
    SUM(CASE WHEN enabled = 0 THEN 1 ELSE 0 END) as disabled_rules
FROM audit_alert_rule;

-- 5. Group statistics by Group
SELECT 
    inlong_group_id,
    COUNT(*) as rule_count,
    GROUP_CONCAT(alert_name) as alert_names
FROM audit_alert_rule
GROUP BY inlong_group_id
ORDER BY rule_count DESC;