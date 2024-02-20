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

ALTER TABLE `inlong_stream`
    ADD COLUMN `wrap_type` varchar(256) DEFAULT 'INLONG_MSG_V0' COMMENT 'The message body wrap type, including: RAW, INLONG_MSG_V0, INLONG_MSG_V1, etc';

UPDATE inlong_group SET status = 130 where status = 150;

INSERT INTO `audit_base`(`name`, `type`, `is_sent`, `audit_id`)
VALUES ('audit_sort_mysql_binlog_input', 'MYSQL_BINLOG', 0, '29'),
       ('audit_sort_mysql_binlog_output', 'MYSQL_BINLOG', 1, '30'),
       ('audit_sort_pulsar_input', 'PULSAR', 0, '31'),
       ('audit_sort_pulsar_output', 'PULSAR', 1, '32'),
       ('audit_sort_tube_input', 'TUBEMQ', 0, '33'),
       ('audit_sort_tube_output', 'TUBEMQ', 1, '34'),
       ('audit_agent_sent_failed', 'AGENT', 2, '10004'),
       ('audit_agent_read_realtime', 'AGENT', 3, '30001'),
       ('audit_agent_send_realtime', 'AGENT', 4, '30002'),
       ('audit_agent_add_instance_mem', 'AGENT', 5, '30003'),
       ('audit_agent_del_instance_mem', 'AGENT', 6, '30004'),
       ('audit_agent_add_instance_db', 'AGENT', 7, '30005'),
       ('audit_agent_del_instance_db', 'AGENT', 8, '30006'),
       ('audit_agent_task_mgr_heartbeat', 'AGENT', 9, '30007'),
       ('audit_agent_task_heartbeat', 'AGENT', 10, '30008'),
       ('audit_agent_instance_mgr_heartbeat', 'AGENT', 11, '30009'),
       ('audit_agent_instance_heartbeat', 'AGENT', 12, '30010'),
       ('audit_agent_sent_failed_realtime', 'AGENT', 13, '30011'),
       ('audit_agent_del_instance_mem_unusual', 'AGENT', 14, '30014');

ALTER TABLE `operation_log`
    ADD COLUMN  `inlong_group_id`  varchar(256) DEFAULT NULL COMMENT 'Inlong group id';

ALTER TABLE `operation_log`
    ADD COLUMN  `inlong_stream_id` varchar(256) DEFAULT NULL COMMENT 'Inlong stream id';

ALTER TABLE `operation_log`
    ADD COLUMN `operation_target` varchar(256) DEFAULT NULL COMMENT 'Operation target';

CREATE INDEX operation_log_group_stream_index ON operation_log (`inlong_group_id`, `inlong_stream_id`);

CREATE INDEX `operation_log_request_time_index` ON operation_log (`request_time`);


