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

USE `apache_inlong_audit`;

ALTER TABLE audit_data ADD COLUMN audit_tag varchar(100) DEFAULT '' COMMENT 'Audit tag' after `audit_id`;


