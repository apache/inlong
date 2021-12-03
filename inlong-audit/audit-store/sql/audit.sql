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

CREATE TABLE `audit_data` (
  `id` int(32) not null primary key auto_increment,
  `ip` varchar(32) NOT NULL DEFAULT '',
  `docker_id` varchar(100) NOT NULL DEFAULT '',
  `thread_id` varchar(50) NOT NULL DEFAULT '',
  `sdk_ts` TIMESTAMP NOT NULL DEFAULT '0000-00-00 00:00:00',
  `packet_id` BIGINT NOT NULL DEFAULT '0' COMMENT '',
  `log_ts` TIMESTAMP NOT NULL DEFAULT '0000-00-00 00:00:00' COMMENT '',
  `inlong_group_id` varchar(100) NOT NULL DEFAULT '' COMMENT '',
  `inlong_stream_id` varchar(100) NOT NULL DEFAULT '' COMMENT '',
  `audit_id` varchar(100) NOT NULL DEFAULT '' COMMENT '',
  `count` BIGINT NOT NULL DEFAULT '0' COMMENT '',
  `size` BIGINT NOT NULL DEFAULT '0' COMMENT '',
  `delay` BIGINT NOT NULL DEFAULT '0' COMMENT '',
  `updateTime` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  INDEX ip_packet(`ip`,`inlong_group_id`,`inlong_stream_id`,`log_ts`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8