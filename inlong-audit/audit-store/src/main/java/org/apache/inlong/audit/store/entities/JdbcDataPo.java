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

package org.apache.inlong.audit.store.entities;

import lombok.Data;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.MessageId;

import java.sql.Timestamp;

@Data
public class JdbcDataPo {

    private String ip;
    private String dockerId;
    private String threadId;
    private Timestamp sdkTs;
    private Long packetId;
    private Timestamp logTs;
    private String inLongGroupId;
    private String inLongStreamId;
    private String auditId;
    private String auditTag;
    private long auditVersion;
    private Long count;
    private Long size;
    private Long delay;
    private Timestamp updateTime;
    private Consumer<byte[]> consumer;
    private MessageId messageId;
}
