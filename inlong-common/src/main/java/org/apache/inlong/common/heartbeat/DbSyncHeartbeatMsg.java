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

package org.apache.inlong.common.heartbeat;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.inlong.common.dbsync.position.LogPosition;

import java.util.List;

/**
 * Heartbeat template for DbSync.
 */
@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class DbSyncHeartbeatMsg {

    /**
     * Agent address
     */
    private String instance;

    /**
     * ServerId of the task, is the ID of data_node table
     */
    private String serverId;

    /**
     * Task IDs being collected by DbSync, is the ID of stream_source table
     */
    private List<String> taskIds;

    /**
     * Agent running status
     */
    private String agentStatus;

    /**
     * Currently collected DB
     */
    private String currentDb;

    /**
     * Currently mysql ip
     */
    private String dbIp;

    /**
     * Currently mysql port
     */
    private String dbPort;

    /**
     * BinLog index currently collected
     */
    private String dbDumpIndex;

    /**
     * If exists, backup mysql ip
     */
    private String backupDbIp;

    /**
     * If exists, backup mysql port
     */
    private String backupDbPort;

    /**
     * Error message
     */
    private String errorMsg;

    /**
     * reporting time
     */
    private Long reportTime;

}
