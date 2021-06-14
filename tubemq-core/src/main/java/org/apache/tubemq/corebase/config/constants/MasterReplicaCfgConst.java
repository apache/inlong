/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.tubemq.corebase.config.constants;

public interface MasterReplicaCfgConst extends CommonCfgConst {

    /**
     * -------------------------------------------------------------
     * Replication constants.
     * -------------------------------------------------------------
     */

    String REP_GROUP_NAME = "repGroupName";

    String REP_NODE_NAME = "repNodeName";

    String REP_NODE_PORT = "repNodePort";

    String REP_HELPER_HOST = "repHelperHost";

    String META_LOCAL_SYNC_POLICY = "metaLocalSyncPolicy";

    String META_REPLICA_SYNC_POLICY = "metaReplicaSyncPolicy";

    String REP_REPLICA_ACK_POLICY = "repReplicaAckPolicy";

    String REP_STATUS_CHECK_TIMEOUT_MS = "repStatusCheckTimeoutMs";

    /**
     * --------------------------------------------------------------
     * Replication constants.
     * --------------------------------------------------------------
     */

    /**
     * Default value of {@link #REP_GROUP_NAME}.
     */
    String DEFAULT_REP_GROUP_NAME = "tubemqMasterGroup";

    /**
     * Default value of {@link #REP_NODE_NAME}.
     */
    String DEFAULT_REP_NODE_NAME = "";

    /**
     * Default value of {@link #REP_NODE_PORT}.
     */
    int DEFAULT_REP_NODE_PORT = 9001;

    /**
     * Default value of {@link #REP_HELPER_HOST}.
     */
    String DEFAULT_REP_HELPER_HOST = "127.0.0.1:9001";

    /**
     * Default value of {@link #META_LOCAL_SYNC_POLICY}.
     */
    int DEFAULT_META_LOCAL_SYNC_POLICY = 1;

    /**
     * Default value of {@link #META_REPLICA_SYNC_POLICY}.
     */
    int DEFAULT_META_REPLICA_SYNC_POLICY = 3;

    /**
     * Default value of {@link #REP_REPLICA_ACK_POLICY}.
     */
    int DEFAULT_REP_REPLICA_ACK_POLICY = 1;

    /**
     * Default value of {@link #REP_STATUS_CHECK_TIMEOUT_MS}.
     */
    long DEFAULT_REP_STATUS_CHECK_TIMEOUT_MS = 10000L;
}
