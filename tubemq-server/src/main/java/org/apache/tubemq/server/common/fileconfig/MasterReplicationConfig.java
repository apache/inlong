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

package org.apache.tubemq.server.common.fileconfig;

import com.sleepycat.je.Durability;
import org.apache.tubemq.corebase.config.constants.MasterReplicaCfgConst;

/* Named `MasterReplicationConfig` to avoid conflict with `com.sleepycat.je.rep.ReplicationConfig` */
public class MasterReplicationConfig {
    private String repGroupName = MasterReplicaCfgConst.DEFAULT_REP_GROUP_NAME;
    private String repNodeName;
    private int repNodePort = MasterReplicaCfgConst.DEFAULT_REP_NODE_PORT;
    private String repHelperHost = MasterReplicaCfgConst.DEFAULT_REP_HELPER_HOST;
    private int metaLocalSyncPolicy = MasterReplicaCfgConst.DEFAULT_META_LOCAL_SYNC_POLICY;
    private int metaReplicaSyncPolicy = MasterReplicaCfgConst.DEFAULT_META_REPLICA_SYNC_POLICY;
    private int repReplicaAckPolicy = MasterReplicaCfgConst.DEFAULT_REP_REPLICA_ACK_POLICY;
    private long repStatusCheckTimeoutMs = MasterReplicaCfgConst.DEFAULT_REP_STATUS_CHECK_TIMEOUT_MS;

    public MasterReplicationConfig() {

    }

    public String getRepGroupName() {
        return repGroupName;
    }

    public void setRepGroupName(String repGroupName) {
        this.repGroupName = repGroupName;
    }

    public String getRepNodeName() {
        return repNodeName;
    }

    public void setRepNodeName(String repNodeName) {
        this.repNodeName = repNodeName;
    }

    public int getRepNodePort() {
        return repNodePort;
    }

    public void setRepNodePort(int repNodePort) {
        this.repNodePort = repNodePort;
    }

    public String getRepHelperHost() {
        return repHelperHost;
    }

    public void setRepHelperHost(String repHelperHost) {
        this.repHelperHost = repHelperHost;
    }

    public Durability.SyncPolicy getMetaLocalSyncPolicy() {
        switch (metaLocalSyncPolicy) {
            case 1:
                return Durability.SyncPolicy.SYNC;
            case 2:
                return Durability.SyncPolicy.NO_SYNC;
            case 3:
                return Durability.SyncPolicy.WRITE_NO_SYNC;
            default:
                return Durability.SyncPolicy.SYNC;
        }
    }

    public void setMetaLocalSyncPolicy(int metaLocalSyncPolicy) {
        this.metaLocalSyncPolicy = metaLocalSyncPolicy;
    }

    public Durability.SyncPolicy getMetaReplicaSyncPolicy() {
        switch (metaReplicaSyncPolicy) {
            case 1:
                return Durability.SyncPolicy.SYNC;
            case 2:
                return Durability.SyncPolicy.NO_SYNC;
            case 3:
                return Durability.SyncPolicy.WRITE_NO_SYNC;
            default:
                return Durability.SyncPolicy.SYNC;
        }
    }

    public void setMetaReplicaSyncPolicy(int metaReplicaSyncPolicy) {
        this.metaReplicaSyncPolicy = metaReplicaSyncPolicy;
    }

    public Durability.ReplicaAckPolicy getRepReplicaAckPolicy() {
        switch (repReplicaAckPolicy) {
            case 1:
                return Durability.ReplicaAckPolicy.SIMPLE_MAJORITY;
            case 2:
                return Durability.ReplicaAckPolicy.ALL;
            case 3:
                return Durability.ReplicaAckPolicy.NONE;
            default:
                return Durability.ReplicaAckPolicy.SIMPLE_MAJORITY;
        }
    }

    public void setRepReplicaAckPolicy(int repReplicaAckPolicy) {
        this.repReplicaAckPolicy = repReplicaAckPolicy;
    }

    public long getRepStatusCheckTimeoutMs() {
        return repStatusCheckTimeoutMs;
    }

    public void setRepStatusCheckTimeoutMs(long repStatusCheckTimeoutMs) {
        this.repStatusCheckTimeoutMs = repStatusCheckTimeoutMs;
    }

    @Override
    public String toString() {
        return new StringBuilder(512)
                .append("\"MasterReplicationConfig\":{\"repGroupName\":").append(repGroupName)
                .append("\",\"repNodeName\":\"").append(repNodeName)
                .append("\",\"repNodePort\":").append(repNodePort)
                .append("\",\"repHelperHost\":\"").append(repHelperHost)
                .append("\",\"metaLocalSyncPolicy\":").append(metaLocalSyncPolicy)
                .append(",\"metaReplicaSyncPolicy\":").append(metaReplicaSyncPolicy)
                .append(",\"repReplicaAckPolicy\":").append(repReplicaAckPolicy)
                .append(",\"repStatusCheckTimeoutMs\":").append(repStatusCheckTimeoutMs)
                .append("}").toString();
    }
}
