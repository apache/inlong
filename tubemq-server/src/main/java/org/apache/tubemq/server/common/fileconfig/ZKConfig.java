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

import org.apache.tubemq.corebase.TServerConstants;
import org.apache.tubemq.corebase.config.constants.ZKCfgConst;

public class ZKConfig {

    private String zkServerAddr = ZKCfgConst.DEFAULT_ZK_SERVER_ADDR;
    private String zkNodeRoot = ZKCfgConst.DEFAULT_ZK_NODE_ROOT;
    private int zkSessionTimeoutMs = ZKCfgConst.DEFAULT_ZK_SESSION_TIMEOUT_MS;
    private int zkConnectionTimeoutMs = ZKCfgConst.DEFAULT_ZK_CONNECTION_TIMEOUT_MS;
    private int zkSyncTimeMs = ZKCfgConst.DEFAULT_ZK_SYNC_TIME_MS;
    private long zkCommitPeriodMs = ZKCfgConst.DEFAULT_ZK_COMMIT_PERIOD_MS;
    private int zkCommitFailRetries = TServerConstants.CFG_ZK_COMMIT_DEFAULT_RETRIES;
    public ZKConfig() {

    }

    public String getZkServerAddr() {
        return zkServerAddr;
    }

    public void setZkServerAddr(String zkServerAddr) {
        this.zkServerAddr = zkServerAddr;
    }

    public String getZkNodeRoot() {
        return zkNodeRoot;
    }

    public void setZkNodeRoot(String zkNodeRoot) {
        this.zkNodeRoot = zkNodeRoot;
    }

    public int getZkSessionTimeoutMs() {
        return zkSessionTimeoutMs;
    }

    public void setZkSessionTimeoutMs(int zkSessionTimeoutMs) {
        this.zkSessionTimeoutMs = zkSessionTimeoutMs;
    }

    public int getZkConnectionTimeoutMs() {
        return zkConnectionTimeoutMs;
    }

    public void setZkConnectionTimeoutMs(int zkConnectionTimeoutMs) {
        this.zkConnectionTimeoutMs = zkConnectionTimeoutMs;
    }

    public int getZkSyncTimeMs() {
        return zkSyncTimeMs;
    }

    public void setZkSyncTimeMs(int zkSyncTimeMs) {
        this.zkSyncTimeMs = zkSyncTimeMs;
    }

    public int getZkCommitFailRetries() {
        return zkCommitFailRetries;
    }

    public void setZkCommitFailRetries(int zkCommitFailRetries) {
        this.zkCommitFailRetries = zkCommitFailRetries;
    }

    public long getZkCommitPeriodMs() {
        return zkCommitPeriodMs;
    }

    public void setZkCommitPeriodMs(long zkCommitPeriodMs) {
        this.zkCommitPeriodMs = zkCommitPeriodMs;
    }

    @Override
    public String toString() {
        return new StringBuilder(512)
                .append("\"ZKConfig\":{\"zkServerAddr\":\"").append(zkServerAddr)
                .append("\",\"zkNodeRoot\":\"").append(zkNodeRoot)
                .append("\",\"zkSessionTimeoutMs\":").append(zkSessionTimeoutMs)
                .append(",\"zkConnectionTimeoutMs\":").append(zkConnectionTimeoutMs)
                .append(",\"zkSyncTimeMs\":").append(zkSyncTimeMs)
                .append(",\"zkCommitPeriodMs\":").append(zkCommitPeriodMs)
                .append(",\"zkCommitFailRetries\":").append(zkCommitFailRetries)
                .append("}").toString();
    }
}
