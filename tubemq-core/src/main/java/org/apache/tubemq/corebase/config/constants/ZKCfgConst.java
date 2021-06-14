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

public interface ZKCfgConst extends CommonCfgConst {

    /**
     * ----------------------------------------------------------
     * Zookeeper constants.
     * ----------------------------------------------------------
     */

    /**
     * Address list of zookeeper cluster.
     */
    String ZK_SERVER_ADDR = "zkServerAddr";

    /**
     * Root path of tubeMq in zookeeper.
     */
    String ZK_NODE_ROOT = "zkNodeRoot";

    /**
     * Zookeeper session timeout setting, millis.
     */
    String ZK_SESSION_TIMEOUT_MS = "zkSessionTimeoutMs";

    /**
     * Zookeeper connection timeout setting. millis.
     */
    String ZK_CONNECTION_TIMEOUT_MS = "zkConnectionTimeoutMs";

    String ZK_SYNC_TIME_MS = "zkSyncTimeMs";

    String ZK_COMMIT_PERIOD_MS = "zkCommitPeriodMs";

    String ZK_COMMIT_FAIL_RETRIES = "zkCommitFailRetries";

    /**
     * --------------------------------------------------------------
     * Zookeeper default values.
     * --------------------------------------------------------------
     */

    /**
     * Default value of {@link #ZK_SERVER_ADDR}.
     */
    String DEFAULT_ZK_SERVER_ADDR = "localhost:2181";

    /**
     * Default value of {@link #ZK_NODE_ROOT}.
     */
    String DEFAULT_ZK_NODE_ROOT = "/tubemq";

    /**
     * Default value of {@link #ZK_SESSION_TIMEOUT_MS}.
     */
    int DEFAULT_ZK_SESSION_TIMEOUT_MS = 180000;

    /**
     * Default value of {@link #ZK_CONNECTION_TIMEOUT_MS}.
     */
    int DEFAULT_ZK_CONNECTION_TIMEOUT_MS = 600000;

    /**
     * Default value of {@link #ZK_SYNC_TIME_MS}.
     */
    int DEFAULT_ZK_SYNC_TIME_MS = 1000;

    /**
     * Default value of {@link #ZK_COMMIT_FAIL_RETRIES}.
     */
    int DEFAULT_ZK_COMMIT_FAIL_RETRIES = 10;

    /**
     * Default value of {@link #ZK_COMMIT_PERIOD_MS}.
     */
    long DEFAULT_ZK_COMMIT_PERIOD_MS = 5000L;
}
