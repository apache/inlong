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

import org.apache.tubemq.corebase.TBaseConstants;
import org.apache.tubemq.corerpc.RpcConstants;

public interface MasterCfgConst extends CommonCfgConst {
    /**
     * -----------------------------------------------------------------
     * Master constants.
     * -----------------------------------------------------------------
     */
    String MASTER_PORT = PORT;

    String MASTER_HOST_NAME = HOST_NAME;

    String MASTER_WEB_PORT = WEB_PORT;

    String WEB_RESOURCE_PATH = "webResourcePath";

    String META_DATA_PATH = "metaDataPath";

    String CONSUMER_BALANCE_PERIOD_MS = "consumerBalancePeriodMs";

    String FIRST_BALANCE_DELAY_AFTER_START_MS = "firstBalanceDelayAfterStartMs";

    String CONSUMER_HEARTBEAT_TIMEOUT_MS = "consumerHeartbeatTimeoutMs";

    String PRODUCER_HEARTBEAT_TIMEOUT_MS = "producerHeartbeatTimeoutMs";

    String BROKER_HEARTBEAT_TIMEOUT_MS = "brokerHeartbeatTimeoutMs";

    String MASTER_SOCKET_SEND_BUFFER = SOCKET_SEND_BUFFER;

    String MASTER_SOCKET_RECV_BUFFER = SOCKET_RECV_BUFFER;

    String MASTER_RPC_READ_TIMEOUT_MS = RPC_READ_TIMEOUT_MS;

    String MASTER_NETTY_WRITE_BUFFER_HIGH_WATERMARK = NETTY_WRITE_BUFFER_HIGH_WATERMARK;

    String MASTER_NETTY_WRITE_BUFFER_LOW_WATERMARK = NETTY_WRITE_BUFFER_LOW_WATERMARK;

    String ONLINE_ONLY_READ_TO_RW_PERIOD_MS = "onlineOnlyReadToRWPeriodMs";

    String STEP_CHG_WAIT_PERIOD_MS = "stepChgWaitPeriodMs";

    String OFFLINE_ONLY_READ_TO_RW_PERIOD_MS = "offlineOnlyReadToRWPeriodMs";

    String CONF_MOD_AUTH_TOKEN = "confModAuthToken";

    String MAX_GROUP_BROKER_CONSUME_RATE = "maxGroupBrokerConsumeRate";

    String MAX_GROUP_REBALANCE_WAIT_PERIOD = "maxGroupRebalanceWaitPeriod";

    String START_OFFSET_RESET_CHECK = "startOffsetResetCheck";

    String MASTER_ROW_LOCK_WAIT_DUR_MS = ROW_LOCK_WAIT_DUR_MS;

    String MAX_AUTO_FORBIDDEN_CNT = "maxAutoForbiddenCnt";

    String VISIT_TOKEN_VALID_PERIOD_MS = "visitTokenValidPeriodMs";

    String MASTER_AUTH_VALID_TIME_STAMP_PERIOD_MS = AUTH_VALID_TIMESTAMP_PERIOD_MS;

    String START_VISIT_TOKEN_CHECK = "startVisitTokenCheck";

    String START_PRODUCE_AUTHENTICATE = "startProduceAuthenticate";

    String START_PRODUCE_AUTHORIZE = "startProduceAuthorize";

    String USE_WEB_PROXY = "useWebProxy";

    String START_CONSUME_AUTHENTICATE = "startConsumeAuthenticate";

    String START_CONSUME_AUTHORIZE = "startConsumeAuthorize";

    String NEED_BROKER_VISIT_AUTH = "needBrokerVisitAuth";

    String MASTER_VISIT_NAME = VISIT_NAME;

    String MASTER_VISIT_PASSWORD = VISIT_PASSWORD;

    String REBALANCE_PARALLEL = "rebalanceParallel";

    /**
     * -------------------------------------------------------------------
     * Bdb constants.
     * -------------------------------------------------------------------
     */
    @Deprecated
    String BDB_REP_GROUP_NAME = "bdbRepGroupName";

    @Deprecated
    String BDB_NODE_NAME = "bdbNodeName";

    @Deprecated
    String BDB_NODE_PORT = "bdbNodePort";

    @Deprecated
    String BDB_ENV_HOME = "bdbEnvHome";

    @Deprecated
    String BDB_HELPER_HOST = "bdbHelperHost";

    @Deprecated
    String BDB_LOCAL_SYNC = "bdbLocalSync";

    @Deprecated
    String BDB_REPLICA_SYNC = "bdbReplicaSync";

    @Deprecated
    String BDB_REPLICA_ACK = "bdbReplicaAck";

    @Deprecated
    String BDB_STATUS_CHECK_TIMEOUT_MS = "bdbStatusCheckTimeoutMs";


    /**
     * --------------------------------------------------------------
     * Master default values.
     * --------------------------------------------------------------
     */

    // String DEFAULT_MASTER_PORT = PORT;
    // String DEFAULT_MASTER_HOST_NAME = HOST_NAME;

    /**
     * Default value of {@link #MASTER_WEB_PORT}.
     */
    int DEFAULT_MASTER_WEB_PORT = 8080;

    /**
     * Default value of {@link #WEB_RESOURCE_PATH}.
     */
    String DEFAULT_WEB_RESOURCE_PATH = "../resources";

    /**
     * Default value of {@link #META_DATA_PATH}.
     */
    String DEFAULT_META_DATA_PATH = "var/meta_data";

    /**
     * Default value of {@link #CONSUMER_BALANCE_PERIOD_MS}.
     */
    int DEFAULT_CONSUMER_BALANCE_PERIOD_MS = 60 * 1000;

    /**
     * Default value of {@link #FIRST_BALANCE_DELAY_AFTER_START_MS}.
     */
    int DEFAULT_FIRST_BALANCE_DELAY_AFTER_START_MS = 30 * 1000;

    /**
     * Default value of {@link #CONSUMER_HEARTBEAT_TIMEOUT_MS}.
     */
    int DEFAULT_CONSUMER_HEARTBEAT_TIMEOUT_MS = 30 * 1000;

    /**
     * Default value of {@link #PRODUCER_HEARTBEAT_TIMEOUT_MS}.
     */
    int DEFAULT_PRODUCER_HEARTBEAT_TIMEOUT_MS = 30 * 1000;

    /**
     * Default value of {@link #BROKER_HEARTBEAT_TIMEOUT_MS}.
     */
    int DEFAULT_BROKER_HEARTBEAT_TIMEOUT_MS = 30 * 1000;

    /**
     * Default value of {@link #MASTER_SOCKET_SEND_BUFFER}.
     */
    long DEFAULT_MASTER_SOCKET_SEND_BUFFER = -1L;

    /**
     * Default value of {@link #MASTER_SOCKET_RECV_BUFFER}.
     */
    long DEFAULT_MASTER_SOCKET_RECV_BUFFER = -1L;

    /**
     * Default value of {@link #MASTER_RPC_READ_TIMEOUT_MS}.
     */
    long DEFAULT_MASTER_RPC_READ_TIMEOUT_MS = RpcConstants.CFG_RPC_READ_TIMEOUT_DEFAULT_MS;

    /**
     * Default value of {@link #MASTER_NETTY_WRITE_BUFFER_HIGH_WATERMARK}.
     */
    long DEFAULT_MASTER_NETTY_WRITE_BUFFER_HIGH_WATERMARK = 10 * 1024 * 1024L;

    /**
     * Default value of {@link #MASTER_NETTY_WRITE_BUFFER_LOW_WATERMARK}.
     */
    long DEFAULT_MASTER_NETTY_WRITE_BUFFER_LOW_WATERMARK = 5 * 1024 * 1024L;

    /**
     * Default value of {@link #ONLINE_ONLY_READ_TO_RW_PERIOD_MS}.
     */
    long DEFAULT_ONLINE_ONLY_READ_TO_RW_PERIOD_MS = 2 * 60 * 1000L;

    /**
     * Default value of {@link #STEP_CHG_WAIT_PERIOD_MS}.
     */
    long DEFAULT_STEP_CHG_WAIT_PERIOD_MS = 12 * 1000L;

    /**
     * Default value of {@link #OFFLINE_ONLY_READ_TO_RW_PERIOD_MS}.
     */
    long DEFAULT_OFFLINE_ONLY_READ_TO_RW_PERIOD_MS = 30 * 1000L;

    /**
     * Default value of {@link #CONF_MOD_AUTH_TOKEN}.
     */
    String DEFAULT_CONF_MOD_AUTH_TOKEN = "ASDFGHJKL";

    /**
     * Default value of {@link #MAX_GROUP_BROKER_CONSUME_RATE}.
     */
    int DEFAULT_MAX_GROUP_BROKER_CONSUME_RATE = 50;

    /**
     * Default value of {@link #MAX_GROUP_REBALANCE_WAIT_PERIOD}.
     */
    int DEFAULT_MAX_GROUP_REBALANCE_WAIT_PERIOD = 2;

    /**
     * Default value of {@link #START_OFFSET_RESET_CHECK}.
     */
    boolean DEFAULT_START_OFFSET_RESET_CHECK = false;

    /**
     * Default value of {@link #MASTER_ROW_LOCK_WAIT_DUR_MS}.
     */
    // TServerConstants.CFG_ROWLOCK_DEFAULT_DURATION = 30000
    int DEFAULT_MASTER_ROW_LOCK_WAIT_DUR_MS = 30000;

    /**
     * Default value of {@link #MAX_AUTO_FORBIDDEN_CNT}.
     */
    int DEFAULT_MAX_AUTO_FORBIDDEN_CNT = 5;

    /**
     * Default value of {@link #VISIT_TOKEN_VALID_PERIOD_MS}.
     */
    long DEFAULT_VISIT_TOKEN_VALID_PERIOD_MS = 5 * 60 * 1000L;

    /**
     * Default value of {@link #MASTER_AUTH_VALID_TIME_STAMP_PERIOD_MS}.
     */
    long DEFAULT_MASTER_AUTH_VALID_TIME_STAMP_PERIOD_MS = TBaseConstants.CFG_DEFAULT_AUTH_TIMESTAMP_VALID_INTERVAL;

    /**
     * Default value of {@link #START_VISIT_TOKEN_CHECK}.
     */
    boolean DEFAULT_START_VISIT_TOKEN_CHECK = false;

    /**
     * Default value of {@link #START_PRODUCE_AUTHENTICATE}.
     */
    boolean DEFAULT_START_PRODUCE_AUTHENTICATE = false;

    /**
     * Default value of {@link #START_PRODUCE_AUTHORIZE}.
     */
    boolean DEFAULT_START_PRODUCE_AUTHORIZE = false;

    /**
     * Default value of {@link #USE_WEB_PROXY}.
     */
    boolean DEFAULT_USE_WEB_PROXY = false;

    /**
     * Default value of {@link #START_CONSUME_AUTHENTICATE}.
     */
    boolean DEFAULT_START_CONSUME_AUTHENTICATE = false;

    /**
     * Default value of {@link #START_CONSUME_AUTHORIZE}.
     */
    boolean DEFAULT_START_CONSUME_AUTHORIZE = false;

    /**
     * Default value of {@link #NEED_BROKER_VISIT_AUTH}.
     */
    boolean DEFAULT_NEED_BROKER_VISIT_AUTH = false;

    /**
     * Default value of {@link #MASTER_VISIT_NAME}.
     */
    String DEFAULT_MASTER_VISIT_NAME = "";

    /**
     * Default value of {@link #MASTER_VISIT_PASSWORD}.
     */
    String DEFAULT_MASTER_VISIT_PASSWORD = "";

    /**
     * Default value of {@link #REBALANCE_PARALLEL}.
     */
    int DEFAULT_REBALANCE_PARALLEL = 4;

    /**
     * --------------------------------------------------------------
     * Bdb default values.
     * --------------------------------------------------------------
     */
    // nothing.
}
