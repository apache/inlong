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
import org.apache.tubemq.corebase.TServerConstants;

public interface BrokerCfgConst extends CommonCfgConst {

    /**
     * --------------------------------------------------------------------------
     * Broker constants.
     * --------------------------------------------------------------------------
     */
    /**
     * Broker id.
     */
    String BROKER_ID = "brokerId";

    /**
     * Broker port.
     */
    String BROKER_PORT = PORT;

    /**
     * Broker primaryPath.
     */
    String PRIMARY_PATH = "primaryPath";

    /**
     * Broker hostname.
     */
    String BROKER_HOST_NAME = HOST_NAME;

    /**
     * Default network interface.
     */
    String DEF_ETH_NAME = "defEthName";

    /**
     * Master service address.
     */
    String MASTER_ADDRESS_LIST = "masterAddressList";

    /**
     * Broker web service port.
     */
    String BROKER_WEB_PORT = WEB_PORT;

    /**
     * Max data segment size.
     */
    String MAX_SEGMENT_SIZE = "maxSegmentSize";

    /**
     * Transfer size.
     */
    String TRANSFER_SIZE = "transferSize";

    /**
     * Transfer index count.
     */
    String INDEX_TRANS_COUNT = "indexTransCount";

    /**
     * Log cleanup interval in milliseconds.
     */
    String LOG_CLEAR_UP_DURATION_MS = "logClearupDurationMs";

    /**
     * Log flush to disk interval in milliseconds.
     */
    String LOG_FLUSH_DISK_DUR_MS = "logFlushDiskDurMs";

    /**
     * Memory flush to disk interval in milliseconds.
     */
    String LOG_FLUSH_MEM_DUR_MS = "logFlushMemDurMs";

    String BROKER_AUTH_VALID_TIMESTAMP_PERIOD_MS = AUTH_VALID_TIMESTAMP_PERIOD_MS;

    String VISIT_TOKEN_CHECK_IN_VALID_TIME_MS = "visitTokenCheckInValidTimeMs";

    /**
     * Socket send buffer.
     */
    String BROKER_SOCKET_SEND_BUFFER = SOCKET_SEND_BUFFER;

    /**
     * Socket receive buffer.
     */
    String BROKER_SOCKET_RECV_BUFFER = SOCKET_RECV_BUFFER;

    /**
     * Max index segment size.
     */
    String MAX_INDEX_SEGMENT_SIZE = "maxIndexSegmentSize";

    String UPDATE_CONSUMER_OFFSETS = "updateConsumerOffsets";

    /**
     * Rpc read timeout in milliseconds.
     */
    String BROKER_RPC_READ_TIMEOUT_MS = RPC_READ_TIMEOUT_MS;

    /**
     * Netty write buffer high water mark.
     */
    String BROKER_NETTY_WRITE_BUFFER_HIGH_WATERMARK = NETTY_WRITE_BUFFER_HIGH_WATERMARK;

    /**
     * Netty write buffer low water mark.
     */
    String BROKER_NETTY_WRITE_BUFFER_LOW_WATERMARK = NETTY_WRITE_BUFFER_LOW_WATERMARK;

    /**
     * Heartbeat interval in milliseconds.
     */
    String HEARTBEAT_PERIOD_MS = "heartbeatPeriodMs";

    /**
     * Tcp write service thread count.
     */
    String TCP_WRITE_SERVICE_THREAD = "tcpWriteServiceThread";

    /**
     * Tcp read service thread count.
     */
    String TCP_READ_SERVICE_THREAD = "tcpReadServiceThread";

    /**
     * TLS write service thread count.
     */
    String TLS_WRITE_SERVICE_THREAD = "tlsWriteServiceThread";

    /**
     * TLS read service thread count.
     */
    String TLS_READ_SERVICE_THREAD = "tlsReadServiceThread";

    /**
     * Consumer register timeout in milliseconds.
     */
    String CONSUMER_REG_TIMEOUT_MS = "consumerRegTimeoutMs";

    String DEFAULT_DEDUCE_READ_SIZE = "defaultDeduceReadSize";

    /**
     * Row lock wait duration.
     */
    String BROKER_ROW_LOCK_WAIT_DUR_MS = ROW_LOCK_WAIT_DUR_MS;

    /**
     * Read io exception max count.
     */
    String ALLOWED_READ_IO_EXCEPTION_CNT = "allowedReadIOExcptCnt";

    /**
     * Write io exception max count.
     */
    String ALLOWED_WRITE_IO_EXCEPTION_CNT = "allowedWriteIOExcptCnt";

    String IO_EXCEPTION_STATS_DURATION_MS = "ioExcptStatsDurationMs";

    String VISIT_MASTER_AUTH = "visitMasterAuth";

    String BROKER_VISIT_NAME = VISIT_NAME;

    String BROKER_VISIT_PASSWORD = VISIT_PASSWORD;

    /**
     * --------------------------------------------------------------
     * Broker default values.
     * --------------------------------------------------------------
     */

    /**
     * Default value of {@link #BROKER_ID}.
     */
    int DEFAULT_BROKER_ID = 0;

    /**
     * Default value of {@link #BROKER_PORT}.
     */
    int DEFAULT_BROKER_PORT = TBaseConstants.META_DEFAULT_BROKER_PORT;

    /**
     * Default value of {@link #PRIMARY_PATH}.
     */
    String DEFAULT_PRIMARY_PATH = "";

    /**
     * Default value of {@link #BROKER_HOST_NAME}.
     */
    String DEFAULT_BROKER_HOST_NAME = "";

    /**
     * Default value of {@link #DEF_ETH_NAME}.
     */
    String DEFAULT_DEF_ETH_NAME = "eth1";

    /**
     * Default value of {@link #MASTER_ADDRESS_LIST}.
     */
    String DEFAULT_MASTER_ADDRESS_LIST = "";

    /**
     * Default value of {@link #BROKER_WEB_PORT}.
     */
    int DEFAULT_BROKER_WEB_PORT = 8081;

    /**
     * Default value of {@link #MAX_SEGMENT_SIZE}.
     */
    int DEFAULT_MAX_SEGMENT_SIZE = 1024 * 1024 * 1024;

    /**
     * Default value of {@link #TRANSFER_SIZE}.
     */
    int DEFAULT_TRANSFER_SIZE = 512 * 1024;

    /**
     * Default value of {@link #INDEX_TRANS_COUNT}.
     */
    int DEFAULT_INDEX_TRANS_COUNT = 1000;

    /**
     * Default value of {@link #LOG_CLEAR_UP_DURATION_MS}.
     */
    long DEFAULT_LOG_CLEAR_UP_DURATION_MS = 3 * 60 * 1000L;

    /**
     * Default value of {@link #LOG_FLUSH_DISK_DUR_MS}.
     */
    long DEFAULT_LOG_FLUSH_DISK_DUR_MS = 20 * 1000L;

    /**
     * Default value of {@link #LOG_FLUSH_MEM_DUR_MS}.
     */
    long DEFAULT_LOG_FLUSH_MEM_DUR_MS = 10 * 1000L;

    /**
     * Default value of {@link #BROKER_AUTH_VALID_TIMESTAMP_PERIOD_MS}.
     */
    long DEFAULT_BROKER_AUTH_VALID_TIMESTAMP_PERIOD_MS = TBaseConstants.CFG_DEFAULT_AUTH_TIMESTAMP_VALID_INTERVAL;

    /**
     * Default value of {@link #VISIT_TOKEN_CHECK_IN_VALID_TIME_MS}.
     */
    long DEFAULT_VISIT_TOKEN_CHECK_IN_VALID_TIME_MS = 120000L;

    /**
     * Default value of {@link #BROKER_SOCKET_SEND_BUFFER}.
     */
    long DEFAULT_BROKER_SOCKET_SEND_BUFFER = -1;

    /**
     * Default value of {@link #BROKER_SOCKET_RECV_BUFFER}.
     */
    long DEFAULT_BROKER_SOCKET_RECV_BUFFER = -1;


    /**
     * Default value of {@link #MAX_INDEX_SEGMENT_SIZE}.
     */
    // DataStoreUtils.STORE_INDEX_HEAD_LEN = 28
    // int DEFAULT_MAX_INDEX_SEGMENT_SIZE = 700000 * 28;

    /**
     * Default value of {@link #UPDATE_CONSUMER_OFFSETS}.
     */
    boolean DEFAULT_UPDATE_CONSUMER_OFFSETS = true;

    /**
     * Default value of {@link #BROKER_RPC_READ_TIMEOUT_MS}.
     */
    long DEFAULT_BROKER_RPC_READ_TIMEOUT_MS = 10 * 1000;

    /**
     * Default value of {@link #BROKER_NETTY_WRITE_BUFFER_HIGH_WATERMARK}.
     */
    long DEFAULT_BROKER_NETTY_WRITE_BUFFER_HIGH_WATERMARK = 10 * 1024 * 1024L;

    /**
     * Default value of {@link #BROKER_NETTY_WRITE_BUFFER_LOW_WATERMARK}.
     */
    long DEFAULT_BROKER_NETTY_WRITE_BUFFER_LOW_WATERMARK = 5 * 1024 * 1024L;

    /**
     * Default value of {@link #HEARTBEAT_PERIOD_MS}.
     */
    long DEFAULT_HEARTBEAT_PERIOD_MS = 8000L;

    /**
     * Default value of {@link #TCP_WRITE_SERVICE_THREAD}.
     */
    int DEFAULT_TCP_WRITE_SERVICE_THREAD = Runtime.getRuntime().availableProcessors() * 2;

    /**
     * Default value of {@link #TCP_READ_SERVICE_THREAD}.
     */
    int DEFAULT_TCP_READ_SERVICE_THREAD = Runtime.getRuntime().availableProcessors() * 2;

    /**
     * Default value of {@link #TLS_WRITE_SERVICE_THREAD}.
     */
    int DEFAULT_TLS_WRITE_SERVICE_THREAD = Runtime.getRuntime().availableProcessors() * 2;

    /**
     * Default value of {@link #TLS_READ_SERVICE_THREAD}.
     */
    int DEFAULT_TLS_READ_SERVICE_THREAD = Runtime.getRuntime().availableProcessors() * 2;
    ;

    /**
     * Default value of {@link #CONSUMER_REG_TIMEOUT_MS}.
     */
    int DEFAULT_CONSUMER_REG_TIMEOUT_MS = 30000;

    /**
     * Default value of {@link #DEFAULT_DEDUCE_READ_SIZE}.
     */
    long DEFAULT_DEFAULT_DEDUCE_READ_SIZE = 7 * 1024 * 1024 * 1024L;

    /**
     * Default value of {@link #BROKER_ROW_LOCK_WAIT_DUR_MS}.
     */
    int DEFAULT_BROKER_ROW_LOCK_WAIT_DUR_MS = TServerConstants.CFG_ROWLOCK_DEFAULT_DURATION;

    /**
     * Default value of {@link #ALLOWED_READ_IO_EXCEPTION_CNT}.
     */
    int DEFAULT_ALLOWED_READ_IO_EXCEPTION_CNT = 10;

    /**
     * Default value of {@link #ALLOWED_WRITE_IO_EXCEPTION_CNT}.
     */
    int DEFAULT_ALLOWED_WRITE_IO_EXCEPTION_CNT = 10;

    /**
     * Default value of {@link #IO_EXCEPTION_STATS_DURATION_MS}.
     */
    long DEFAULT_IO_EXCEPTION_STATS_DURATION_MS = 120000L;

    /**
     * Default value of {@link #VISIT_MASTER_AUTH}.
     */
    boolean DEFAULT_VISIT_MASTER_AUTH = false;

    /**
     * Default value of {@link #BROKER_VISIT_NAME}.
     */
    String DEFAULT_BROKER_VISIT_NAME = "";

    /**
     * Default value of {@link #BROKER_VISIT_PASSWORD}.
     */
    String DEFAULT_BROKER_VISIT_PASSWORD = "";

}
