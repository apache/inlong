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

import java.io.Serializable;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

public interface CommonCfgConst extends Serializable {

    /**
     * --------------------------------------------------
     * Sections constants.
     * --------------------------------------------------
     */
    String SECT_TOKEN_MASTER = "master";

    String SECT_TOKEN_BROKER = "broker";

    String SECT_TOKEN_BDB = "bdbStore";

    String SECT_TOKEN_TLS = "tlsSetting";

    String SECT_TOKEN_ZKEEPER = "zookeeper";

    String SECT_TOKEN_REPLICATION = "replication";

    /**
     * -------------------------------------------------------
     * Common constants between broker and master components.
     * -------------------------------------------------------
     */
    String PORT = "port";

    String WEB_PORT = "webPort";

    String HOST_NAME = "hostName";

    String VISIT_NAME = "visitName";

    String VISIT_PASSWORD = "visitPassword";

    String AUTH_VALID_TIMESTAMP_PERIOD_MS = "authValidTimeStampPeriodMs";

    String ROW_LOCK_WAIT_DUR_MS = "rowLockWaitDurMs";

    String SOCKET_SEND_BUFFER = "socketSendBuffer";

    String SOCKET_RECV_BUFFER = "socketRecvBuffer";

    String RPC_READ_TIMEOUT_MS = "rpcReadTimeoutMs";

    String NETTY_WRITE_BUFFER_HIGH_WATERMARK = "nettyWriteBufferHighWaterMark";

    String NETTY_WRITE_BUFFER_LOW_WATERMARK = "nettyWriteBufferLowWaterMark";

    Charset DEFAULT_CHARSET = StandardCharsets.UTF_8;

}
