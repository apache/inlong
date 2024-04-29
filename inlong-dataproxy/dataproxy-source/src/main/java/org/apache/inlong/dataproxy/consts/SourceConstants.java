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

package org.apache.inlong.dataproxy.consts;

public class SourceConstants {

    // source host
    public static final String SRCCXT_CONFIG_HOST = "host";
    // system env source host
    public static final String SYSENV_HOST_IP = "inlongHostIp";
    // default source host
    public static final String VAL_DEF_HOST_VALUE = "0.0.0.0";
    // loopback host
    public static final String VAL_LOOPBACK_HOST_VALUE = "127.0.0.1";
    // source port
    public static final String SRCCXT_CONFIG_PORT = "port";
    // system env source port
    public static final String SYSENV_HOST_PORT = "inlongHostPort";
    // source logic execute type
    public static final String SRCCXT_LOGIC_EXECUTE_TYPE = "logic-execute-type";
    // message factory name
    public static final String SRCCXT_MSG_FACTORY_NAME = "msg-factory-name";
    // message handler name
    public static final String SRCCXT_MESSAGE_HANDLER_NAME = "message-handler-name";
    // max message length
    public static final String SRCCXT_MAX_MSG_LENGTH = "max-msg-length";
    // allowed max message length
    public static final int VAL_MAX_MAX_MSG_LENGTH = 20 * 1024 * 1024;
    public static final int VAL_MIN_MAX_MSG_LENGTH = 5;
    public static final int VAL_DEF_MAX_MSG_LENGTH = 1024 * 64;
    // whether compress message
    public static final String SRCCXT_MSG_COMPRESSED = "msg-compressed";
    public static final boolean VAL_DEF_MSG_COMPRESSED = true;
    // whether filter empty message
    public static final String SRCCXT_FILTER_EMPTY_MSG = "filter-empty-msg";
    public static final boolean VAL_DEF_FILTER_EMPTY_MSG = false;
    // whether custom channel processor
    public static final String SRCCXT_CUSTOM_CHANNEL_PROCESSOR = "custom-cp";
    public static final boolean VAL_DEF_CUSTOM_CH_PROCESSOR = false;
    // max net accept process threads
    public static final String SRCCXT_MAX_ACCEPT_THREADS = "max-accept-threads";
    public static final int VAL_DEF_NET_ACCEPT_THREADS = 1;
    public static final int VAL_MIN_ACCEPT_THREADS = 1;
    public static final int VAL_MAX_ACCEPT_THREADS = 10;
    // max net worker process threads
    public static final String SRCCXT_MAX_WORKER_THREADS = "max-threads";
    public static final int VAL_DEF_WORKER_THREADS = Runtime.getRuntime().availableProcessors();
    public static final int VAL_MIN_WORKER_THREADS = 1;
    // max connection count
    public static final String SRCCXT_MAX_CONNECTION_CNT = "connections";
    public static final int VAL_DEF_MAX_CONNECTION_CNT = 5000;
    public static final int VAL_MIN_CONNECTION_CNT = 0;
    // max receive buffer size
    public static final String SRCCXT_RECEIVE_BUFFER_SIZE = "receiveBufferSize";
    public static final int VAL_DEF_RECEIVE_BUFFER_SIZE = 64 * 1024;
    public static final int VAL_MIN_RECEIVE_BUFFER_SIZE = 0;
    // max send buffer size
    public static final String SRCCXT_SEND_BUFFER_SIZE = "sendBufferSize";
    public static final int VAL_DEF_SEND_BUFFER_SIZE = 64 * 1024;
    public static final int VAL_MIN_SEND_BUFFER_SIZE = 0;
    // connect backlog
    public static final String SRCCXT_CONN_BACKLOG = "con-backlog";
    public static final int VAL_DEF_CONN_BACKLOG = 128;
    public static final int VAL_MIN_CONN_BACKLOG = 0;
    // connect linger
    public static final String SRCCXT_CONN_LINGER = "con-linger";
    public static final int VAL_MIN_CONN_LINGER = 0;
    // connect reuse address
    public static final String SRCCXT_REUSE_ADDRESS = "reuse-address";
    public static final boolean VAL_DEF_REUSE_ADDRESS = true;
    // tcp parameter no delay
    public static final String SRCCXT_TCP_NO_DELAY = "tcpNoDelay";
    public static final boolean VAL_DEF_TCP_NO_DELAY = true;
    // tcp parameter keep alive
    public static final String SRCCXT_TCP_KEEP_ALIVE = "keepAlive";
    public static final boolean VAL_DEF_TCP_KEEP_ALIVE = true;
    // tcp parameter high water mark
    public static final String SRCCXT_TCP_HIGH_WATER_MARK = "highWaterMark";
    public static final int VAL_DEF_TCP_HIGH_WATER_MARK = 64 * 1024;
    public static final int VAL_MIN_TCP_HIGH_WATER_MARK = 0;
    // tcp parameter enable busy wait
    public static final String SRCCXT_TCP_ENABLE_BUSY_WAIT = "enableBusyWait";
    public static final boolean VAL_DEF_TCP_ENABLE_BUSY_WAIT = false;
    // tcp parameters max read idle time
    public static final String SRCCXT_MAX_READ_IDLE_TIME_MS = "maxReadIdleTime";
    public static final long VAL_DEF_READ_IDLE_TIME_MS = 3 * 60 * 1000;
    public static final long VAL_MIN_READ_IDLE_TIME_MS = 60 * 1000;
    public static final long VAL_MAX_READ_IDLE_TIME_MS = 70 * 60 * 1000;
    // source protocol type
    public static final String SRC_PROTOCOL_TYPE_TCP = "tcp";
    public static final String SRC_PROTOCOL_TYPE_UDP = "udp";
    public static final String SRC_PROTOCOL_TYPE_HTTP = "http";
}
