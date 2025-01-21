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

package org.apache.inlong.sdk.dataproxy.common;

public class SdkConsts {

    public static String PREFIX_HTTP = "http://";
    public static String PREFIX_HTTPS = "https://";

    // dataproxy node config
    public static final String MANAGER_DATAPROXY_API = "/inlong/manager/openapi/dataproxy/getIpList/";
    public static final String META_STORE_SUB_DIR = "/.inlong/";
    public static final String LOCAL_DP_CONFIG_FILE_SUFFIX = ".local";
    public static final String REMOTE_DP_CACHE_FILE_SUFFIX = ".proxyip";
    public static final String REMOTE_ENCRYPT_CACHE_FILE_SUFFIX = ".pubKey";
    // authorization key
    public static final String BASIC_AUTH_HEADER = "authorization";
    // default region name
    public static final String VAL_DEF_REGION_NAME = "";
    // config info sync interval in minutes
    public static final int VAL_DEF_CONFIG_SYNC_INTERVAL_MIN = 3;
    public static final int VAL_MIN_CONFIG_SYNC_INTERVAL_MIN = 1;
    public static final int VAL_MAX_CONFIG_SYNC_INTERVAL_MIN = 30;
    public static final long VAL_UNIT_MIN_TO_MS = 60 * 1000L;
    // config info sync max retry if failure
    public static final int VAL_DEF_RETRY_IF_CONFIG_QUERY_FAIL = 3;
    public static final int VAL_DEF_RETRY_IF_CONFIG_SYNC_FAIL = 2;
    public static final int VAL_MAX_RETRY_IF_CONFIG_SYNC_FAIL = 5;
    // config wait ms if retry failure
    public static final int VAL_DEF_WAIT_MS_IF_CONFIG_QUERY_FAIL = 500;
    public static final int VAL_DEF_WAIT_MS_IF_CONFIG_SYNC_FAIL = 800;
    public static final int VAL_MAX_WAIT_MS_IF_CONFIG_REQ_FAIL = 20000;
    public static final int VAL_MIN_WAIT_MS_IF_CONFIG_REQ_FAIL = 100;

    // cache config expired time in ms
    public static final long VAL_DEF_CACHE_CONFIG_EXPIRED_MS = 20 * 60 * 1000L;
    // cache config fail status expired time in ms
    public static final long VAL_DEF_CONFIG_FAIL_STATUS_EXPIRED_MS = 400L;
    public static final long VAL_MAX_CONFIG_FAIL_STATUS_EXPIRED_MS = 3 * 60 * 1000L;

    // node force choose interval in ms
    public static final long VAL_DEF_FORCE_CHOOSE_INR_MS = 10 * 60 * 1000L;
    public static final long VAL_MIN_FORCE_CHOOSE_INR_MS = 30 * 1000L;

    // connection timeout in milliseconds
    public static final int VAL_DEF_TCP_CONNECT_TIMEOUT_MS = 8000;
    public static final int VAL_DEF_HTTP_CONNECT_TIMEOUT_MS = 8000;
    public static final int VAL_MIN_CONNECT_TIMEOUT_MS = 2000;
    public static final int VAL_MAX_CONNECT_TIMEOUT_MS = 60000;
    public static final int VAL_DEF_CONNECT_CLOSE_DELAY_MS = 500;
    // socket timeout in milliseconds
    public static final int VAL_DEF_MGR_SOCKET_TIMEOUT_MS = 15000;
    public static final int VAL_DEF_NODE_SOCKET_TIMEOUT_MS = 10000;
    public static final int VAL_MIN_SOCKET_TIMEOUT_MS = 2000;
    public static final int VAL_MAX_SOCKET_TIMEOUT_MS = 60000;
    // active connects
    public static final int VAL_DEF_ALIVE_CONNECTIONS = 7;
    public static final int VAL_MIN_ALIVE_CONNECTIONS = 1;
    // request timeout in milliseconds
    public static final long VAL_DEF_REQUEST_TIMEOUT_MS = 10000L;
    public static final long VAL_MIN_REQUEST_TIMEOUT_MS = 500L;
    // reconnect wait ms
    public static final long VAL_DEF_FROZEN_RECONNECT_WAIT_MS = 30000L;
    public static final long VAL_DEF_BUSY_RECONNECT_WAIT_MS = 20000L;
    public static final long VAL_DEF_RECONNECT_FAIL_WAIT_MS = 120000L;
    public static final long VAL_MAX_RECONNECT_WAIT_MS = 600000L;
    // socket buffer size
    public static final int DEFAULT_SEND_BUFFER_SIZE = 16 * 1024 * 1024; // 16M
    public static final int DEFAULT_RECEIVE_BUFFER_SIZE = 16 * 1024 * 1024; // 16M
    // max inflight msg count per connection
    public static final int MAX_INFLIGHT_MSG_COUNT_PER_CONNECTION = 4000;

    // data compress enable size
    public static final int VAL_DEF_COMPRESS_ENABLE_SIZE = 120;
    public static final int VAL_MIN_COMPRESS_ENABLE_SIZE = 1;
    // TCP netty worker thread num
    public static final int VAL_DEF_TCP_NETTY_WORKER_THREAD_NUM =
            Runtime.getRuntime().availableProcessors();
    public static final int VAL_MIN_TCP_NETTY_WORKER_THREAD_NUM = 1;
    // sync message timeout allowed count
    public static final int VAL_DEF_SYNC_MSG_TIMEOUT_CNT = 10;
    // sync message timeout check duration ms
    public static final long VAL_DEF_SYNC_TIMEOUT_CHK_DUR_MS = 3 * 60 * 1000L;
    public static final long VAL_MIN_SYNC_TIMEOUT_CHK_DUR_MS = 10 * 1000L;

    // HTTP sdk close wait period ms
    public static final long VAL_DEF_HTTP_SDK_CLOSE_WAIT_MS = 20000L;
    public static final long VAL_MAX_HTTP_SDK_CLOSE_WAIT_MS = 90000L;
    public static final long VAL_MIN_HTTP_SDK_CLOSE_WAIT_MS = 100L;
    // HTTP node reuse wait ms if failed
    public static final long VAL_DEF_HTTP_REUSE_FAIL_WAIT_MS = 20000L;
    public static final long VAL_MAX_HTTP_REUSE_FAIL_WAIT_MS = 300000L;
    public static final long VAL_MIN_HTTP_REUSE_FAIL_WAIT_MS = 1000L;
    // HTTP async report request cache size
    public static final int VAL_DEF_HTTP_ASYNC_RPT_CACHE_SIZE = 2000;
    public static final int VAL_MIN_HTTP_ASYNC_RPT_CACHE_SIZE = 1;
    // HTTP async report worker thread count
    public static final int VAL_DEF_HTTP_ASYNC_RPT_WORKER_NUM =
            Runtime.getRuntime().availableProcessors();
    public static final int VAL_MIN_HTTP_ASYNC_RPT_WORKER_NUM = 1;
    // HTTP async report worker thread sleep ms if idle
    public static final int VAL_DEF_HTTP_ASYNC_WORKER_IDLE_WAIT_MS = 300;
    public static final int VAL_MAX_HTTP_ASYNC_WORKER_IDLE_WAIT_MS = 3000;
    public static final int VAL_MIN_HTTP_ASYNC_WORKER_IDLE_WAIT_MS = 10;

    public static final int MAX_TIMEOUT_CNT = 10;
    public static final int LOAD_THRESHOLD = 0;
    public static final int CYCLE = 30;

    public static final int MSG_TYPE = 7;
    public static final int COMPRESS_SIZE = 120;

    /* Configure the thread pool size for sync message sending. */
    public static final int SYNC_THREAD_POOL_SIZE = 5;
    public static final int MAX_SYNC_THREAD_POOL_SIZE = 10;

    /* Configure the in-memory callback size for asynchronously message sending. */
    public static final int ASYNC_CALLBACK_SIZE = 50000;
    public static final int MAX_ASYNC_CALLBACK_SIZE = 2000000;

    /* Configure the proxy IP list refresh parameters. */
    public static final int PROXY_UPDATE_INTERVAL_MINUTES = 5;

    /* one hour interval */
    public static final int PROXY_HTTP_UPDATE_INTERVAL_MINUTES = 60;

    public static final int MAX_LINE_CNT = 30;

    public static final String RECEIVE_BUFFER_SIZE = "receiveBufferSize";
    public static final String SEND_BUFFER_SIZE = "sendBufferSize";

    public static final int FLAG_ALLOW_AUTH = 1 << 7;
    public static final int FLAG_ALLOW_ENCRYPT = 1 << 6;
    public static final int FLAG_ALLOW_COMPRESS = 1 << 5;

    public static final int EXT_FIELD_FLAG_DISABLE_ID2NUM = 1 << 2;
    public static final int EXT_FIELD_FLAG_SEP_BY_LF = 1 << 5;

    public static int DEFAULT_SENDER_MAX_ATTEMPT = 1;

    /* Reserved attribute data size(bytes). */
    public static int RESERVED_ATTRIBUTE_LENGTH = 256;
}
