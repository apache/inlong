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

package org.apache.inlong.sdk.dataproxy;

public class ConfigConstants {

    public static final String PROXY_SDK_VERSION = "1.2.11";

    public static String HTTP = "http://";
    public static String HTTPS = "https://";

    // dataproxy node config
    public static final String MANAGER_DATAPROXY_API = "/inlong/manager/openapi/dataproxy/getIpList/";
    public static final String META_STORE_SUB_DIR = "/.inlong/";
    public static final String LOCAL_DP_CONFIG_FILE_SUFFIX = ".local";
    public static final String REMOTE_DP_CACHE_FILE_SUFFIX = ".proxyip";
    public static final String REMOTE_ENCRYPT_CACHE_FILE_SUFFIX = ".pubKey";
    // authorization key
    public static final String BASIC_AUTH_HEADER = "authorization";

    // config info sync interval in minutes
    public static final int VAL_DEF_CONFIG_SYNC_INTERVAL_MIN = 3;
    public static final int VAL_MIN_CONFIG_SYNC_INTERVAL_MIN = 1;
    public static final int VAL_MAX_CONFIG_SYNC_INTERVAL_MIN = 30;
    public static final long VAL_UNIT_MIN_TO_MS = 60 * 1000L;
    // config info sync max retry if failure
    public static final int VAL_DEF_RETRY_IF_CONFIG_SYNC_FAIL = 3;
    public static final int VAL_MAX_RETRY_IF_CONFIG_SYNC_FAIL = 5;
    // cache config expired time in ms
    public static final long VAL_DEF_CACHE_CONFIG_EXPIRED_MS = 20 * 60 * 1000L;
    // node force choose interval in ms
    public static final long VAL_DEF_FORCE_CHOOSE_INR_MS = 10 * 60 * 1000L;
    public static final long VAL_MIN_FORCE_CHOOSE_INR_MS = 30 * 1000L;

    // connection timeout in milliseconds
    public static final int VAL_DEF_CONNECT_TIMEOUT_MS = 10000;
    public static final int VAL_MIN_CONNECT_TIMEOUT_MS = 2000;
    public static final int VAL_MAX_CONNECT_TIMEOUT_MS = 60000;
    public static final int VAL_DEF_CONNECT_CLOSE_DELAY_MS = 500;
    // socket timeout in milliseconds
    public static final int VAL_DEF_SOCKET_TIMEOUT_MS = 20000;
    public static final int VAL_MIN_SOCKET_TIMEOUT_MS = 2000;
    public static final int VAL_MAX_SOCKET_TIMEOUT_MS = 60000;

    public static final int ALIVE_CONNECTIONS = 3;
    public static final int MAX_TIMEOUT_CNT = 3;
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

    // request timeout in milliseconds
    public static final long VAL_DEF_REQUEST_TIMEOUT_MS = 10000L;
    public static final long VAL_MIN_REQUEST_TIMEOUT_MS = 500L;

    public static final int DEFAULT_SEND_BUFFER_SIZE = 16777216;
    public static final int DEFAULT_RECEIVE_BUFFER_SIZE = 16777216;

    public static final String RECEIVE_BUFFER_SIZE = "receiveBufferSize";
    public static final String SEND_BUFFER_SIZE = "sendBufferSize";

    public static final int FLAG_ALLOW_AUTH = 1 << 7;
    public static final int FLAG_ALLOW_ENCRYPT = 1 << 6;
    public static final int FLAG_ALLOW_COMPRESS = 1 << 5;

    public static LoadBalance DEFAULT_LOAD_BALANCE = LoadBalance.ROBIN;
    public static int DEFAULT_VIRTUAL_NODE = 1000;
    public static int DEFAULT_RANDOM_MAX_RETRY = 1000;

    public static int DEFAULT_SENDER_MAX_ATTEMPT = 1;

    /* Reserved attribute data size(bytes). */
    public static int RESERVED_ATTRIBUTE_LENGTH = 10000;

}
