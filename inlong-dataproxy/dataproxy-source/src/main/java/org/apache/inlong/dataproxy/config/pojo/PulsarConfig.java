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

package org.apache.inlong.dataproxy.config.pojo;

import org.apache.flume.Context;

import java.util.HashMap;
import java.util.Map;

public class PulsarConfig extends Context {

    private Map<String, String> url2token = new HashMap<>();

    /*
     * properties key for pulsar client
     */
    public static final String PULSAR_SERVER_URL_LIST = "pulsar_server_url_list";
    /*
     * properties key pulsar producer
     */
    public static final String TOKEN = "token";
    private static final String SEND_TIMEOUT = "send_timeout_mill";
    private static final String CLIENT_TIMEOUT = "client_timeout_second";
    private static final String ENABLE_BATCH = "enable_batch";
    private static final String BLOCK_IF_QUEUE_FULL = "block_if_queue_full";
    private static final String MAX_PENDING_MESSAGES = "max_pending_messages";
    private static final String MAX_BATCHING_MESSAGES = "max_batching_messages";
    private static final String RETRY_INTERVAL_WHEN_SEND_ERROR_MILL = "retry_interval_when_send_error_ms";
    private static final String SINK_THREAD_NUM = "thread-num";
    private static final String DISK_IO_RATE_PER_SEC = "disk-io-rate-per-sec";
    /*
     * properties for stat
     */
    private static final String STAT_INTERVAL_SEC = "stat_interval_sec";
    private static final String LOG_EVERY_N_EVENTS = "log-every-n-events";
    private static final String CLIENT_ID_CACHE = "client_id_cache";
    private static final String RETRY_CNT = "retry_currentSuccSendedCnt";

    private static final int DEFAULT_CLIENT_TIMEOUT_SECOND = 30;
    private static final int DEFAULT_SEND_TIMEOUT_MILL = 30 * 1000;
    private static final long DEFAULT_RETRY_INTERVAL_WHEN_SEND_ERROR_MILL = 30 * 1000L;
    private static final boolean DEFAULT_ENABLE_BATCH = true;
    private static final boolean DEFAULT_BLOCK_IF_QUEUE_FULL = true;
    private static final int DEFAULT_MAX_PENDING_MESSAGES = 10000;
    private static final int DEFAULT_MAX_BATCHING_MESSAGES = 1000;
    private static final int DEFAULT_RETRY_CNT = -1;
    private static final int DEFAULT_LOG_EVERY_N_EVENTS = 100000;
    private static final int DEFAULT_STAT_INTERVAL_SEC = 60;
    private static final int DEFAULT_THREAD_NUM = 4;
    private static final boolean DEFAULT_CLIENT_ID_CACHE = true;
    private static final long DEFAULT_DISK_IO_RATE_PER_SEC = 0L;

    public Map<String, String> getUrl2token() {
        return url2token;
    }

    public void setUrl2token(Map<String, String> url2token) {
        this.url2token = url2token;
    }

    public int getSendTimeoutMs() {
        return getInteger(SEND_TIMEOUT, DEFAULT_SEND_TIMEOUT_MILL);
    }

    public int getClientTimeoutSecond() {
        return getInteger(CLIENT_TIMEOUT, DEFAULT_CLIENT_TIMEOUT_SECOND);
    }

    public boolean getEnableBatch() {
        return getBoolean(ENABLE_BATCH, DEFAULT_ENABLE_BATCH);
    }

    public boolean getBlockIfQueueFull() {
        return getBoolean(BLOCK_IF_QUEUE_FULL, DEFAULT_BLOCK_IF_QUEUE_FULL);
    }

    public int getMaxPendingMessages() {
        return getInteger(MAX_PENDING_MESSAGES, DEFAULT_MAX_PENDING_MESSAGES);
    }

    public int getMaxBatchingMessages() {
        return getInteger(MAX_BATCHING_MESSAGES, DEFAULT_MAX_BATCHING_MESSAGES);
    }

    public long getRetryIntervalWhenSendErrorMs() {
        return getLong(RETRY_INTERVAL_WHEN_SEND_ERROR_MILL, DEFAULT_RETRY_INTERVAL_WHEN_SEND_ERROR_MILL);
    }

    public int getRetyCnt() {
        return getInteger(RETRY_CNT, DEFAULT_RETRY_CNT);
    }

    public int getStatIntervalSec() {
        return getInteger(STAT_INTERVAL_SEC, DEFAULT_STAT_INTERVAL_SEC);
    }

    public int getLogEveryNEvents() {
        return getInteger(LOG_EVERY_N_EVENTS, DEFAULT_LOG_EVERY_N_EVENTS);
    }

    public boolean getClientIdCache() {
        return getBoolean(CLIENT_ID_CACHE, DEFAULT_CLIENT_ID_CACHE);
    }

    public int getThreadNum() {
        return getInteger(SINK_THREAD_NUM, DEFAULT_THREAD_NUM);
    }

    public long getDiskIoRatePerSec() {
        return getLong(DISK_IO_RATE_PER_SEC, DEFAULT_DISK_IO_RATE_PER_SEC);
    }
}