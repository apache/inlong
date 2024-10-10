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

package org.apache.inlong.agent.constant;

/**
 * Common constants.
 */
public class CommonConstants {

    public static final String PROXY_INLONG_GROUP_ID = "proxy.inlongGroupId";
    public static final String DEFAULT_PROXY_INLONG_GROUP_ID = "default_inlong_group_id";

    public static final String PROXY_INLONG_STREAM_ID = "proxy.inlongStreamId";
    public static final String DEFAULT_PROXY_INLONG_STREAM_ID = "default_inlong_stream_id";

    public static final String PROXY_TOTAL_ASYNC_PROXY_SIZE = "proxy.total.async.proxy.size";
    public static final int DEFAULT_PROXY_TOTAL_ASYNC_PROXY_SIZE = 200 * 1024 * 1024;

    public static final String PROXY_ALIVE_CONNECTION_NUM = "proxy.alive.connection.num";
    public static final int DEFAULT_PROXY_ALIVE_CONNECTION_NUM = 10;

    public static final String PROXY_MSG_TYPE = "proxy.msgType";
    public static final int DEFAULT_PROXY_MSG_TYPE = 7;

    public static final String PROXY_IS_COMPRESS = "proxy.is.compress";
    public static final boolean DEFAULT_PROXY_IS_COMPRESS = true;

    public static final String PROXY_MAX_SENDER_PER_GROUP = "proxy.max.sender.per.group";
    public static final int DEFAULT_PROXY_MAX_SENDER_PER_GROUP = 10;

    // max size of message list
    public static final String PROXY_PACKAGE_MAX_SIZE = "proxy.package.maxSize";

    // the same task must have the same Partition Key if choose sync
    public static final String PROXY_SEND_PARTITION_KEY = "proxy.partitionKey";

    // max size of single batch in bytes, default is 500KB
    public static final int DEFAULT_PROXY_PACKAGE_MAX_SIZE = 500 * 1024;

    public static final String PROXY_INLONG_STREAM_ID_QUEUE_MAX_NUMBER = "proxy.group.queue.maxNumber";
    public static final int DEFAULT_PROXY_INLONG_STREAM_ID_QUEUE_MAX_NUMBER = 10000;

    public static final String PROXY_PACKAGE_MAX_TIMEOUT_MS = "proxy.package.maxTimeout.ms";
    public static final int DEFAULT_PROXY_PACKAGE_MAX_TIMEOUT_MS = 4 * 1000;

    public static final String PROXY_BATCH_FLUSH_INTERVAL = "proxy.batch.flush.interval";
    public static final int DEFAULT_PROXY_BATCH_FLUSH_INTERVAL = 1;

    public static final String PROXY_SENDER_MAX_TIMEOUT = "proxy.sender.maxTimeout";
    // max timeout in seconds.
    public static final int DEFAULT_PROXY_SENDER_MAX_TIMEOUT = 60;

    public static final String PROXY_SENDER_MAX_RETRY = "proxy.sender.maxRetry";
    public static final int DEFAULT_PROXY_SENDER_MAX_RETRY = 5;

    public static final String PROXY_IS_FILE = "proxy.isFile";
    public static final boolean DEFAULT_IS_FILE = false;

    public static final String PROXY_CLIENT_IO_THREAD_NUM = "client.iothread.num";
    public static final int DEFAULT_PROXY_CLIENT_IO_THREAD_NUM =
            Runtime.getRuntime().availableProcessors();

    public static final String PROXY_CLIENT_ENABLE_BUSY_WAIT = "client.enable.busy.wait";
    public static final boolean DEFAULT_PROXY_CLIENT_ENABLE_BUSY_WAIT = false;

    public static final String PROXY_RETRY_SLEEP = "proxy.retry.sleep";
    public static final long DEFAULT_PROXY_RETRY_SLEEP = 500;

    public static final String FIELD_SPLITTER = "proxy.field.splitter";
    public static final String DEFAULT_FIELD_SPLITTER = "|";

    public static final String PROXY_KEY_GROUP_ID = "inlongGroupId";
    public static final String PROXY_KEY_STREAM_ID = "inlongStreamId";
    public static final String PROXY_KEY_DATA = "dataKey";

    public static final int DEFAULT_FILE_MAX_NUM = 4096;
    public static final String TASK_ID_PREFIX = "task";
    public static final String INSTANCE_ID_PREFIX = "ins";
    public static final String OFFSET_ID_PREFIX = "offset";
    public static final String AGENT_OS_NAME = "os.name";
    public static final String AGENT_NIX_OS = "nix";
    public static final String AGENT_NUX_OS = "nux";
    public static final String AGENT_COLON = ":";
    public static final Integer DEFAULT_MAP_CAPACITY = 16;
    public static final String COMMA = ",";
}
