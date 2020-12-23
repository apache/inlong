/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.tubemq.agent.constants;

import org.apache.tubemq.agent.utils.AgentUtils;

public class CommonConstants {
    public static final String BUS_TDMANAGER_HOST = "bus.tdmanager.host";
    public static final String BUS_TDMANAGER_PORT = "bus.tdmanager.port";

    public static final String BUS_NET_TAG = "bus.net.tag";
    public static final String DEFAULT_BUS_NET_TAG = "";

    public static final String BUS_BID = "bus.bid";

    public static final String BUS_LOCALHOST = "bus.localHost";
    public static final String DEFAULT_BUS_LOCALHOST = AgentUtils.getLocalIP();

    public static final String BUS_IS_LOCAL_VISIT = "bus.isLocalVisit";
    public static final boolean DEFAULT_BUS_IS_LOCAL_VISIT = true;

    public static final String BUS_TOTAL_ASYNC_BUF_SIZE = "bus.total.async.bus.size";
    public static final int DEFAULT_BUS_TOTAL_ASYNC_BUF_SIZE = 200 * 1024 * 1024;

    public static final String BUS_ALIVE_CONNECTION_NUM = "bus.alive.connection.num";
    public static final int DEFAULT_BUS_ALIVE_CONNECTION_NUM = 10;

    public static final String BUS_MSG_TYPE = "bus.msgType";
    public static final int DEFAULT_BUS_MSG_TYPE = 7;

    public static final String BUS_IS_COMPRESS = "bus.is.compress";
    public static final boolean DEFAULT_BUS_IS_COMPRESS = true;

    public static final String BUS_MAX_SENDER_PER_BID = "bus.max.sender.per.pid";
    public static final int DEFAULT_BUS_MAX_SENDER_PER_PID = 10;

    // max size of message list
    public static final String BUS_PACKAGE_MAX_SIZE = "bus.package.maxSize";
    // max size of single batch in bytes, default is 200KB.
    public static final int DEFAULT_BUS_PACKAGE_MAX_SIZE = 200000;

    public static final String BUS_TID_QUEUE_MAX_NUMBER = "bus.tid.queue.maxNumber";
    public static final int DEFAULT_BUS_TID_QUEUE_MAX_NUMBER = 10000;

    public static final String BUS_PACKAGE_MAX_TIMEOUT_MS = "bus.package.maxTimeout.ms";
    public static final int DEFAULT_BUS_PACKAGE_MAX_TIMEOUT_MS = 4 * 1000;

    public static final String BUS_BATCH_FLUSH_INTERVAL = "bus.batch.flush.interval";
    public static final int DEFAULT_BUS_BATCH_FLUSH_INTERVAL = 2 * 1000;

    public static final String BUS_SENDER_MAX_TIMEOUT = "bus.sender.maxTimeout";
    // max timeout in seconds.
    public static final int DEFAULT_BUS_SENDER_MAX_TIMEOUT = 20;

    public static final String BUS_SENDER_MAX_RETRY = "bus.sender.maxRetry";
    public static final int DEFAULT_BUS_SENDER_MAX_RETRY = 5;

    public static final String BUS_IS_FILE = "bus.isFile";
    public static final boolean DEFAULT_IS_FILE = true;

    public static final String BUS_RETRY_SLEEP = "bus.retry.sleep";
    public static final long DEFAULT_BUS_RETRY_SLEEP = 500;
}
