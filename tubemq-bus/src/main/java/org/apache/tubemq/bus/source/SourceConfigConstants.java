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
package org.apache.tubemq.bus.source;

public class SourceConfigConstants {
    public static final String CONFIG_PORT = "port";
    public static final String CONFIG_HOST = "host";
    public static final String DEFAULT_CONFIG_HOST = "0.0.0.0";

    public static final String MSG_FACTORY_NAME = "msg-factory-name";
    public static final String DEFAULT_MSG_FACTORY_NAME = "org.apache.tubemq.bus.source.ServerMessageFactory";

    public static final String MAX_THREADS = "max-threads";
    public static final int DEFAULT_MAX_THREADS = 32;

    public static final int DEFAULT_BOSS_NUM = 1;

    public static final String SEND_BUFFER_SIZE = "sendBufferSize";
    public static final int DEFAULT_SEND_BUFFER_SIZE = 1024 * 64;

    public static final String TCP_NO_DELAY = "tcpNoDelay";
    public static final boolean DEFAULT_TCP_NO_DELAY = true;

    public static final String KEEP_ALIVE = "keepAlive";
    public static final boolean DEFAULT_KEEP_ALIVE = true;

    public static final String HIGH_WATER_MARK = "highWaterMark";
    public static final int DEFAULT_HIGH_WATER_MARK = 64 * 1024;

    public static final String RECEIVE_BUFFER_SIZE = "receiveBufferSize";
    public static final int DEFAULT_RECEIVE_BUFFER_SIZE = 1024 * 64;

    public static final String TRAFFIC_CLASS = "trafficClass";
    public static final int DEFAULT_TRAFFIC_CLASS = 0;

    public static final String SERVICE_PROCESSOR_NAME = "service-decoder-name";
    public static final String DEFAULT_SERVICE_PROCESSOR_NAME = "org.apache.tubemq.bus.source.DefaultServiceDecoder";

    public static final String MESSAGE_HANDLER_NAME = "message-handler-name";
    public static final String DEFAULT_MESSAGE_HANDLER_NAME = "org.apache.tubemq.bus.source.ServerMessageHandler";

    public static final String TCP_PROTOCOL = "tcp";

    public static final int DEFAULT_READ_IDLE_TIME = 70 * 60 * 1000;
}
