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

package org.apache.inlong.common.constant;

/**
 * The constants class
 */
public class Constants {

    public static final int RESULT_SUCCESS = 0;

    public static final int RESULT_FAIL = 1;

    // default kafka topic is {groupId}.{streamId}
    public static final String DEFAULT_KAFKA_TOPIC_FORMAT = "%s.%s";
    public static final String METRICS_AUDIT_PROXY_HOSTS_KEY = "metrics.audit.proxy.hosts";

    // Default audit version is -1
    public static final long DEFAULT_AUDIT_VERSION = -1;

    public static class DataNodeType {

        public static final String KAFKA = "KAFKA";
        public static final String PULSAR = "PULSAR";
        public static final String CLS = "CLS";
        public static final String ELASTICSEARCH = "ELASTICSEARCH";
    }

    public static class DeserializationType {

        public static final String INLONG_MSG = "INLONG_MSG";
        public static final String INLONG_MSG_PB = "INLONG_MSG_PB";
        public static final String CSV = "CSV";
        public static final String KV = "KV";
    }

    public static class CompressionType {

        public static final String GZIP = "GZIP";
        public static final String SNAPPY = "SNAPPY";
        public static final String LZO = "LZO";
        public static final String NONE = "NONE";
    }

    /**
     * Constants of MQ type.
     */
    public static class MQType {

        public static final String TUBEMQ = "TUBEMQ";
        public static final String PULSAR = "PULSAR";
        public static final String KAFKA = "KAFKA";
        public static final String TDMQ_PULSAR = "TDMQ_PULSAR";

        /**
         * Not use any MQ
         */
        public static final String NONE = "NONE";

    }

    /**
     * Constants of protocol type.
     */
    public static class ProtocolType {

        public static final String TCP = "TCP";
        public static final String UDP = "UDP";

        public static final String HTTP = "HTTP";
        public static final String HTTPS = "HTTPS";

    }

    public static class SinkType {

        public static final String KAFKA = "KAFKA";
        public static final String PULSAR = "PULSAR";
        public static final String CLS = "CLS";
        public static final String ELASTICSEARCH = "ELASTICSEARCH";
    }
}
