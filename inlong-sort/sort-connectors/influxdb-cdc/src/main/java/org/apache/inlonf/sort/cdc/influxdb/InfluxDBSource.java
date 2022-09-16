/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.inlonf.sort.cdc.influxdb;

import com.ververica.cdc.debezium.DebeziumDeserializationSchema;
import com.ververica.cdc.debezium.Validator;
import com.ververica.cdc.debezium.internal.DebeziumOffset;
import java.util.Properties;

public class InfluxDBSource<T> {

    private static final String DATABASE_SERVER_NAME = "oracle_logminer";

    /*
    TCP port on which the split reader's HTTP server is running on.
     */
    private int port = 8000;

    /*
    The maximum number of lines that should be parsed per HTTP request.
     */
    private int linesPerRequest = 1000;

    /*
    Size of queue that buffers HTTP requests data points before fetching.
     */
    private int ingestQueueCapacity = 1000;

    /*
    The time-out in seconds for enqueuing an HTTP request to the queue.
     */
    private long enqueueTimeout = 5L;

    private DebeziumDeserializationSchema<T> deserializer;
    private String inlongMetric;
    private String inlongAudit;

    public static <T> Builder<T> builder() {
        return new Builder<>();
    }

    /** Builder class of {@link InfluxDBSource}. */
    public static class Builder<T> {

        private Integer port = 8000;

        private Integer linesPerRequest = 1000;

        private Integer ingestQueueCapacity = 1000;

        private Long enqueueTimeout = 5L;

        private String serverURL = "http://127.0.0.1:8086";

        private String username = "root";

        private String password = "root";

        private Integer connectTimeout = 10;

        private Integer writeTimeout = 10;

        private Integer readTimeout = 10;

        private Boolean retryOnConnectionFailure = true;

        private DebeziumDeserializationSchema<T> deserializer;

        private String inlongMetric;

        private String inlongAudit;

        public Builder() {
        }

        /*
        TCP port on which the split reader's HTTP server is running on.
         */
        public Builder<T> port(int port) {
            this.port = port;
            return this;
        }

        public Builder<T> linesPerRequest(int linesPerRequest) {
            this.linesPerRequest = linesPerRequest;
            return this;
        }

        public Builder<T> ingestQueueCapacity(int ingestQueueCapacity) {
            this.ingestQueueCapacity = ingestQueueCapacity;
            return this;
        }

        public Builder<T> enqueueTimeout(long timeout) {
            this.enqueueTimeout = timeout;
            return this;
        }

        public Builder<T> serverURL(String serverURL) {
            this.serverURL = serverURL;
            return this;
        }

        public Builder<T> username(String username) {
            this.username = username;
            return this;
        }

        public Builder<T> password(String password) {
            this.password = password;
            return this;
        }

        public Builder<T> connectTimeout(int connectTimeout) {
            this.connectTimeout = connectTimeout;
            return this;
        }

        public Builder<T> writeTimeout(int writeTimeout) {
            this.writeTimeout = writeTimeout;
            return this;
        }

        public Builder<T> readTimeout(int readTimeout) {
            this.readTimeout = readTimeout;
            return this;
        }

        public Builder<T> retryOnConnectionFailure(boolean retryOnConnectionFailure) {
            this.retryOnConnectionFailure = retryOnConnectionFailure;
            return this;
        }

        /**
         * The deserializer used to convert from consumed {@link
         * org.apache.kafka.connect.source.SourceRecord}.
         */
        public Builder<T> deserializer(DebeziumDeserializationSchema<T> deserializer) {
            this.deserializer = deserializer;
            return this;
        }

        public Builder<T> inlongMetric(String inlongMetric) {
            this.inlongMetric = inlongMetric;
            return this;
        }

        public Builder<T> inlongAudit(String inlongAudit) {
            this.inlongAudit = inlongAudit;
            return this;
        }

        public DebeziumSourceFunction<T> build() {
            Properties props = new Properties();
//            props.setProperty("connector.class", InfluxDbConnector.class.getCanonicalName());

            props.setProperty("source.influxDB.port", String.valueOf(port));
            props.setProperty("source.influxDB.timeout.enqueue", String.valueOf(enqueueTimeout));
            props.setProperty("source.influxDB.queue_capacity.ingest", String.valueOf(ingestQueueCapacity));
            props.setProperty("source.influxDB.limit.lines_per_request", String.valueOf(linesPerRequest));
            props.setProperty("source.influxDB.limit.serverURL", serverURL);
            props.setProperty("source.influxDB.limit.username", username);
            props.setProperty("source.influxDB.limit.password", password);
            props.setProperty("source.influxDB.limit.connectTimeout", String.valueOf(connectTimeout));
            props.setProperty("source.influxDB.limit.writeTimeout", String.valueOf(writeTimeout));
            props.setProperty("source.influxDB.limit.readTimeout", String.valueOf(readTimeout));
            props.setProperty("source.influxDB.limit.retryOnConnectionFailure",
                String.valueOf(retryOnConnectionFailure));
            DebeziumOffset specificOffset = null;
            return new DebeziumSourceFunction<>(
                    deserializer, props, specificOffset, Validator.getDefaultValidator(), inlongMetric, inlongAudit);
        }
    }

}
