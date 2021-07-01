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

package org.apache.inlong.sort.configuration;

import static org.apache.inlong.sort.configuration.ConfigOptions.key;

import java.time.Duration;

public class Constants {
    public static final long UNKNOWN_DATAFLOW_ID = -1L;

    public static final String SOURCE_TYPE_TUBE = "tubemq";

    public static final String SOURCE_TYPE_PULSAR = "pulsar";

    public static final String SINK_TYPE_CLICKHOUSE = "clickhouse";

    public static final String SINK_TYPE_HIVE = "hive";

    // ------------------------------------------------------------------------
    //  Operator uid
    // ------------------------------------------------------------------------
    public static final String SOURCE_UID = "source_uid";

    public static final String DESERIALIZATION_SCHEMA_UID = "deserialization_schema_uid";

    public static final String SINK_UID = "sink_uid";

    /**
     * It uses dt as the built-in data time field name. It's a work-around solution and should be replaced by later.
     */
    public static final String DATA_TIME_FIELD = "dt";

    // ------------------------------------------------------------------------
    //  Common configs
    // ------------------------------------------------------------------------
    /**
     * The ID of the cluster, used to separate multiple clusters.
     */
    public static final ConfigOption<String> CLUSTER_ID =
            key("cluster-id")
                    .noDefaultValue()
                    .withDescription("The ID of the cluster, used to separate multiple clusters.");

    /**
     * The ZooKeeper quorum to use.
     */
    public static final ConfigOption<String> ZOOKEEPER_QUORUM =
            key("zookeeper.quorum")
                    .noDefaultValue()
                    .withDescription("The ZooKeeper quorum to use");

    /**
     * The root path under which it stores its entries in ZooKeeper.
     */
    public static final ConfigOption<String> ZOOKEEPER_ROOT =
            key("zookeeper.path.root")
                    .defaultValue("/inlong-sort")
                    .withDescription("The root path in ZooKeeper.");

    public static final ConfigOption<Integer> ETL_RECORD_SERIALIZATION_BUFFER_SIZE =
            key("etl.record.serialization.buffer.size")
                    .defaultValue(1024);

    public static final ConfigOption<String> SOURCE_TYPE =
            key("source.type")
                    .noDefaultValue()
                    .withDescription("The type of source, currently only 'tubemq' is supported");

    public static final ConfigOption<String> SINK_TYPE =
            key("sink.type")
                    .noDefaultValue()
                    .withDescription("The type of sink, currently only 'clickhouse' and 'iceberg' are supported");

    // ------------------------------------------------------------------------
    //  Operator parallelism configs
    // ------------------------------------------------------------------------
    public static final ConfigOption<Integer> SOURCE_PARALLELISM =
            key("source.parallelism")
                    .defaultValue(1);

    public static final ConfigOption<Integer> DESERIALIZATION_PARALLELISM =
            key("deserialization.parallelism")
                    .defaultValue(1);

    public static final ConfigOption<Integer> SINK_PARALLELISM =
            key("sink.parallelism")
                    .defaultValue(1);

    public static final ConfigOption<Integer> COMMITTER_PARALLELISM =
            key("committer.parallelism")
                    .defaultValue(1);

    // ------------------------------------------------------------------------
    //  TubeMQ source configs
    // ------------------------------------------------------------------------
    public static final ConfigOption<String> TUBE_MASTER_ADDRESS =
            key("tubemq.master.address")
                    .noDefaultValue()
                    .withDescription("The address of tubeMQ master.");

    public static final ConfigOption<String> TUBE_SESSION_KEY =
            key("tubemq.session.key")
                    .defaultValue("inlong-sort")
                    .withDescription("The session key of tubeMQ consumer.");

    public static final ConfigOption<Boolean> TUBE_BOOTSTRAP_FROM_MAX =
            key("tubemq.bootstrap.from.max")
                    .defaultValue(false)
                    .withDescription("Consume tubeMQ from max offset.");

    public static final ConfigOption<String> TUBE_MESSAGE_NOT_FOUND_WAIT_PERIOD =
            key("tubemq.message.not.found.wait.period")
                    .defaultValue("350ms")
                    .withDescription("The time of waiting period if "
                            + "tubeMQ broker return message not found.");

    public static final ConfigOption<Long> TUBE_SUBSCRIBE_RETRY_TIMEOUT =
            key("tubemq.subscribe.retry.timeout")
                    .defaultValue(300000L)
                    .withDescription("The time of subscribing tubeMQ timeout, in millisecond");

    public static final ConfigOption<Integer> SOURCE_EVENT_QUEUE_CAPACITY =
            key("source.event.queue.capacity")
                    .defaultValue(1024);

    // ------------------------------------------------------------------------
    //  ZooKeeper Client Settings
    // ------------------------------------------------------------------------

    public static final ConfigOption<Integer> ZOOKEEPER_SESSION_TIMEOUT =
            key("zookeeper.client.session-timeout")
                    .defaultValue(60000)
                    .withDescription(
                            "Defines the session timeout for the ZooKeeper session in ms.");

    public static final ConfigOption<Integer> ZOOKEEPER_CONNECTION_TIMEOUT =
            key("zookeeper.client.connection-timeout")
                    .defaultValue(15000)
                    .withDescription("Defines the connection timeout for ZooKeeper in ms.");

    public static final ConfigOption<Integer> ZOOKEEPER_RETRY_WAIT =
            key("zookeeper.client.retry-wait")
                    .defaultValue(5000)
                    .withDescription("Defines the pause between consecutive retries in ms.");

    public static final ConfigOption<Integer> ZOOKEEPER_MAX_RETRY_ATTEMPTS =
            key("zookeeper.client.max-retry-attempts")
                    .defaultValue(3)
                    .withDescription(
                            "Defines the number of connection retries before the client gives up.");

    public static final ConfigOption<String> ZOOKEEPER_CLIENT_ACL =
            key("zookeeper.client.acl")
                    .defaultValue("open")
                    .withDescription(
                            "Defines the ACL (open|creator) to be configured on ZK node. The "
                                    + "configuration value can be"
                                    + " set to “creator” if the ZooKeeper server configuration "
                                    + "has the “authProvider” property mapped to use"
                                    + " SASLAuthenticationProvider and the cluster is configured "
                                    + "to run in secure mode (Kerberos).");

    public static final ConfigOption<Boolean> ZOOKEEPER_SASL_DISABLE =
            key("zookeeper.sasl.disable")
                    .defaultValue(false);

    // ------------------------------------------------------------------------
    //  Hive sink related configs
    // ------------------------------------------------------------------------
    public static final ConfigOption<Long> HIKARICP_IDLE_TIMEOUT_MS =
            key("hikaricp.idle.timeout.ms")
                    .defaultValue(10000L);

    public static final ConfigOption<Long> HIKARICP_CONNECTION_TIMEOUT_MS =
            key("hikaricp.connection.timeout.ms")
                    .defaultValue(30000L);

    public static final ConfigOption<Integer> HIKARICP_MAXIMUM_POOL_SIZE =
            key("hikaricp.maximum.pool.size")
                    .defaultValue(10);

    public static final ConfigOption<Long> HIKARICP_MAXIMUM_LIFETIME_MS =
            key("hikaricp.maximum.lifetime.ms")
                    .defaultValue(30000L);

    // ------------------------------------------------------------------------
    //  Hive sink related configs (initial version, refactor later)
    // ------------------------------------------------------------------------

    public static final ConfigOption<Integer> SINK_HIVE_COMMITTED_PARTITIONS_CACHE_SIZE =
            key("sink.hive.committed.partitions.cache.size")
                    .defaultValue(1024);

    public static final ConfigOption<Long> SINK_HIVE_ROLLING_POLICY_FILE_SIZE =
            key("sink.hive.rolling-policy.file-size")
                    .defaultValue(128L << 20)
                    .withDescription("The maximum part file size before rolling.");

    public static final ConfigOption<Long> SINK_HIVE_ROLLING_POLICY_ROLLOVER_INTERVAL =
            key("sink.hive.rolling-policy.rollover-interval")
                    .defaultValue(Duration.ofMinutes(30).toMillis())
                    .withDescription("The maximum time duration a part file can stay open before rolling"
                            + " (by default long enough to avoid too many small files). The frequency at which"
                            + " this is checked is controlled by the 'sink.rolling-policy.check-interval' option.");

    public static final ConfigOption<Long> SINK_HIVE_ROLLING_POLICY_CHECK_INTERVAL =
            key("sink.hive.rolling-policy.check-interval")
                    .defaultValue(Duration.ofMinutes(1).toMillis())
                    .withDescription("The interval for checking time based rolling policies. "
                            + "This controls the frequency to check whether a part file should rollover based on"
                            + " 'sink.rolling-policy.rollover-interval'.");

}
