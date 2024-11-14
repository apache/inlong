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

import org.apache.inlong.agent.utils.AgentUtils;

import io.netty.util.NettyRuntime;
import io.netty.util.internal.SystemPropertyUtil;

/**
 * Configuration constants of agent.
 */
public class AgentConstants {

    public static final String AGENT_HOME = "agent.home";
    public static final String DEFAULT_AGENT_HOME = System.getProperty("agent.home");
    public static final String AGENT_ROCKS_DB_PATH = "agent.rocks.db.path";
    public static final String DEFAULT_AGENT_ROCKS_DB_PATH = ".rocksdb";
    public static final String AGENT_STORE_PATH_TASK = ".localdb/task";
    public static final String AGENT_STORE_PATH_INSTANCE = ".localdb/instance";
    public static final String AGENT_STORE_PATH_OFFSET = ".localdb/offset";
    public static final String AGENT_UNIQ_ID = "agent.uniq.id";
    // default is empty.
    public static final String AGENT_FETCHER_CLASSNAME = "agent.fetcher.classname";
    public static final String AGENT_CONF_PARENT = "agent.conf.parent";
    public static final String DEFAULT_AGENT_CONF_PARENT = "conf";
    public static final String CHANNEL_MEMORY_CAPACITY = "channel.memory.capacity";
    public static final int DEFAULT_CHANNEL_MEMORY_CAPACITY = 2000;
    public static final String JOB_NUMBER_LIMIT = "job.number.limit";
    public static final int DEFAULT_JOB_NUMBER_LIMIT = 15;
    public static final String AGENT_LOCAL_IP = "agent.local.ip";
    public static final String DEFAULT_LOCAL_IP = "127.0.0.1";
    public static final String DEFAULT_LOCAL_HOST = "localhost";
    public static final String AGENT_STORE_CLASSNAME = "agent.store.classname";
    public static final String DEFAULT_AGENT_STORE_CLASSNAME = "org.apache.inlong.agent.plugin.store.RocksDBStoreImpl";

    // default use local ip as uniq id for agent.
    public static final String DEFAULT_AGENT_UNIQ_ID = AgentUtils.getLocalIp();
    public static final String CUSTOM_FIXED_IP = "agent.custom.fixed.ip";

    public static final String AGENT_CLUSTER_NAME = "agent.cluster.name";
    public static final String AGENT_CLUSTER_TAG = "agent.cluster.tag";
    public static final String AGENT_CLUSTER_IN_CHARGES = "agent.cluster.inCharges";
    public static final String AGENT_INSTALL_PLATFORM = "agent.install.platform";

    public static final String AGENT_LOCAL_UUID = "agent.local.uuid";
    public static final String AGENT_LOCAL_UUID_OPEN = "agent.local.uuid.open";
    public static final Boolean DEFAULT_AGENT_LOCAL_UUID_OPEN = false;
    public static final String AGENT_NODE_GROUP = "agent.node.group";

    public static final String PROMETHEUS_EXPORTER_PORT = "agent.prometheus.exporter.port";
    public static final int DEFAULT_PROMETHEUS_EXPORTER_PORT = 8080;

    public static final String AUDIT_ENABLE = "audit.enable";
    public static final boolean DEFAULT_AUDIT_ENABLE = true;
    public static final String AUDIT_PROXY_ADDRESS = "audit.proxy.address";

    public static final String AGENT_HISTORY_PATH = "agent.history.path";
    public static final String DEFAULT_AGENT_HISTORY_PATH = ".history";

    public static final String AGENT_ENABLE_OOM_EXIT = "agent.enable.oom.exit";
    public static final boolean DEFAULT_ENABLE_OOM_EXIT = false;

    // pulsar sink config
    public static final String PULSAR_CLIENT_IO_TREHAD_NUM = "agent.sink.pulsar.client.io.thread.num";
    public static final int DEFAULT_PULSAR_CLIENT_IO_TREHAD_NUM = Math.max(1,
            SystemPropertyUtil.getInt("io.netty.eventLoopThreads", NettyRuntime.availableProcessors() * 2));

    public static final String PULSAR_CONNECTION_PRE_BROKER = "agent.sink.pulsar.connection.pre.broker";
    public static final int DEFAULT_PULSAR_CONNECTION_PRE_BROKER = 1;

    public static final String PULSAR_CLIENT_TIMEOUT_SECOND = "agent.sink.pulsar.send.timeout.second";
    public static final int DEFAULT_PULSAR_CLIENT_TIMEOUT_SECOND = 30;

    public static final String PULSAR_CLIENT_ENABLE_BATCH = "agent.sink.pullsar.enable.batch";
    public static final boolean DEFAULT_PULSAR_CLIENT_ENABLE_BATCH = true;

    public static final String PULSAR_CLIENT_BLOCK_IF_QUEUE_FULL = "agent.sink.pulsar.block.if.queue.full";
    public static final boolean DEFAULT_BLOCK_IF_QUEUE_FULL = true;

    public static final String PULSAR_CLIENT_MAX_PENDING_MESSAGES = "agent.sink.pulsar.max.pending.messages";
    public static final int DEFAULT_MAX_PENDING_MESSAGES = 10000;

    public static final String PULSAR_CLIENT_MAX_PENDING_MESSAGES_ACROSS_PARTITION =
            "agent.sink.pulsar.max.messages.across.partition";
    public static final int DEFAULT_MAX_PENDING_MESSAGES_ACROSS_PARTITION = 500000;

    public static final String PULSAR_CLIENT_MAX_BATCH_BYTES = "agent.sink.pulsar.max.batch.bytes";
    public static final int DEFAULT_MAX_BATCH_BYTES = 128 * 1024;

    public static final String PULSAR_CLIENT_MAX_BATCH_MESSAGES = "agent.sink.pulsar.max.batch.messages";
    public static final int DEFAULT_MAX_BATCH_MESSAGES = 1000;

    public static final String PULSAR_CLIENT_MAX_BATCH_INTERVAL_MILLIS = "agent.sink.pulsar.max.batch.interval.millis";
    public static final int DEFAULT_MAX_BATCH_INTERVAL_MILLIS = 1;

    public static final String PULSAR_CLIENT_COMPRESSION_TYPE = "agent.sink.pulsar.compression.type";
    public static final String DEFAULT_COMPRESSION_TYPE = "NONE";

    public static final String PULSAR_CLIENT_PRODUCER_NUM = "agent.sink.pulsar.producer.num";
    public static final String KAFKA_SINK_PRODUCER_NUM = "agent.sink.kafka.producer.num";
    public static final int DEFAULT_PRODUCER_NUM = 3;

    public static final String PULSAR_CLIENT_ENABLE_ASYNC_SEND = "agent.sink.pulsar.enbale.async.send";
    public static final String KAFKA_PRODUCER_ENABLE_ASYNC_SEND = "agent.sink.kafka.enbale.async.send";
    public static final boolean DEFAULT_ENABLE_ASYNC_SEND = true;

    public static final String PULSAR_SINK_SEND_QUEUE_SIZE = "agent.sink.pulsar.send.queue.size";
    public static final String KAFKA_SINK_SEND_QUEUE_SIZE = "agent.sink.kafka.send.queue.size";
    public static final int DEFAULT_SEND_QUEUE_SIZE = 20000;

    public static final String DEFAULT_KAFKA_SINK_SEND_ACKS = "1";
    public static final long DEFAULT_KAFKA_SINK_SYNC_SEND_TIMEOUT_MS = 3000;

    public static final String DEFAULT_KAFKA_SINK_SEND_COMPRESSION_TYPE = "none";

    public static final String DEFAULT_KAFKA_SINK_SEND_KEY_SERIALIZER =
            "org.apache.kafka.common.serialization.StringSerializer";

    public static final String DEFAULT_KAFKA_SINK_SEND_VALUE_SERIALIZER =
            "org.apache.kafka.common.serialization.ByteArraySerializer";
}
