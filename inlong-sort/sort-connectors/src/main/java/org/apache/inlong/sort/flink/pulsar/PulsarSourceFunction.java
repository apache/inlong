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

package org.apache.inlong.sort.flink.pulsar;

import static org.apache.flink.util.TimeUtils.parseDuration;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.OperatorStateStore;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.runtime.util.ExecutorThreadFactory;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.util.Preconditions;
import org.apache.inlong.sort.flink.pulsar.PulsarDeserializationSchema.DeserializationResult;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Reader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Pulsar source (consumer) which receives messages from topics.
 *
 * <p>
 * Copied from Flink-connector with a bit of change. We need whole Message of Pulsar to deserializing. So we fork this
 * pulsar source function until our requirements have been satisfied in Flink-connector.
 * </p>
 */
public class PulsarSourceFunction<T>
        extends RichParallelSourceFunction<T>
        implements ResultTypeQueryable<T>, CheckpointedFunction {

    private static final long serialVersionUID = 1L;

    private static final Logger LOG =
            LoggerFactory.getLogger(PulsarSourceFunction.class);

    private static final String OFFSETS_STATE_NAME = "pulsar-offset-state";
    private static final String READER_THREAD_NAME_PREFIX = "pulsar-reader-";
    private static final String COMMITTER_THREAD_NAME = "pulsar-committer";
    private static final String SUBSCRIPTION_NAME_OPTION_KEY = "subscriptionName";

    /**
     * The pulsar admin console address.
     */
    private final String adminUrl;

    /**
     * The pulsar service provider address.
     */
    private final String serviceUrl;

    /**
     * The pulsar topic name.
     */
    private final String topic;

    /**
     * The pulsar consumerGroup if assigned.
     */
    private final String consumerGroup;

    /**
     * The authentication to create client, null if not exists;
     */
    private final String authentication;

    /**
     * The configuration for the pulsar consumer.
     */
    private final Configuration configuration;

    /**
     * The deserializer for records.
     */
    private final PulsarDeserializationSchema<T> deserializationSchema;

    /**
     * The pulsar client.
     */
    private transient PulsarClient client;

    /**
     * The pulsar admin.
     */
    private transient PulsarAdmin admin;

    /**
     * The readers for assigned partitions.
     *
     * <p>NOTE: The map can only be accessed in the task thread.</p>
     */
    private transient Map<String, PulsarPartitionReader> partitionReaders;

    /**
     * Accessor for state in the operator state backend.
     */
    private transient ListState<Tuple2<String, MessageId>> unionOffsetState;

    /**
     * The restored offsets of queues which are restored from flink state.
     *
     * <p>NOTE: The map can only be accessed in the task thread.</p>
     */
    private transient Map<String, MessageId> restoredOffsets;

    /**
     * The current offsets of queues which are stored in flink state
     * once a checkpoint is triggered.
     *
     * <p>NOTE: The map should be guarded by the checkpoint lock.</p>
     */
    private transient Map<String, MessageId> currentOffsets;

    /**
     * Flag indicating whether the consumer is still running.
     **/
    private transient volatile boolean isRunning;

    /**
     * The timeout for the reader's pulling.
     */
    private transient Duration receiveTimeout;

    /**
     * The last committed offsets for topic partition.
     */
    private transient Map<String, MessageId> lastCommittedOffsets;

    /**
     * The scheduler for auto commit tasks.
     */
    private transient ScheduledExecutorService executor;

    public PulsarSourceFunction(
            String adminUrl,
            String serviceUrl,
            String topic,
            String consumerGroup,
            String authentication,
            PulsarDeserializationSchema<T> deserializationSchema,
            Configuration configuration
    ) {
        Preconditions.checkNotNull(topic,
                "The topic must not be null.");
        Preconditions.checkNotNull(consumerGroup,
                "The consumer group must not be null.");
        Preconditions.checkNotNull(serviceUrl,
                "The service url must not be null.");
        Preconditions.checkNotNull(adminUrl,
                "The admin url must not be null.");
        Preconditions.checkNotNull(deserializationSchema,
                "The deserialization schema must not be null.");
        Preconditions.checkNotNull(configuration,
                "The configuration must not be null.");

        this.adminUrl = adminUrl;
        this.serviceUrl = serviceUrl;
        this.topic = topic;
        this.consumerGroup = consumerGroup;
        this.configuration = configuration;
        this.authentication = authentication;
        this.deserializationSchema = deserializationSchema;
    }

    @Override
    public TypeInformation<T> getProducedType() {
        return deserializationSchema.getProducedType();
    }

    @Override
    public void open(Configuration parameters) throws Exception {

        super.open(parameters);

        LOG.info("Opening the pulsar source: adminUrl: {}, serviceUrl: {}, topic: {}, consumer group: {}.",
                adminUrl, serviceUrl, topic, consumerGroup);

        LOG.info("Pulsar source configuration: {}", configuration);

        admin = PulsarUtils.createAdmin(adminUrl, authentication);
        client = PulsarUtils.createClient(serviceUrl, authentication, configuration);
        partitionReaders = new HashMap<>();
        currentOffsets = new HashMap<>();
        lastCommittedOffsets = new HashMap<>();

        receiveTimeout =
                parseDuration(configuration.getString(
                        PulsarOptions.CONSUMER_RECEIVE_TIMEOUT));

        isRunning = true;
    }

    @Override
    public void run(SourceContext<T> context) throws Exception {

        int numTasks = getRuntimeContext().getNumberOfParallelSubtasks();
        int taskIndex = getRuntimeContext().getIndexOfThisSubtask();

        boolean isAutoCommitEnabled =
                configuration.getBoolean(PulsarOptions.CONSUMER_ENABLE_AUTO_COMMIT);
        if (isAutoCommitEnabled) {
            Duration autoCommitInterval =
                    parseDuration(configuration.getString(
                            PulsarOptions.CONSUMER_AUTO_COMMIT_INTERVAL));

            ThreadFactory threadFactory =
                    new ExecutorThreadFactory(COMMITTER_THREAD_NAME);
            executor =
                    Executors.newScheduledThreadPool(1, threadFactory);
            executor.scheduleWithFixedDelay(
                    new AutoCommitTask(context),
                    autoCommitInterval.toMillis(),
                    autoCommitInterval.toMillis(),
                    TimeUnit.MILLISECONDS
            );
        }

        Duration checkPartitionInterval =
                parseDuration(configuration.getString(
                        PulsarOptions.CONSUMER_CHECK_PARTITION_INTERVAL));
        Duration maxIdleTime =
                parseDuration(configuration.getString(
                        PulsarOptions.CONSUMER_MAX_IDLE_TIME));

        Instant lastEmitInstant = Instant.MIN;

        // set subscriptionName
        HashMap<String, Object> readerConf = new HashMap<>();
        readerConf.putIfAbsent(SUBSCRIPTION_NAME_OPTION_KEY, consumerGroup);

        while (isRunning) {

            // Discover new partitions
            Set<String> topicPartitions = PulsarUtils.getTopicPartitions(admin, topic);

            Set<String> assignedTopicPartitions =
                    topicPartitions.stream()
                            .filter(p -> (((((long) p.hashCode()) & 0xffffffffL) % numTasks) == taskIndex))
                            .collect(Collectors.toCollection(TreeSet::new));

            for (String topicPartition : assignedTopicPartitions) {
                if (!partitionReaders.containsKey(topicPartition)) {

                    MessageId bootstrapOffset =
                            getBootstrapOffset(topicPartition);

                    LOG.info("Discover a new topic partition {} starting from offset {}.", topicPartition,
                            bootstrapOffset);

                    Reader<byte[]> reader =
                            PulsarUtils.createReader(
                                    client,
                                    topicPartition,
                                    bootstrapOffset,
                                    configuration,
                                    readerConf
                            );

                    PulsarPartitionReader partitionReader =
                            new PulsarPartitionReader(
                                    context,
                                    topicPartition,
                                    reader
                            );
                    partitionReaders.put(topicPartition, partitionReader);

                    partitionReader.start();
                }
            }

            // Check the status of each partition reader
            for (PulsarPartitionReader partitionReader :
                    partitionReaders.values()) {

                Throwable partitionThrowable = partitionReader.getThrowable();
                if (partitionThrowable != null) {
                    throw new IOException("Could not properly read messages from topic partition "
                            + partitionReader.getTopicPartition() + ".", partitionThrowable);
                }

                Instant partitionLastEmitInstant =
                        partitionReader.getLastEmitInstant();
                if (partitionLastEmitInstant.compareTo(lastEmitInstant) > 0) {
                    lastEmitInstant = partitionLastEmitInstant;
                }
            }

            // Mark the consumer as idle if the idle time exceeds the limit.
            Duration idleTime =
                    Duration.between(lastEmitInstant, Instant.now());
            if (idleTime.compareTo(maxIdleTime) > 0) {
                context.markAsTemporarilyIdle();
            }

            //noinspection BusyWait
            Thread.sleep(checkPartitionInterval.toMillis());
        }

        // Waits until all readers exit
        for (
                PulsarPartitionReader partitionReader :
                partitionReaders.values()
        ) {
            try {
                partitionReader.join();
                partitionReader.close();
            } catch (Throwable t) {
                LOG.warn("Could not properly close the reader for topic partition {}.",
                        partitionReader.getTopicPartition(), t);
            }
        }
    }

    @Override
    public void initializeState(
            FunctionInitializationContext context
    ) throws Exception {
        OperatorStateStore stateStore = context.getOperatorStateStore();
        unionOffsetState = stateStore.getUnionListState(
                new ListStateDescriptor<>(
                        OFFSETS_STATE_NAME,
                        TypeInformation.of(new TypeHint<Tuple2<String, MessageId>>() {
                        })
                )
        );

        restoredOffsets = new TreeMap<>();
        if (context.isRestored()) {
            for (Tuple2<String, MessageId> restoredOffset :
                    unionOffsetState.get()) {
                String topicPartition = restoredOffset.f0;
                MessageId offset = restoredOffset.f1;

                restoredOffsets.put(topicPartition, offset);
            }

            LOG.info("The pulsar source successfully restores the offsets ({}).", restoredOffsets);
        } else {
            LOG.info("The pulsar source does not restore from any checkpoint.");
        }
    }

    @Override
    public void snapshotState(
            FunctionSnapshotContext context
    ) throws Exception {

        unionOffsetState.clear();
        for (Map.Entry<String, MessageId> entry : currentOffsets.entrySet()) {
            unionOffsetState.add(Tuple2.of(entry.getKey(), entry.getValue()));
        }

        LOG.info("Successfully save the offsets {} in checkpoint {}.",
                currentOffsets, context.getCheckpointId());
    }

    @Override
    public void cancel() {
        isRunning = false;
    }

    @Override
    public void close() throws Exception {

        cancel();

        if (executor != null) {
            try {
                executor.shutdown();
            } catch (Throwable t) {
                LOG.warn("Could not properly shutdown the executor.", t);
            }
        }

        if (client != null) {
            try {
                client.close();
            } catch (Throwable t) {
                LOG.warn("Could not properly close the pulsar client.", t);
            }
        }

        if (admin != null) {
            try {
                admin.close();
            } catch (Throwable t) {
                LOG.warn("Could not properly close the pulsar admin.", t);
            }
        }

        super.close();
    }

    private MessageId getBootstrapOffset(
            String topicPartition
    ) throws PulsarAdminException {
        MessageId restoredOffset = restoredOffsets.get(topicPartition);
        if (restoredOffset != null) {
            return restoredOffset;
        }

        String bootstrapMode = configuration.getString(
                PulsarOptions.CONSUMER_BOOTSTRAP_MODE);

        switch (bootstrapMode) {
            case PulsarOptions.BOOTSTRAP_MODE_EARLIEST:
                return MessageId.earliest;
            case PulsarOptions.BOOTSTRAP_MODE_LATEST:
                return MessageId.latest;
            default:
                return PulsarUtils.getTopicPartitionOffset(admin, topicPartition, consumerGroup);
        }
    }

    private class PulsarPartitionReader extends Thread implements AutoCloseable {

        private final SourceContext<T> context;

        private final String topicPartition;

        private final Reader<byte[]> reader;

        private volatile Throwable throwable;

        private volatile Instant lastEmitInstant;

        PulsarPartitionReader(
                SourceContext<T> context,
                String topicPartition,
                Reader<byte[]> reader
        ) {
            super(READER_THREAD_NAME_PREFIX + topicPartition);

            this.context = context;
            this.topicPartition = topicPartition;
            this.reader = reader;
            this.throwable = null;
            this.lastEmitInstant = Instant.MIN;
        }

        String getTopicPartition() {
            return topicPartition;
        }

        Throwable getThrowable() {
            return throwable;
        }

        Instant getLastEmitInstant() {
            return lastEmitInstant;
        }

        @Override
        public void run() {
            try {
                LOG.info("The reader for {} starts.", topicPartition);

                while (isRunning) {
                    //noinspection rawtypes
                    Message message =
                            reader.readNext(
                                    (int) receiveTimeout.toMillis(),
                                    TimeUnit.MILLISECONDS
                            );

                    if (message != null) {
                        String topicPartition = message.getTopicName();
                        MessageId offset = message.getMessageId();

                        DeserializationResult<T> result = deserializationSchema.deserialize(message);

                        synchronized (context.getCheckpointLock()) {
                            context.collect(result.getRecord());
                            currentOffsets.put(topicPartition, offset);
                        }

                        lastEmitInstant = Instant.now();
                    } else {
                        LOG.debug("The reader read is null, maybe it reached the end of the topic:{}", topicPartition);
                    }
                }
            } catch (Throwable t) {
                LOG.error("Error occurred of partition reader", t);
                throwable = t;
            } finally {
                LOG.info("The reader for {} exits.", topicPartition);
            }
        }

        @Override
        public void close() throws Exception {
            reader.close();
        }
    }

    private class AutoCommitTask implements Runnable {

        private final SourceContext<T> context;

        private AutoCommitTask(SourceContext<T> context) {
            this.context = context;
        }

        @Override
        public void run() {
            try {

                if (!isRunning) {
                    return;
                }

                Map<String, MessageId> currentOffsetsCopy;
                synchronized (context.getCheckpointLock()) {
                    currentOffsetsCopy = new HashMap<>(currentOffsets);
                }

                for (
                        Map.Entry<String, MessageId> entry :
                        currentOffsetsCopy.entrySet()
                ) {
                    String topicPartition = entry.getKey();
                    MessageId currentOffset =
                            currentOffsetsCopy.get(topicPartition);

                    MessageId lastCommittedOffset =
                            lastCommittedOffsets.get(topicPartition);

                    // Skips the committing if the offset is not changed.
                    if (Objects.equals(currentOffset, lastCommittedOffset)) {
                        continue;
                    }

                    PulsarUtils.commitTopicPartitionOffset(
                            admin,
                            topicPartition,
                            currentOffset,
                            consumerGroup
                    );

                    lastCommittedOffsets.put(topicPartition, currentOffset);
                }
            } catch (Throwable throwable) {
                LOG.warn("Could not properly commit the offset.", throwable);
            }
        }
    }
}
