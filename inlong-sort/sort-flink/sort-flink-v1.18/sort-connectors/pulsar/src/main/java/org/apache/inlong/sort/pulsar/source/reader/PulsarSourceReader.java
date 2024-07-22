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

package org.apache.inlong.sort.pulsar.source.reader;

import org.apache.inlong.sort.pulsar.table.source.PulsarTableDeserializationSchema;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.connector.source.ReaderOutput;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.SourceReaderBase;
import org.apache.flink.connector.base.source.reader.splitreader.SplitReader;
import org.apache.flink.connector.base.source.reader.synchronization.FutureCompletingBlockingQueue;
import org.apache.flink.connector.pulsar.common.crypto.PulsarCrypto;
import org.apache.flink.connector.pulsar.common.schema.BytesSchema;
import org.apache.flink.connector.pulsar.common.schema.PulsarSchema;
import org.apache.flink.connector.pulsar.source.config.SourceConfiguration;
import org.apache.flink.connector.pulsar.source.enumerator.topic.TopicPartition;
import org.apache.flink.connector.pulsar.source.reader.PulsarPartitionSplitReader;
import org.apache.flink.connector.pulsar.source.reader.PulsarRecordEmitter;
import org.apache.flink.connector.pulsar.source.reader.PulsarSourceFetcherManager;
import org.apache.flink.connector.pulsar.source.reader.deserializer.PulsarDeserializationSchema;
import org.apache.flink.connector.pulsar.source.reader.deserializer.PulsarDeserializationSchemaInitializationContext;
import org.apache.flink.connector.pulsar.source.reader.deserializer.PulsarSchemaWrapper;
import org.apache.flink.connector.pulsar.source.split.PulsarPartitionSplit;
import org.apache.flink.connector.pulsar.source.split.PulsarPartitionSplitState;
import org.apache.flink.core.io.InputStatus;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import static org.apache.flink.connector.pulsar.common.config.PulsarClientFactory.createClient;

/**
 * The source reader for pulsar subscription Failover and Exclusive, which consumes the ordered
 * messages.
 *
 * @param <OUT> The output message type for flink.
 * Modify from  {@link org.apache.flink.connector.pulsar.source.reader.PulsarSourceReader}
 */
@Internal
public class PulsarSourceReader<OUT>
        extends
            SourceReaderBase<Message<byte[]>, OUT, PulsarPartitionSplit, PulsarPartitionSplitState> {

    private static final Logger LOG = LoggerFactory.getLogger(PulsarSourceReader.class);

    private final SourceConfiguration sourceConfiguration;
    private final PulsarClient pulsarClient;
    @VisibleForTesting
    final SortedMap<Long, Map<TopicPartition, MessageId>> cursorsToCommit;
    private final ConcurrentMap<TopicPartition, MessageId> cursorsOfFinishedSplits;
    private final AtomicReference<Throwable> cursorCommitThrowable;
    private final PulsarDeserializationSchema<OUT> deserializationSchema;
    private ScheduledExecutorService cursorScheduler;

    private PulsarSourceReader(
            FutureCompletingBlockingQueue<RecordsWithSplitIds<Message<byte[]>>> elementsQueue,
            PulsarSourceFetcherManager fetcherManager,
            PulsarDeserializationSchema<OUT> deserializationSchema,
            SourceConfiguration sourceConfiguration,
            PulsarClient pulsarClient,
            SourceReaderContext context) {
        super(
                elementsQueue,
                fetcherManager,
                new PulsarRecordEmitter<>(deserializationSchema),
                sourceConfiguration,
                context);

        this.deserializationSchema = deserializationSchema;
        this.sourceConfiguration = sourceConfiguration;
        this.pulsarClient = pulsarClient;

        this.cursorsToCommit = Collections.synchronizedSortedMap(new TreeMap<>());
        this.cursorsOfFinishedSplits = new ConcurrentHashMap<>();
        this.cursorCommitThrowable = new AtomicReference<>();
    }

    @Override
    public void start() {
        super.start();
        if (sourceConfiguration.isEnableAutoAcknowledgeMessage()) {
            this.cursorScheduler = Executors.newSingleThreadScheduledExecutor();

            // Auto commit cursor, this could be enabled when checkpoint is also enabled.
            cursorScheduler.scheduleAtFixedRate(
                    this::cumulativeAcknowledgmentMessage,
                    sourceConfiguration.getMaxFetchTime().toMillis(),
                    sourceConfiguration.getAutoCommitCursorInterval(),
                    TimeUnit.MILLISECONDS);
        }
    }

    @Override
    public InputStatus pollNext(ReaderOutput<OUT> output) throws Exception {
        Throwable cause = cursorCommitThrowable.get();
        if (cause != null) {
            throw new FlinkRuntimeException("An error occurred in acknowledge message.", cause);
        }

        return super.pollNext(output);
    }

    @Override
    protected void onSplitFinished(Map<String, PulsarPartitionSplitState> finishedSplitIds) {
        // Close all the finished splits.
        for (String splitId : finishedSplitIds.keySet()) {
            ((PulsarSourceFetcherManager) splitFetcherManager).closeFetcher(splitId);
        }

        // We don't require new splits, all the splits are pre-assigned by source enumerator.
        if (LOG.isDebugEnabled()) {
            LOG.debug("onSplitFinished event: {}", finishedSplitIds);
        }

        for (Map.Entry<String, PulsarPartitionSplitState> entry : finishedSplitIds.entrySet()) {
            PulsarPartitionSplitState state = entry.getValue();
            MessageId latestConsumedId = state.getLatestConsumedId();
            if (latestConsumedId != null) {
                cursorsOfFinishedSplits.put(state.getPartition(), latestConsumedId);
            }
        }
    }

    @Override
    protected PulsarPartitionSplitState initializedState(PulsarPartitionSplit split) {
        return new PulsarPartitionSplitState(split);
    }

    @Override
    protected PulsarPartitionSplit toSplitType(
            String splitId, PulsarPartitionSplitState splitState) {
        return splitState.toPulsarPartitionSplit();
    }

    @Override
    public void pauseOrResumeSplits(
            Collection<String> splitsToPause, Collection<String> splitsToResume) {
        splitFetcherManager.pauseOrResumeSplits(splitsToPause, splitsToResume);
    }

    @Override
    public List<PulsarPartitionSplit> snapshotState(long checkpointId) {
        List<PulsarPartitionSplit> splits = super.snapshotState(checkpointId);

        // Perform a snapshot for these splits.
        Map<TopicPartition, MessageId> cursors =
                cursorsToCommit.computeIfAbsent(checkpointId, id -> new HashMap<>());
        // Put the cursors of the active splits.
        for (PulsarPartitionSplit split : splits) {
            MessageId latestConsumedId = split.getLatestConsumedId();
            if (latestConsumedId != null) {
                cursors.put(split.getPartition(), latestConsumedId);
            }
        }
        // Put cursors of all the finished splits.
        cursors.putAll(cursorsOfFinishedSplits);
        if (deserializationSchema instanceof PulsarTableDeserializationSchema) {
            ((PulsarTableDeserializationSchema) deserializationSchema).updateCurrentCheckpointId(checkpointId);
        }
        return splits;
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) {
        LOG.debug("Committing cursors for checkpoint {}", checkpointId);
        Map<TopicPartition, MessageId> cursors = cursorsToCommit.get(checkpointId);
        try {
            ((PulsarSourceFetcherManager) splitFetcherManager).acknowledgeMessages(cursors);
            LOG.debug("Successfully acknowledge cursors for checkpoint {}", checkpointId);

            // Clean up the cursors.
            cursorsOfFinishedSplits.keySet().removeAll(cursors.keySet());
            cursorsToCommit.headMap(checkpointId + 1).clear();
            if (deserializationSchema instanceof PulsarTableDeserializationSchema) {
                PulsarTableDeserializationSchema schema = (PulsarTableDeserializationSchema) deserializationSchema;
                schema.flushAudit();
                schema.updateLastCheckpointId(checkpointId);
            }
        } catch (Exception e) {
            LOG.error("Failed to acknowledge cursors for checkpoint {}", checkpointId, e);
            cursorCommitThrowable.compareAndSet(null, e);
        }

    }

    @Override
    public void close() throws Exception {
        if (cursorScheduler != null) {
            cursorScheduler.shutdown();
        }

        // Close the all the consumers.
        super.close();

        // Close shared pulsar resources.
        pulsarClient.shutdown();
    }

    // ----------------- helper methods --------------

    /** Acknowledge the pulsar topic partition cursor by the last consumed message id. */
    private void cumulativeAcknowledgmentMessage() {
        Map<TopicPartition, MessageId> cursors = new HashMap<>(cursorsOfFinishedSplits);

        // We reuse snapshotState for acquiring a consume status snapshot.
        // So the checkpoint didn't really happen, so we just pass a fake checkpoint id.
        List<PulsarPartitionSplit> splits = super.snapshotState(1L);
        for (PulsarPartitionSplit split : splits) {
            MessageId latestConsumedId = split.getLatestConsumedId();
            if (latestConsumedId != null) {
                cursors.put(split.getPartition(), latestConsumedId);
            }
        }

        try {
            ((PulsarSourceFetcherManager) splitFetcherManager).acknowledgeMessages(cursors);
            // Clean up the finish splits.
            cursorsOfFinishedSplits.keySet().removeAll(cursors.keySet());
        } catch (Exception e) {
            LOG.error("Fail in auto cursor commit.", e);
            cursorCommitThrowable.compareAndSet(null, e);
        }
    }

    /** Factory method for creating PulsarSourceReader. */
    public static <OUT> PulsarSourceReader<OUT> create(
            SourceConfiguration sourceConfiguration,
            PulsarDeserializationSchema<OUT> deserializationSchema,
            PulsarCrypto pulsarCrypto,
            SourceReaderContext readerContext)
            throws Exception {

        // Create a message queue with the predefined source option.
        int queueCapacity = sourceConfiguration.getMessageQueueCapacity();
        FutureCompletingBlockingQueue<RecordsWithSplitIds<Message<byte[]>>> elementsQueue =
                new FutureCompletingBlockingQueue<>(queueCapacity);

        PulsarClient pulsarClient = createClient(sourceConfiguration);

        // Initialize the deserialization schema before creating the pulsar reader.
        PulsarDeserializationSchemaInitializationContext initializationContext =
                new PulsarDeserializationSchemaInitializationContext(readerContext, pulsarClient);
        deserializationSchema.open(initializationContext, sourceConfiguration);

        // Choose the right schema bytes to use.
        Schema<byte[]> schema;
        if (sourceConfiguration.isEnableSchemaEvolution()) {
            // Wrap the schema into a byte array schema with extra schema info check.
            PulsarSchema<?> pulsarSchema =
                    ((PulsarSchemaWrapper<?>) deserializationSchema).pulsarSchema();
            schema = new BytesSchema(pulsarSchema);
        } else {
            schema = Schema.BYTES;
        }

        // Create an ordered split reader supplier.
        Supplier<SplitReader<Message<byte[]>, PulsarPartitionSplit>> splitReaderSupplier =
                () -> new PulsarPartitionSplitReader(
                        pulsarClient,
                        sourceConfiguration,
                        schema,
                        pulsarCrypto,
                        readerContext.metricGroup());

        PulsarSourceFetcherManager fetcherManager =
                new PulsarSourceFetcherManager(
                        elementsQueue, splitReaderSupplier, readerContext.getConfiguration());

        return new PulsarSourceReader<>(
                elementsQueue,
                fetcherManager,
                deserializationSchema,
                sourceConfiguration,
                pulsarClient,
                readerContext);
    }
}
