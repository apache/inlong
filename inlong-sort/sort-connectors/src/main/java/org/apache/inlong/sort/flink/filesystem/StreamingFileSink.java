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

package org.apache.inlong.sort.flink.filesystem;

import com.google.common.base.Preconditions;
import java.io.IOException;
import java.io.Serializable;
import org.apache.flink.api.common.serialization.BulkWriter;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.OperatorStateStore;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.api.common.typeutils.base.array.BytePrimitiveArraySerializer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.state.CheckpointListener;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeCallback;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeService;

/**
 * Copied from Apache Flink project {@link org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink}.
 */
public class StreamingFileSink<IN>
        extends RichSinkFunction<IN>
        implements CheckpointedFunction, CheckpointListener, ProcessingTimeCallback {

    private static final long serialVersionUID = 1L;

    // -------------------------- state descriptors ---------------------------

    private static final ListStateDescriptor<byte[]> BUCKET_STATE_DESC =
            new ListStateDescriptor<>("bucket-states", BytePrimitiveArraySerializer.INSTANCE);

    private static final ListStateDescriptor<Long> MAX_PART_COUNTER_STATE_DESC =
            new ListStateDescriptor<>("max-part-counter", LongSerializer.INSTANCE);

    // ------------------------ configuration fields --------------------------

    private final long bucketCheckInterval;

    private final StreamingFileSink.BucketsBuilder<IN, ?> bucketsBuilder;

    // --------------------------- runtime fields -----------------------------

    private transient Buckets<IN, ?> buckets;

    private transient ProcessingTimeService processingTimeService;

    // --------------------------- State Related Fields -----------------------------

    private transient ListState<byte[]> bucketStates;

    private transient ListState<Long> maxPartCountersState;

    /**
     * Creates a new {@code StreamingFileSink} that writes files to the given base directory.
     */
    public StreamingFileSink(
            final StreamingFileSink.BucketsBuilder<IN, ?> bucketsBuilder,
            final long bucketCheckInterval) {

        Preconditions.checkArgument(bucketCheckInterval > 0L);

        this.bucketsBuilder = Preconditions.checkNotNull(bucketsBuilder);
        this.bucketCheckInterval = bucketCheckInterval;
    }

    // ------------------------------------------------------------------------

    // --------------------------- Sink Builders  -----------------------------

    /**
     * The base abstract class for the {@link StreamingFileSink.BulkFormatBuilder}.
     */
    protected abstract static class BucketsBuilder<IN, BucketID> implements Serializable {

        private static final long serialVersionUID = 1L;

        abstract Buckets<IN, BucketID> createBuckets(final int subtaskIndex) throws IOException;
    }

    /**
     * A builder for configuring the sink for bulk-encoding formats, e.g. Parquet/ORC.
     */
    public static class BulkFormatBuilder<IN, BucketID> extends StreamingFileSink.BucketsBuilder<IN, BucketID> {

        private static final long serialVersionUID = 1L;

        private long bucketCheckInterval = 60L * 1000L;

        private final long dataFlowId;

        private final Path basePath;

        private final BulkWriter.Factory<IN> writerFactory;

        private final BucketAssigner<IN, BucketID> bucketAssigner;

        private BucketFactory<IN, BucketID> bucketFactory = new DefaultBucketFactoryImpl<>();

        private final RollingPolicy<IN, BucketID> rollingPolicy;

        public BulkFormatBuilder(
                long dataFlowId,
                Path basePath,
                BulkWriter.Factory<IN> writerFactory,
                BucketAssigner<IN, BucketID> assigner,
                RollingPolicy<IN, BucketID> rollingPolicy) {
            this.dataFlowId = dataFlowId;
            this.basePath = Preconditions.checkNotNull(basePath);
            this.writerFactory = writerFactory;
            this.bucketAssigner = Preconditions.checkNotNull(assigner);
            this.rollingPolicy = Preconditions.checkNotNull(rollingPolicy);
        }

        public StreamingFileSink.BulkFormatBuilder<IN, BucketID> withBucketCheckInterval(long interval) {
            this.bucketCheckInterval = interval;
            return this;
        }

        public StreamingFileSink.BulkFormatBuilder<IN, BucketID> withBucketFactory(
                final BucketFactory<IN, BucketID> factory) {
            this.bucketFactory = Preconditions.checkNotNull(factory);
            return this;
        }

        /**
         * Creates the actual sink.
         */
        public StreamingFileSink<IN> build() {
            return new StreamingFileSink<>(this, bucketCheckInterval);
        }

        @Override
        Buckets<IN, BucketID> createBuckets(int subtaskIndex) throws IOException {
            return new Buckets<>(
                    dataFlowId,
                    basePath,
                    bucketAssigner,
                    bucketFactory,
                    new BulkPartWriter.Factory<>(writerFactory),
                    rollingPolicy,
                    subtaskIndex);
        }
    }

    // --------------------------- Sink Methods -----------------------------

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        final int subtaskIndex = getRuntimeContext().getIndexOfThisSubtask();
        this.buckets = bucketsBuilder.createBuckets(subtaskIndex);

        final OperatorStateStore stateStore = context.getOperatorStateStore();
        bucketStates = stateStore.getListState(BUCKET_STATE_DESC);
        maxPartCountersState = stateStore.getUnionListState(MAX_PART_COUNTER_STATE_DESC);

        if (context.isRestored()) {
            buckets.initializeState(bucketStates, maxPartCountersState);
        }
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {
        buckets.commitUpToCheckpoint(checkpointId);
    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        Preconditions.checkState(bucketStates != null && maxPartCountersState != null, "sink has not been initialized");

        buckets.snapshotState(
                context.getCheckpointId(),
                bucketStates,
                maxPartCountersState);
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        this.processingTimeService = ((StreamingRuntimeContext) getRuntimeContext()).getProcessingTimeService();
        long currentProcessingTime = processingTimeService.getCurrentProcessingTime();
        processingTimeService.registerTimer(currentProcessingTime + bucketCheckInterval, this);
    }

    @Override
    public void onProcessingTime(long timestamp) throws Exception {
        final long currentTime = processingTimeService.getCurrentProcessingTime();
        buckets.onProcessingTime(currentTime);
        processingTimeService.registerTimer(currentTime + bucketCheckInterval, this);
    }

    @Override
    public void invoke(IN value, SinkFunction.Context context) throws Exception {
        buckets.onElement(value, context);
    }

    @Override
    public void close() throws Exception {
        if (buckets != null) {
            buckets.close();
        }
    }
}
