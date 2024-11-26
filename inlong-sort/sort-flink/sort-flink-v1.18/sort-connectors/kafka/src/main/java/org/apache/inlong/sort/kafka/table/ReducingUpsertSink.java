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

package org.apache.inlong.sort.kafka.table;

import org.apache.flink.api.connector.sink.Sink;
import org.apache.flink.api.connector.sink.SinkWriter;
import org.apache.flink.api.connector.sink2.Committer;
import org.apache.flink.connector.kafka.sink.TwoPhaseCommittingStatefulSink;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.util.function.SerializableFunction;

import java.io.IOException;
import java.util.Collection;

/**
 * A wrapper of a {@link Sink}. It will buffer the data emitted by the wrapper {@link SinkWriter}
 * and only emit it when the buffer is full or a timer is triggered or a checkpoint happens.
 *
 * <p>The sink provides eventual consistency guarantees under {@link
 * org.apache.flink.connector.base.DeliveryGuarantee#AT_LEAST_ONCE} because the updates are
 * idempotent therefore duplicates have no effect.
 * copied from org.apache.flink:flink-connector-kafka:3.2.0
 */
class ReducingUpsertSink<WriterState, Comm>
        implements
            TwoPhaseCommittingStatefulSink<RowData, WriterState, Comm> {

    private final TwoPhaseCommittingStatefulSink<RowData, WriterState, Comm> wrappedSink;
    private final DataType physicalDataType;
    private final int[] keyProjection;
    private final SinkBufferFlushMode bufferFlushMode;
    private final SerializableFunction<RowData, RowData> valueCopyFunction;

    ReducingUpsertSink(
            TwoPhaseCommittingStatefulSink<RowData, WriterState, Comm> wrappedSink,
            DataType physicalDataType,
            int[] keyProjection,
            SinkBufferFlushMode bufferFlushMode,
            SerializableFunction<RowData, RowData> valueCopyFunction) {
        this.wrappedSink = wrappedSink;
        this.physicalDataType = physicalDataType;
        this.keyProjection = keyProjection;
        this.bufferFlushMode = bufferFlushMode;
        this.valueCopyFunction = valueCopyFunction;
    }

    @Override
    public TwoPhaseCommittingStatefulSink.PrecommittingStatefulSinkWriter<RowData, WriterState, Comm> createWriter(
            InitContext context) throws IOException {
        return new ReducingUpsertWriter<>(
                wrappedSink.createWriter(context),
                physicalDataType,
                keyProjection,
                bufferFlushMode,
                context.getProcessingTimeService(),
                valueCopyFunction);
    }

    @Override
    public Committer<Comm> createCommitter() throws IOException {
        return wrappedSink.createCommitter();
    }

    @Override
    public SimpleVersionedSerializer<Comm> getCommittableSerializer() {
        return wrappedSink.getCommittableSerializer();
    }

    @Override
    public TwoPhaseCommittingStatefulSink.PrecommittingStatefulSinkWriter<RowData, WriterState, Comm> restoreWriter(
            InitContext context, Collection<WriterState> recoveredState)
            throws IOException {
        final TwoPhaseCommittingStatefulSink.PrecommittingStatefulSinkWriter<RowData, WriterState, Comm> wrappedWriter =
                wrappedSink.restoreWriter(context, recoveredState);
        return new ReducingUpsertWriter<>(
                wrappedWriter,
                physicalDataType,
                keyProjection,
                bufferFlushMode,
                context.getProcessingTimeService(),
                valueCopyFunction);
    }

    @Override
    public SimpleVersionedSerializer<WriterState> getWriterStateSerializer() {
        return wrappedSink.getWriterStateSerializer();
    }
}
