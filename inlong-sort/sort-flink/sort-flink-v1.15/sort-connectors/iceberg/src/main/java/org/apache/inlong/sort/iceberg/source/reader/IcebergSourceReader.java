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

package org.apache.inlong.sort.iceberg.source.reader;

import org.apache.inlong.sort.base.util.OpenTelemetryLogger;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.connector.base.source.reader.SingleThreadMultiplexSourceReaderBase;
import org.apache.iceberg.flink.source.reader.ReaderFunction;
import org.apache.iceberg.flink.source.reader.RecordAndPosition;
import org.apache.iceberg.flink.source.split.IcebergSourceSplit;
import org.apache.iceberg.flink.source.split.SplitRequestEvent;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.logging.log4j.Level;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Copy from iceberg-flink:iceberg-flink-1.15:1.3.1
 */
@Internal
public class IcebergSourceReader<T>
        extends
            SingleThreadMultiplexSourceReaderBase<RecordAndPosition<T>, T, IcebergSourceSplit, IcebergSourceSplit> {

    private final InlongIcebergSourceReaderMetrics<T> metrics;
    private OpenTelemetryLogger openTelemetryLogger;
    private final boolean enableLogReport;

    public IcebergSourceReader(
            InlongIcebergSourceReaderMetrics<T> metrics,
            ReaderFunction<T> readerFunction,
            SourceReaderContext context,
            boolean enableLogReport) {
        super(
                () -> new IcebergSourceSplitReader<>(metrics, readerFunction, context),
                new IcebergSourceRecordEmitter<>(),
                context.getConfiguration(),
                context);
        this.metrics = metrics;
        this.enableLogReport = enableLogReport;
        if (this.enableLogReport) {
            this.openTelemetryLogger = new OpenTelemetryLogger.Builder()
                    .setLogLevel(Level.ERROR)
                    .setServiceName(this.getClass().getSimpleName())
                    .setLocalHostIp(this.context.getLocalHostName()).build();
        }
    }

    @Override
    public void start() {
        if (this.enableLogReport) {
            this.openTelemetryLogger.install();
        }
        // We request a split only if we did not get splits during the checkpoint restore.
        // Otherwise, reader restarts will keep requesting more and more splits.
        if (getNumberOfCurrentlyAssignedSplits() == 0) {
            requestSplit(Collections.emptyList());
        }
    }

    @Override
    public void close() throws Exception {
        super.close();
        if (this.enableLogReport) {
            openTelemetryLogger.uninstall();
        }
    }

    @Override
    protected void onSplitFinished(Map<String, IcebergSourceSplit> finishedSplitIds) {
        requestSplit(Lists.newArrayList(finishedSplitIds.keySet()));
    }
    @Override
    public List<IcebergSourceSplit> snapshotState(long checkpointId) {
        metrics.updateCurrentCheckpointId(checkpointId);
        return super.snapshotState(checkpointId);
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {
        super.notifyCheckpointComplete(checkpointId);
        metrics.flushAudit();
        metrics.updateLastCheckpointId(checkpointId);
    }

    @Override
    protected IcebergSourceSplit initializedState(IcebergSourceSplit split) {
        return split;
    }

    @Override
    protected IcebergSourceSplit toSplitType(String splitId, IcebergSourceSplit splitState) {
        return splitState;
    }

    private void requestSplit(Collection<String> finishedSplitIds) {
        context.sendSourceEventToCoordinator(new SplitRequestEvent(finishedSplitIds));
    }
}
