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

package org.apache.inlong.sort.cdc.postgres.source.fetch;

import io.debezium.connector.postgresql.PostgresOffsetContext;
import org.apache.flink.util.FlinkRuntimeException;

import org.apache.inlong.sort.cdc.base.relational.JdbcSourceEventDispatcher;
import org.apache.inlong.sort.cdc.base.source.meta.split.SnapshotSplit;
import org.apache.inlong.sort.cdc.base.source.meta.split.SourceSplitBase;
import org.apache.inlong.sort.cdc.base.source.meta.split.StreamSplit;
import org.apache.inlong.sort.cdc.base.source.meta.wartermark.WatermarkKind;
import org.apache.inlong.sort.cdc.base.source.reader.external.FetchTask;
import org.apache.inlong.sort.cdc.postgres.source.offset.PostgresOffset;
import org.apache.inlong.sort.cdc.postgres.source.utils.PgQueryUtils;
import io.debezium.config.Configuration;
import io.debezium.connector.postgresql.PostgresConnectorConfig;
import io.debezium.connector.postgresql.PostgresSchema;
import io.debezium.connector.postgresql.connection.PostgresConnection;
import io.debezium.heartbeat.Heartbeat;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.source.AbstractSnapshotChangeEventSource;
import io.debezium.pipeline.source.spi.ChangeEventSource;
import io.debezium.pipeline.source.spi.SnapshotProgressListener;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.pipeline.spi.SnapshotResult;
import io.debezium.relational.RelationalSnapshotChangeEventSource;
import io.debezium.relational.SnapshotChangeRecordEmitter;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.util.Clock;
import io.debezium.util.ColumnUtils;
import io.debezium.util.Strings;
import io.debezium.util.Threads;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Objects;

import static io.debezium.connector.postgresql.Utils.currentOffset;
import static io.debezium.connector.postgresql.Utils.refreshSchema;
import static io.debezium.relational.RelationalSnapshotChangeEventSource.LOG_INTERVAL;

/** A {@link FetchTask} implementation for Postgres to read snapshot split. */
public class PostgresScanFetchTask implements FetchTask<SourceSplitBase> {

    private static final Logger LOG = LoggerFactory.getLogger(PostgresScanFetchTask.class);

    private final SnapshotSplit split;
    private volatile boolean taskRunning = false;

    public PostgresScanFetchTask(SnapshotSplit split) {
        this.split = split;
    }

    @Override
    public SourceSplitBase getSplit() {
        return split;
    }

    @Override
    public boolean isRunning() {
        return taskRunning;
    }

    @Override
    public void execute(Context context) throws Exception {
        LOG.info("Execute ScanFetchTask for split: {}", split);
        PostgresSourceFetchTaskContext ctx = (PostgresSourceFetchTaskContext) context;
        LOG.info("lk_test Execute ScanFetchTask ctx.getOffsetContext: {}", ctx.getOffsetContext());

        taskRunning = true;

        SnapshotSplitReadTask snapshotSplitReadTask =
                new SnapshotSplitReadTask(
                        ctx.getConnection(),
                        ctx.getDbzConnectorConfig(),
                        ctx.getDatabaseSchema(),
                        ctx.getOffsetContext(),
                        ctx.getDispatcher(),
                        ctx.getSnapshotChangeEventSourceMetrics(),
                        split);

        SnapshotSplitChangeEventSourceContext changeEventSourceContext =
                new SnapshotSplitChangeEventSourceContext();
        SnapshotResult<PostgresOffsetContext> snapshotResult =
                snapshotSplitReadTask.execute(changeEventSourceContext, ctx.getOffsetContext());

        if (!snapshotResult.isCompletedOrSkipped()) {
            LOG.error("snapshotResult.isCompletedOrSkipped throw.");
            taskRunning = false;
            throw new IllegalStateException(
                    String.format("Read snapshot for postgres split %s fail", split));
        }

        executeBackfillTask(ctx, changeEventSourceContext);
    }

    private void executeBackfillTask(
            PostgresSourceFetchTaskContext ctx,
            SnapshotSplitChangeEventSourceContext changeEventSourceContext)
            throws InterruptedException {
        final StreamSplit backfillSplit =
                new StreamSplit(
                        split.splitId(),
                        changeEventSourceContext.getLowWatermark(),
                        changeEventSourceContext.getHighWatermark(),
                        new ArrayList<>(),
                        split.getTableSchemas(),
                        0);

        // optimization that skip the WAL read when the low watermark >= high watermark
        final boolean backfillRequired =
                backfillSplit.getEndingOffset().isAtOrAfter(backfillSplit.getStartingOffset());
        // lk_test TODO maybe has bug
        LOG.info("in executeBackfillTask get getEndingOffset:{}, getStartingOffset:{}, backfillRequired:{}",
                backfillSplit.getEndingOffset(), backfillSplit.getStartingOffset(), backfillRequired);
        if (backfillRequired) {
            LOG.info(
                    "Skip the backfill {} for split {}: low watermark >= high watermark",
                    backfillSplit,
                    split);
            ctx.getDispatcher()
                    .dispatchWatermarkEvent(
                            ctx.getOffsetContext().getPartition(),
                            backfillSplit,
                            backfillSplit.getEndingOffset(),
                            WatermarkKind.END);

            taskRunning = false;
            return;
        }

        final PostgresOffsetContext.Loader loader =
                new PostgresOffsetContext.Loader(ctx.getDbzConnectorConfig());
        final PostgresOffsetContext postgresOffsetContext =
                loader.load(backfillSplit.getStartingOffset().getOffset());

        // we should only capture events for the current table,
        // otherwise, we may not find corresponding schema
        Configuration dbzConf =
                ctx.getDbzConnectorConfig()
                        .getConfig()
                        .edit()
                        .with("table.include.list", split.getTableId().toString())
                        // Disable heartbeat event in snapshot split fetcher
                        .with(Heartbeat.HEARTBEAT_INTERVAL, 0)
                        .build();

        final PostgresStreamFetchTask.StreamSplitReadTask backfillReadTask =
                new PostgresStreamFetchTask.StreamSplitReadTask(
                        new PostgresConnectorConfig(dbzConf),
                        ctx.getSnapShotter(),
                        ctx.getConnection(),
                        ctx.getDispatcher(),
                        ctx.getErrorHandler(),
                        ctx.getTaskContext().getClock(),
                        ctx.getDatabaseSchema(),
                        ctx.getTaskContext(),
                        ctx.getReplicationConnection(),
                        backfillSplit);
        LOG.info("Execute backfillReadTask for split {}", split);
        backfillReadTask.execute(new PostgresChangeEventSourceContext(), postgresOffsetContext);
    }

    static class SnapshotSplitChangeEventSourceContext
            implements
                ChangeEventSource.ChangeEventSourceContext {

        private PostgresOffset lowWatermark;
        private PostgresOffset highWatermark;

        public PostgresOffset getLowWatermark() {
            return lowWatermark;
        }

        public void setLowWatermark(PostgresOffset lowWatermark) {
            this.lowWatermark = lowWatermark;
        }

        public PostgresOffset getHighWatermark() {
            return highWatermark;
        }

        public void setHighWatermark(PostgresOffset highWatermark) {
            this.highWatermark = highWatermark;
        }

        @Override
        public boolean isRunning() {
            return lowWatermark != null && highWatermark != null;
        }
    }

    class PostgresChangeEventSourceContext implements ChangeEventSource.ChangeEventSourceContext {

        public void finished() {
            taskRunning = false;
        }

        @Override
        public boolean isRunning() {
            return taskRunning;
        }
    }

    /** A SnapshotChangeEventSource implementation for Postgres to read snapshot split. */
    public static class SnapshotSplitReadTask extends AbstractSnapshotChangeEventSource {

        private static final Logger LOG = LoggerFactory.getLogger(SnapshotSplitReadTask.class);

        private final PostgresConnection jdbcConnection;
        private final PostgresConnectorConfig connectorConfig;
        private final JdbcSourceEventDispatcher dispatcher;
        private final SnapshotSplit snapshotSplit;
        private final PostgresOffsetContext offsetContext;
        private final PostgresSchema databaseSchema;
        private final SnapshotProgressListener snapshotProgressListener;
        private final Clock clock;

        public SnapshotSplitReadTask(
                PostgresConnection jdbcConnection,
                PostgresConnectorConfig connectorConfig,
                PostgresSchema databaseSchema,
                PostgresOffsetContext previousOffset,
                JdbcSourceEventDispatcher dispatcher,
                SnapshotProgressListener snapshotProgressListener,
                SnapshotSplit snapshotSplit) {
            super(connectorConfig, snapshotProgressListener);
            this.jdbcConnection = jdbcConnection;
            this.connectorConfig = connectorConfig;
            this.snapshotProgressListener = snapshotProgressListener;
            this.databaseSchema = databaseSchema;
            this.dispatcher = dispatcher;
            this.snapshotSplit = snapshotSplit;
            this.offsetContext = previousOffset;
            this.clock = Clock.SYSTEM;
        }

        @Override
        protected SnapshotResult doExecute(
                ChangeEventSourceContext context,
                OffsetContext previousOffset,
                SnapshotContext snapshotContext,
                SnapshottingTask snapshottingTask)
                throws Exception {
            final RelationalSnapshotChangeEventSource.RelationalSnapshotContext ctx =
                    (RelationalSnapshotChangeEventSource.RelationalSnapshotContext) snapshotContext;
            ctx.offset = offsetContext;
            refreshSchema(databaseSchema, jdbcConnection, false);

            final PostgresOffset lowWatermark = currentOffset(jdbcConnection);
            LOG.info(
                    "Snapshot step 1 - Determining low watermark {} for split {}",
                    lowWatermark,
                    snapshotSplit);
            ((SnapshotSplitChangeEventSourceContext) (context)).setLowWatermark(lowWatermark);
            dispatcher.dispatchWatermarkEvent(
                    offsetContext.getPartition(), snapshotSplit, lowWatermark, WatermarkKind.LOW);

            LOG.info("Snapshot step 2 - Snapshotting data");
            createDataEvents(ctx, snapshotSplit.getTableId());

            final PostgresOffset highWatermark = currentOffset(jdbcConnection);
            LOG.info(
                    "Snapshot step 3 - Determining high watermark {} for split {}",
                    highWatermark,
                    snapshotSplit);
            ((SnapshotSplitChangeEventSourceContext) (context)).setHighWatermark(highWatermark);
            dispatcher.dispatchWatermarkEvent(
                    offsetContext.getPartition(), snapshotSplit, highWatermark, WatermarkKind.HIGH);
            return SnapshotResult.completed(ctx.offset);
        }

        private void createDataEvents(
                RelationalSnapshotChangeEventSource.RelationalSnapshotContext snapshotContext,
                TableId tableId)
                throws InterruptedException {
            EventDispatcher.SnapshotReceiver snapshotReceiver =
                    dispatcher.getSnapshotChangeEventReceiver();
            LOG.info("Snapshotting table {}", tableId);
            createDataEventsForTable(
                    snapshotContext,
                    snapshotReceiver,
                    Objects.requireNonNull(databaseSchema.tableFor(tableId)));
            snapshotReceiver.completeSnapshot();
        }

        /** Dispatches the data change events for the records of a single table. */
        private void createDataEventsForTable(
                RelationalSnapshotChangeEventSource.RelationalSnapshotContext snapshotContext,
                EventDispatcher.SnapshotReceiver snapshotReceiver,
                Table table)
                throws InterruptedException {

            long exportStart = clock.currentTimeInMillis();
            LOG.info(
                    "Exporting data from split '{}' of table {}",
                    snapshotSplit.splitId(),
                    table.id());

            final String selectSql =
                    PgQueryUtils.buildSplitScanQuery(
                            snapshotSplit.getTableId(),
                            snapshotSplit.getSplitKeyType(),
                            snapshotSplit.getSplitStart() == null,
                            snapshotSplit.getSplitEnd() == null);
            LOG.debug(
                    "For split '{}' of table {} using select statement: '{}'",
                    snapshotSplit.splitId(),
                    table.id(),
                    selectSql);

            try (PreparedStatement selectStatement =
                    PgQueryUtils.readTableSplitDataStatement(
                            jdbcConnection,
                            selectSql,
                            snapshotSplit.getSplitStart() == null,
                            snapshotSplit.getSplitEnd() == null,
                            snapshotSplit.getSplitStart(),
                            snapshotSplit.getSplitEnd(),
                            snapshotSplit.getSplitKeyType().getFieldCount(),
                            connectorConfig.getQueryFetchSize());
                    ResultSet rs = selectStatement.executeQuery()) {

                ColumnUtils.ColumnArray columnArray = ColumnUtils.toArray(rs, table);
                long rows = 0;
                Threads.Timer logTimer = getTableScanLogTimer();

                while (rs.next()) {
                    rows++;
                    final Object[] row = new Object[columnArray.getGreatestColumnPosition()];
                    for (int i = 0; i < columnArray.getColumns().length; i++) {
                        row[columnArray.getColumns()[i].position() - 1] = rs.getObject(i + 1);
                    }
                    if (logTimer.expired()) {
                        long stop = clock.currentTimeInMillis();
                        LOG.info(
                                "Exported {} records for split '{}' after {}",
                                rows,
                                snapshotSplit.splitId(),
                                Strings.duration(stop - exportStart));
                        snapshotProgressListener.rowsScanned(table.id(), rows);
                        logTimer = getTableScanLogTimer();
                    }
                    snapshotContext.offset.event(table.id(), clock.currentTime());
                    SnapshotChangeRecordEmitter emitter =
                            new SnapshotChangeRecordEmitter(snapshotContext.offset, row, clock);
                    dispatcher.dispatchSnapshotEvent(table.id(), emitter, snapshotReceiver);
                }
                LOG.info(
                        "Finished exporting {} records for split '{}', total duration '{}'",
                        rows,
                        snapshotSplit.splitId(),
                        Strings.duration(clock.currentTimeInMillis() - exportStart));
            } catch (SQLException e) {
                throw new FlinkRuntimeException(
                        "Snapshotting of table " + table.id() + " failed", e);
            }
        }

        private Threads.Timer getTableScanLogTimer() {
            return Threads.timer(clock, LOG_INTERVAL);
        }

        @Override
        protected SnapshottingTask getSnapshottingTask(OffsetContext previousOffset) {
            return new SnapshottingTask(false, true);
        }

        @Override
        protected SnapshotContext prepare(ChangeEventSourceContext changeEventSourceContext)
                throws Exception {
            return new PostgresSnapshotContext();
        }

        private static class PostgresSnapshotContext
                extends
                    RelationalSnapshotChangeEventSource.RelationalSnapshotContext {

            public PostgresSnapshotContext() throws SQLException {
                super("");
            }
        }
    }
}