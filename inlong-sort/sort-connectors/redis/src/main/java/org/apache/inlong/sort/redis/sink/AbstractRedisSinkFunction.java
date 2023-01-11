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

package org.apache.inlong.sort.redis.sink;

import static org.apache.flink.util.Preconditions.checkNotNull;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ScheduledExecutorService;
import javax.annotation.concurrent.GuardedBy;
import org.apache.commons.lang3.time.StopWatch;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisConfigBase;
import org.apache.flink.table.data.RowData;
import org.apache.flink.types.Row;
import org.apache.inlong.sort.redis.common.container.InlongRedisCommandsContainer;
import org.apache.inlong.sort.redis.common.container.RedisCommandsContainerBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The Flink Redis Producer.
 */
public abstract class AbstractRedisSinkFunction<OUT>
        extends
            RichSinkFunction<RowData>
        implements
            CheckpointedFunction {

    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(AbstractRedisSinkFunction.class);

    /**
     * The output type info.
     */
    private final TypeInformation<OUT> outputType;

    /**
     * The serializer for values.
     */
    protected final SerializationSchema<RowData> serializationSchema;

    /**
     * The address of redis server, format eg: 127.0.0.1:8080,127.0.0.2:8081 .
     * If server is not cluster mode, server address format eg: 127.0.0.1:8080 .
     */
    private final String address;

    protected final FlinkJedisConfigBase flinkJedisConfigBase;

    /**
     * The redis record expired time.
     */
    protected transient Duration expireTime;

    /**
     * The flag indicating whether the main thread need flush.
     */
    private transient boolean forceFlush;

    private ListState<OUT> listState;

    private transient Object lock;

    private final long batchSize;

    private final long flushIntervalInMillis;

    private static final String DEFAULT_OUTPUT_FLUSH_THREAD_NAME = "OutputFlusher";

    private final List<OUT> rows;

    private transient ScheduledExecutorService executorService;

    /**
     * The container for all available Redis commands.
     */
    private InlongRedisCommandsContainer redisCommandsContainer;

    /**
     * The stop watch to measure time duration.
     */
    @GuardedBy("lock")
    protected transient StopWatch stopWatch;

    public AbstractRedisSinkFunction(
            TypeInformation<OUT> outputType,
            SerializationSchema<RowData> serializationSchema,
            String address,
            long batchSize,
            Duration flushInterval,
            Duration configuration,
            FlinkJedisConfigBase flinkJedisConfigBase) {
        checkNotNull(configuration, "The configuration must not be null.");

        this.address = address;
        this.outputType = outputType;
        this.serializationSchema = serializationSchema;

        this.batchSize = batchSize;
        this.flushIntervalInMillis = flushInterval.toMillis();
        this.forceFlush = false;
        this.rows = new ArrayList<>();
        this.flinkJedisConfigBase = flinkJedisConfigBase;
    }

    @Override
    public void open(Configuration parameters) {
        LOG.info("Opening redis sink with address: {}",
                address);

        lock = new Object();

        stopWatch = new StopWatch();

        try {
            this.redisCommandsContainer = RedisCommandsContainerBuilder.build(this.flinkJedisConfigBase);
            this.redisCommandsContainer.open();
        } catch (Exception e) {
            LOG.error("Redis has not been properly initialized: ", e);
            throw new RuntimeException(e);
        }

        Optional<OutputFlusher> outputFlusher;
        if (this.batchSize == 1 || this.flushIntervalInMillis == 0) {
            LOG.info("Flush records immediately.");
            outputFlusher = Optional.empty();
        } else {
            String threadName = DEFAULT_OUTPUT_FLUSH_THREAD_NAME + " for "
                    + getRuntimeContext().getTaskNameWithSubtasks();
            outputFlusher = Optional.of(new OutputFlusher(threadName, flushIntervalInMillis));
            outputFlusher.get().start();
        }

    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        final ListStateDescriptor<OUT> stateDescriptor = new ListStateDescriptor<>(
                "rowState", outputType);
        this.listState = context.getOperatorStateStore().getListState(stateDescriptor);
        if (context.isRestored()) {
            if (listState != null) {
                listState.get().forEach(rows::add);
            }
        }
    }

    @Override
    public void snapshotState(FunctionSnapshotContext functionSnapshotContext) throws Exception {
        LOG.info("redis start snapshotState, id: {}", functionSnapshotContext.getCheckpointId());
        synchronized (lock) {
            listState.clear();
            listState.addAll(rows);
        }
        LOG.info("redis end snapshotState, id: {}", functionSnapshotContext.getCheckpointId());
    }

    protected abstract List<OUT> serialize(Tuple2<Boolean, Row> in);

    public void invoke(Tuple2<Boolean, Row> in, Context context) {

        List<OUT> redisOutputs = serialize(in);
        synchronized (lock) {
            rows.addAll(redisOutputs);
            if (forceFlush || rows.size() >= batchSize) {
                flush();
            }
        }

    }

    @Override
    public void close() throws Exception {
        closeClient();

        if (executorService != null) {
            try {
                executorService.shutdown();
            } catch (Throwable t) {
                LOG.warn("Could not properly shut down ScheduledExecutorService.", t);
            }
        }

        super.close();

        LOG.info("Closed redis sink.");
    }

    private void closeClient() {
        synchronized (lock) {
            if (redisCommandsContainer != null) {
                flush();
                try {
                    redisCommandsContainer.close();
                    redisCommandsContainer = null;
                } catch (Throwable t) {
                    LOG.warn("Could not properly close the redis client.", t);
                }
            }
        }
    }

    private class OutputFlusher extends Thread {

        private final long timeoutInMillis;
        private volatile boolean running = true;

        OutputFlusher(String name, long timeoutInMillis) {
            super(name);
            setDaemon(true);
            this.timeoutInMillis = timeoutInMillis;
        }

        public void terminate() {
            running = false;
            interrupt();
        }

        @Override
        public void run() {
            while (running) {

                try {
                    try {
                        Thread.sleep(timeoutInMillis);
                    } catch (InterruptedException e) {
                        if (running) {
                            throw new Exception(e);
                        }
                    }
                    if (rows.size() > 0) {
                        flush();
                    }
                } catch (Throwable t) {
                    LOG.error("An exception happened while flushing the outputs", t);
                    // There is no need to handle exceptions in asynchronous threads.
                    // When the number of rows exceeds the batchSize, it will fail directly in the next write.
                    // But there is a possibility of data delay.
                    forceFlush = true;
                    LOG.error("Set the forceFlush to true, it will retry in the main thread.");
                }

            }
        }
    }

    protected abstract void flushInternal(List<OUT> rows);

    private void flush() {
        synchronized (lock) {
            try {
                if (rows != null && rows.size() > 0) {
                    LOG.debug("Flushing {} records to redis...", rows.size());
                    flushInternal(rows);
                    LOG.debug("Flushed {} records to redis...", rows.size());
                    rows.clear();
                }
            } finally {
                forceFlush = false;
            }
        }
    }
}
