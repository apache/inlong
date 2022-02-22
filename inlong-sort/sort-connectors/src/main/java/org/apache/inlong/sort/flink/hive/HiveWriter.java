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

package org.apache.inlong.sort.flink.hive;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.inlong.sort.configuration.Constants.SINK_HIVE_ROLLING_POLICY_CHECK_INTERVAL;
import static org.apache.inlong.sort.configuration.Constants.SINK_HIVE_ROLLING_POLICY_FILE_SIZE;
import static org.apache.inlong.sort.configuration.Constants.SINK_HIVE_ROLLING_POLICY_ROLLOVER_INTERVAL;
import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import org.apache.flink.api.common.serialization.BulkWriter;
import org.apache.flink.api.common.state.CheckpointListener;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.fs.RecoverableWriter;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.inlong.sort.configuration.Configuration;
import org.apache.inlong.sort.flink.filesystem.Bucket;
import org.apache.inlong.sort.flink.filesystem.DefaultBucketFactoryImpl;
import org.apache.inlong.sort.flink.filesystem.DefaultRollingPolicy;
import org.apache.inlong.sort.flink.filesystem.PartFileWriter;
import org.apache.inlong.sort.flink.filesystem.RollingPolicy;
import org.apache.inlong.sort.flink.filesystem.StreamingFileSink;
import org.apache.inlong.sort.flink.filesystem.StreamingFileSink.BulkFormatBuilder;
import org.apache.inlong.sort.flink.hive.partition.HivePartition;
import org.apache.inlong.sort.flink.hive.partition.PartitionCommitInfo;
import org.apache.inlong.sort.flink.hive.partition.PartitionComputer;
import org.apache.inlong.sort.flink.hive.partition.RowPartitionComputer;
import org.apache.inlong.sort.protocol.sink.HiveSinkInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Hive sink writer.
 */
public class HiveWriter extends ProcessFunction<Row, PartitionCommitInfo>
        implements CheckpointedFunction, CheckpointListener {

    private static final Logger LOG = LoggerFactory.getLogger(HiveWriter.class);

    private static final long serialVersionUID = 4293562058643851159L;

    private final long dataFlowId;

    private final StreamingFileSink<Row> fileWriter;

    private final Configuration configuration;

    private transient FileWriterContext fileWriterContext;

    private transient List<HivePartition> newPartitions;

    private static UserGroupInformation proxyUgi;

    public HiveWriter(Configuration configuration, long dataFlowId, HiveSinkInfo hiveSinkInfo) {
        this.configuration = checkNotNull(configuration);
        this.dataFlowId = dataFlowId;
        UserGroupInformation realUgi = null;
        try {
            realUgi = UserGroupInformation.getLoginUser();
        } catch (IOException e) {
            e.printStackTrace();
        }
        String proxyUser = hiveSinkInfo.getHadoopProxyUser();
        if (proxyUser != null && !proxyUser.isEmpty()) {
            //create proxyUser
            proxyUgi = UserGroupInformation.createProxyUser(proxyUser, realUgi);
        }
        final BulkWriter.Factory<Row> bulkWriterFactory = HiveSinkHelper.createBulkWriterFactory(
                hiveSinkInfo, configuration);
        final RowPartitionComputer rowPartitionComputer = new RowPartitionComputer("", hiveSinkInfo);
        final BulkFormatBuilder<Row, HivePartition> bulkFormatBuilder = new BulkFormatBuilder<>(
                new Path(hiveSinkInfo.getDataPath()),
                outputStream ->
                        new PartitionFilterBulkWriter(bulkWriterFactory.create(outputStream), rowPartitionComputer),
                new HivePartitionBucketAssigner<>(rowPartitionComputer),
                DefaultRollingPolicy
                        .create()
                        .withMaxPartSize(configuration.getLong(SINK_HIVE_ROLLING_POLICY_FILE_SIZE))
                        .withRolloverInterval(configuration.getLong(SINK_HIVE_ROLLING_POLICY_ROLLOVER_INTERVAL))
                        .build())
                .withBucketFactory(new BucketWithLifeCycleFactory())
                .withBucketCheckInterval(configuration.getLong(SINK_HIVE_ROLLING_POLICY_CHECK_INTERVAL));
        fileWriter = bulkFormatBuilder.build();
    }

    @Override
    public void initializeState(FunctionInitializationContext functionInitializationContext) throws Exception {
        doAsWithUGI(proxyUgi, () -> {
            newPartitions = new ArrayList<>();
            fileWriterContext = new FileWriterContext();
            fileWriter.setRuntimeContext(getRuntimeContext());
            fileWriter.initializeState(functionInitializationContext);
            return null;
        });
    }

    @Override
    public void open(org.apache.flink.configuration.Configuration parameters) throws Exception {
        doAsWithUGI(proxyUgi, () -> {
            fileWriter.open(parameters);
            return null;
        });

    }

    @Override
    public void close() throws Exception {
        doAsWithUGI(proxyUgi, () -> {
            fileWriter.close();
            return null;
        });
    }

    @Override
    public void snapshotState(FunctionSnapshotContext functionSnapshotContext) throws Exception {
        doAsWithUGI(proxyUgi, () -> {
            fileWriter.snapshotState(functionSnapshotContext);
            return null;
        });
    }

    @Override
    public void processElement(Row in, Context context, Collector<PartitionCommitInfo> collector) throws Exception {
        doAsWithUGI(proxyUgi, () -> {
            fileWriter.invoke(in, fileWriterContext.setContext(context));
            if (!newPartitions.isEmpty()) {
                collector.collect(new PartitionCommitInfo(dataFlowId, new ArrayList<>(newPartitions)));
                newPartitions.clear();
            }
            return null;
        });
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {
        doAsWithUGI(proxyUgi, () -> {
            fileWriter.notifyCheckpointComplete(checkpointId);
            return null;
        });
    }

    public static <R> R doAsWithUGI(UserGroupInformation ugi, Callable<R> callable) throws IOException {
        if (ugi != null) {
            try {
                return ugi.doAs((PrivilegedExceptionAction<R>) callable::call);
            } catch (InterruptedException e) {
                LOG.error("Cannot access resource via doAs. user:{} error:{}", ugi.getUserName(), e);
                throw new IOException(e);
            } catch (Exception e) {
                throw new IOException(e);
            }
        } else {
            try {
                return callable.call();
            } catch (Exception e) {
                throw new IOException(e);
            }
        }
    }

    private class FileWriterContext implements SinkFunction.Context {

        private Context context;

        public FileWriterContext setContext(Context context) {
            this.context = context;
            return this;
        }

        @Override
        public long currentProcessingTime() {
            return context.timerService().currentProcessingTime();
        }

        @Override
        public long currentWatermark() {
            return context.timerService().currentWatermark();
        }

        @Override
        public Long timestamp() {
            return context.timestamp();
        }
    }

    /**
     * There is no life cycle listener in Flink 1.9, so hack it here to get new partition.
     */
    public class BucketWithLifeCycleFactory extends DefaultBucketFactoryImpl<Row, HivePartition> {

        private static final long serialVersionUID = 3374117997155278060L;

        @Override
        public Bucket<Row, HivePartition> getNewBucket(
                final RecoverableWriter fsWriter,
                final int subtaskIndex,
                final HivePartition bucketId,
                final Path bucketPath,
                final long initialPartCounter,
                final PartFileWriter.PartFileFactory<Row, HivePartition> partFileWriterFactory,
                final RollingPolicy<Row, HivePartition> rollingPolicy) {
            final Bucket<Row, HivePartition> newBucket = super.getNewBucket(
                    fsWriter,
                    subtaskIndex,
                    bucketId,
                    bucketPath,
                    initialPartCounter,
                    partFileWriterFactory, rollingPolicy);
            newPartitions.add(newBucket.getBucketId());
            return newBucket;
        }
    }

    private static class PartitionFilterBulkWriter implements BulkWriter<Row> {

        private final BulkWriter<Row> bulkWriter;

        private final PartitionComputer<Row> partitionComputer;

        private PartitionFilterBulkWriter(BulkWriter<Row> bulkWriter, PartitionComputer<Row> partitionComputer) {
            this.bulkWriter = checkNotNull(bulkWriter);
            this.partitionComputer = checkNotNull(partitionComputer);
        }

        @Override
        public void addElement(Row row) throws IOException {
            bulkWriter.addElement(partitionComputer.projectColumnsToWrite(row));
        }

        @Override
        public void flush() throws IOException {
            bulkWriter.flush();
        }

        @Override
        public void finish() throws IOException {
            bulkWriter.finish();
        }
    }
}
