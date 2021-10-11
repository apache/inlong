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

import com.google.common.base.Preconditions;
import java.util.HashMap;
import java.util.Map;
import org.apache.flink.runtime.state.CheckpointListener;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.apache.inlong.sort.configuration.Configuration;
import org.apache.inlong.sort.configuration.Constants;
import org.apache.inlong.sort.flink.SerializedRecord;
import org.apache.inlong.sort.flink.hive.partition.PartitionCommitInfo;
import org.apache.inlong.sort.flink.multitenant.MultiTenantFunctionInitializationContext;
import org.apache.inlong.sort.flink.transformation.RecordTransformer;
import org.apache.inlong.sort.meta.MetaManager;
import org.apache.inlong.sort.meta.MetaManager.DataFlowInfoListener;
import org.apache.inlong.sort.protocol.DataFlowInfo;
import org.apache.inlong.sort.protocol.sink.HiveSinkInfo;
import org.apache.inlong.sort.protocol.sink.SinkInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings("SynchronizeOnNonFinalField")
public class HiveMultiTenantWriter extends ProcessFunction<SerializedRecord, PartitionCommitInfo>
        implements CheckpointedFunction, CheckpointListener {

    private static final long serialVersionUID = -3254800832674516362L;

    private static final Logger LOG = LoggerFactory.getLogger(HiveMultiTenantWriter.class);

    private final Configuration configuration;

    private transient Map<Long, HiveWriter> hiveWriters;

    private transient RecordTransformer recordTransformer;

    private transient FunctionInitializationContext functionInitializationContext;

    private transient ProcessFunctionContext proxyContext;

    public HiveMultiTenantWriter(Configuration configuration) {
        this.configuration = Preconditions.checkNotNull(configuration);
    }

    @Override
    public void open(org.apache.flink.configuration.Configuration parameters) throws Exception {
        hiveWriters = new HashMap<>();
        recordTransformer = new RecordTransformer(
                configuration.getInteger(Constants.ETL_RECORD_SERIALIZATION_BUFFER_SIZE));
        MetaManager metaManager = MetaManager.getInstance(configuration);
        metaManager.registerDataFlowInfoListener(new DataFlowInfoListenerImpl());
        proxyContext = new ProcessFunctionContext();
    }

    @Override
    public void close() throws Exception {
        MetaManager.release();
        if (hiveWriters == null) {
            return;
        }
        synchronized (hiveWriters) {
            for (HiveWriter writer : hiveWriters.values()) {
                writer.close();
            }
            hiveWriters.clear();
        }
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {
        synchronized (hiveWriters) {
            for (HiveWriter writer : hiveWriters.values()) {
                writer.notifyCheckpointComplete(checkpointId);
            }
        }
    }

    @Override
    public void snapshotState(FunctionSnapshotContext functionSnapshotContext) throws Exception {
        synchronized (hiveWriters) {
            for (HiveWriter writer : hiveWriters.values()) {
                writer.snapshotState(functionSnapshotContext);
            }
        }
    }

    @Override
    public void initializeState(FunctionInitializationContext functionInitializationContext) throws Exception {
        this.functionInitializationContext = functionInitializationContext;
    }

    @Override
    public void processElement(SerializedRecord serializedRecord, Context context,
            Collector<PartitionCommitInfo> collector) throws Exception {
        final long dataFlowId = serializedRecord.getDataFlowId();

        synchronized (hiveWriters) {
            final HiveWriter hiveWriter = hiveWriters.get(dataFlowId);
            if (hiveWriter == null) {
                LOG.warn("Cannot get DataFlowInfo with id {}", dataFlowId);
                return;
            }

            hiveWriter.processElement(recordTransformer.toRecord(serializedRecord).getRow(),
                    proxyContext.setContext(context), collector);
        }
    }

    private class ProcessFunctionContext extends ProcessFunction<Row, PartitionCommitInfo>.Context {

        private ProcessFunction<SerializedRecord, PartitionCommitInfo>.Context parentContext;

        private ProcessFunctionContext setContext(
                ProcessFunction<SerializedRecord, PartitionCommitInfo>.Context context) {
            parentContext = context;
            return this;
        }

        @Override
        public Long timestamp() {
            return parentContext.timestamp();
        }

        @Override
        public TimerService timerService() {
            return parentContext.timerService();
        }

        @Override
        public <X> void output(OutputTag<X> outputTag, X value) {
            parentContext.output(outputTag, value);
        }
    }

    private class DataFlowInfoListenerImpl implements DataFlowInfoListener {

        @Override
        public void addDataFlow(DataFlowInfo dataFlowInfo) throws Exception {
            synchronized (hiveWriters) {
                long dataFlowId = dataFlowInfo.getId();
                SinkInfo sinkInfo = dataFlowInfo.getSinkInfo();
                if (!(sinkInfo instanceof HiveSinkInfo)) {
                    LOG.error("SinkInfo type {} of dataFlow {} doesn't match application sink type 'hive'!",
                            sinkInfo.getClass(), dataFlowId);
                    return;
                }
                HiveSinkInfo hiveSinkInfo = (HiveSinkInfo) sinkInfo;
                HiveWriter hiveWriter = new HiveWriter(configuration, dataFlowId, hiveSinkInfo);
                hiveWriter.setRuntimeContext(getRuntimeContext());
                hiveWriter.initializeState(
                        new MultiTenantFunctionInitializationContext(dataFlowId, functionInitializationContext,
                                getRuntimeContext().getExecutionConfig()));
                hiveWriter.open(new org.apache.flink.configuration.Configuration());
                hiveWriters.put(dataFlowId, hiveWriter);
                recordTransformer.addDataFlow(dataFlowInfo);
            }
        }

        @Override
        public void updateDataFlow(DataFlowInfo dataFlowInfo) throws Exception {
            synchronized (hiveWriters) {
                removeDataFlow(dataFlowInfo);
                addDataFlow(dataFlowInfo);
            }
        }

        @Override
        public void removeDataFlow(DataFlowInfo dataFlowInfo) throws Exception {
            synchronized (hiveWriters) {
                final HiveWriter existingHiveWriter = hiveWriters.remove(dataFlowInfo.getId());
                if (existingHiveWriter != null) {
                    existingHiveWriter.close();
                }
                recordTransformer.removeDataFlow(dataFlowInfo);
            }
        }

    }
}
