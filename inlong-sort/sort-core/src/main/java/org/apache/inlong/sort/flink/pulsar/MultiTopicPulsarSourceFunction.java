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

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.inlong.sort.configuration.Configuration;
import org.apache.inlong.sort.configuration.Constants;
import org.apache.inlong.sort.flink.SerializedRecord;
import org.apache.inlong.sort.flink.SourceEvent;
import org.apache.inlong.sort.flink.SourceEvent.SourceEventType;
import org.apache.inlong.sort.flink.multitenant.MultiTenantFunctionInitializationContext;
import org.apache.inlong.sort.flink.tubemq.MultiTopicTubeSourceFunction;
import org.apache.inlong.sort.meta.MetaManager;
import org.apache.inlong.sort.protocol.DataFlowInfo;
import org.apache.inlong.sort.protocol.source.PulsarSourceInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MultiTopicPulsarSourceFunction extends RichParallelSourceFunction<SerializedRecord> implements
        CheckpointedFunction {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(MultiTopicTubeSourceFunction.class);

    private final Configuration configuration;
    private volatile boolean running = true;
    private volatile Throwable throwable;

    private transient MetaManager metaManager;
    private transient MultiTenancyPulsarConsumer<SerializedRecord> pulsarConsumer;
    private transient FunctionInitializationContext functionInitializationContext;
    private transient BlockingQueue<SourceEvent> eventQueue;

    public MultiTopicPulsarSourceFunction(Configuration configuration) {
        this.configuration = configuration;
    }

    @Override
    public void open(org.apache.flink.configuration.Configuration parameters) throws Exception {
        super.open(parameters);
        eventQueue = new ArrayBlockingQueue<>(configuration.getInteger(Constants.SOURCE_EVENT_QUEUE_CAPACITY));
        this.metaManager = MetaManager.getInstance(configuration);
        this.metaManager.registerDataFlowInfoListener(new PulsarDataFlowInfoListener());
        this.pulsarConsumer = new MultiTenancyPulsarConsumer<>(throwable -> this.throwable = throwable);
    }

    @Override
    public void initializeState(FunctionInitializationContext functionInitializationContext) throws Exception {
        this.functionInitializationContext = functionInitializationContext;
    }

    @Override
    public void snapshotState(FunctionSnapshotContext functionSnapshotContext) throws Exception {

        Collection<PulsarSourceFunction<SerializedRecord>> currentSourceFunction = pulsarConsumer
                .getCurrentSourceFunction();
        for (PulsarSourceFunction<SerializedRecord> writer : currentSourceFunction) {
            writer.snapshotState(functionSnapshotContext);
        }
    }

    @Override
    public void run(SourceContext<SerializedRecord> sourceContext) throws Exception {
        pulsarConsumer.start(sourceContext);
        while (running) {
            if (throwable != null) {
                throw new RuntimeException(throwable);
            }
            SourceEvent sourceEvent = eventQueue.poll(1000, TimeUnit.MILLISECONDS);
            if (sourceEvent != null) {
                processEvent(sourceEvent);
            }
            if (!running) {
                return;
            }
        }
    }

    private void processEvent(SourceEvent sourceEvent) throws Exception {
        SourceEventType sourceEventType = sourceEvent.getSourceEventType();
        PulsarSourceInfo pulsarSourceInfo = (PulsarSourceInfo) sourceEvent.getSourceInfo();
        Map<String, Object> properties = sourceEvent.getProperties();
        long dataFlowId = sourceEvent.getDataFlowId();

        switch (sourceEventType) {
            case ADDED:
                PulsarSourceFunction<SerializedRecord> pulsarSourceFunction = generateSourceFunction(dataFlowId,
                        properties, pulsarSourceInfo);
                pulsarConsumer.addPulsarSource(dataFlowId, pulsarSourceFunction);
                break;
            case UPDATE:
                PulsarSourceFunction<SerializedRecord> updateSourceFunction = generateSourceFunction(dataFlowId,
                        properties, pulsarSourceInfo);
                pulsarConsumer.updatePulsarSource(dataFlowId, updateSourceFunction);
                break;
            case REMOVED:
                pulsarConsumer.removePulsarSource(dataFlowId);
                break;
            default:
                LOG.error("Unknown source event type {}", sourceEvent.getSourceEventType());
                throw new RuntimeException("Unknown source event type " + sourceEvent.getSourceEventType());
        }
    }

    public PulsarSourceFunction<SerializedRecord> generateSourceFunction(
            long dataFlowId,
            Map<String, Object> properties,
            PulsarSourceInfo pulsarSourceInfo) throws Exception {
        org.apache.flink.configuration.Configuration config =
                new org.apache.flink.configuration.Configuration();
        putMapToConfig(config, properties);
        PulsarSourceFunction<SerializedRecord> pulsarSourceFunction = new PulsarSourceFunction<>(
                pulsarSourceInfo.getAdminUrl(),
                pulsarSourceInfo.getServiceUrl(),
                pulsarSourceInfo.getTopic(),
                pulsarSourceInfo.getSubscriptionName(),
                new SerializedRecordDeserializationSchema(dataFlowId),
                config);
        pulsarSourceFunction.setRuntimeContext(getRuntimeContext());
        pulsarSourceFunction.initializeState(
                new MultiTenantFunctionInitializationContext(dataFlowId, functionInitializationContext,
                        getRuntimeContext().getExecutionConfig()
                ));
        pulsarSourceFunction.open(config);
        return pulsarSourceFunction;

    }

    public void putMapToConfig(org.apache.flink.configuration.Configuration config, Map<String, Object> properties) {
        for (Map.Entry<String, Object> entry : properties.entrySet()) {
            if (entry.getKey().startsWith(Constants.PULSAR_SOURCE_PREFIX)) {
                config.setString(entry.getKey().replaceFirst(Constants.PULSAR_SOURCE_PREFIX, ""),
                        entry.getValue().toString());
            }
        }
    }

    @Override
    public void cancel() {
        this.running = false;
        pulsarConsumer.cancel();
    }

    @Override
    public void close() throws Exception {
        super.close();
        pulsarConsumer.close();
        MetaManager.release();
    }

    private class PulsarDataFlowInfoListener implements MetaManager.DataFlowInfoListener {

        public void queueEvent(SourceEventType eventType, DataFlowInfo dataFlowInfo) throws InterruptedException {
            if (dataFlowInfo.getSourceInfo() instanceof PulsarSourceInfo) {
                eventQueue.put(new SourceEvent(eventType, dataFlowInfo.getId(),
                        (PulsarSourceInfo) dataFlowInfo.getSourceInfo(), dataFlowInfo.getProperties()));
            } else {
                LOG.warn("Received a non-pulsar data flow notification {}: {}", eventType, dataFlowInfo);
            }
        }

        @Override
        public void addDataFlow(DataFlowInfo dataFlowInfo) throws Exception {
            queueEvent(SourceEventType.ADDED, dataFlowInfo);
        }


        @Override
        public void updateDataFlow(DataFlowInfo dataFlowInfo) throws Exception {
            queueEvent(SourceEventType.UPDATE, dataFlowInfo);
        }

        @Override
        public void removeDataFlow(DataFlowInfo dataFlowInfo) throws Exception {
            queueEvent(SourceEventType.REMOVED, dataFlowInfo);
        }
    }
}
