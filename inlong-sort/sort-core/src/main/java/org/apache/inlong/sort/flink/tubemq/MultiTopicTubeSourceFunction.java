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

package org.apache.inlong.sort.flink.tubemq;

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.common.annotations.VisibleForTesting;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.OperatorStateStore;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.inlong.sort.configuration.Configuration;
import org.apache.inlong.sort.configuration.Constants;
import org.apache.inlong.sort.flink.SerializedRecord;
import org.apache.inlong.sort.meta.MetaManager;
import org.apache.inlong.sort.meta.MetaManager.DataFlowInfoListener;
import org.apache.inlong.sort.protocol.DataFlowInfo;
import org.apache.inlong.sort.protocol.source.TubeSourceInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MultiTopicTubeSourceFunction
        extends RichParallelSourceFunction<SerializedRecord> implements CheckpointedFunction {

    private static final long serialVersionUID = 3074838212472244415L;

    private static final Logger LOG = LoggerFactory.getLogger(MultiTopicTubeSourceFunction.class);

    private final Configuration configuration;

    private volatile boolean running = true;

    private volatile Throwable throwable;

    private transient MultiTenancyTubeConsumer tubeConsumer;

    private transient ListState<Tuple3<String, String, Long>> offsetsState;

    private transient Map<String, Map<String, Long>> currentOffsets;

    private transient BlockingQueue<SourceEvent> eventQueue;

    /**
     * Used to keep the current subscription descriptions.
     */
    private transient Map<String, TubeSubscriptionDescription> tubeTopicSubscriptions;

    public enum SourceEventType {
        ADDED,
        REMOVED
    }

    public MultiTopicTubeSourceFunction(Configuration configuration) {
        this.configuration = checkNotNull(configuration);
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        TupleTypeInfo<Tuple3<String, String, Long>> typeInformation = new TupleTypeInfo<>(
                BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.LONG_TYPE_INFO);
        ListStateDescriptor<Tuple3<String, String, Long>> stateDescriptor = new ListStateDescriptor<>(
                "tube-offset-state", typeInformation);
        OperatorStateStore stateStore = context.getOperatorStateStore();
        offsetsState = stateStore.getListState(stateDescriptor);
        currentOffsets = new HashMap<>();
        tubeTopicSubscriptions = new HashMap<>();

        if (context.isRestored()) {
            for (Tuple3<String, String, Long> tubeOffset : this.offsetsState.get()) {
                String topic = tubeOffset.f0;
                String partition = tubeOffset.f1;
                long offset = tubeOffset.f2;

                if (currentOffsets.containsKey(topic)) {
                    currentOffsets.get(topic).put(partition, offset);
                } else {
                    HashMap<String, Long> partitionToOffset = new HashMap<>();
                    partitionToOffset.put(partition, offset);
                    currentOffsets.put(topic, partitionToOffset);
                }
            }

            LOG.info("Successfully restore the offsets {}.", this.currentOffsets);
        } else {
            LOG.info("No restore offsets.");
        }
    }

    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        this.offsetsState.clear();

        currentOffsets = tubeConsumer.getTopicToOffset();
        for (Entry<String, Map<String, Long>> entry1 : currentOffsets.entrySet()) {
            for (Entry<String, Long> entry2 : entry1.getValue().entrySet()) {
                this.offsetsState.add(Tuple3.of(entry1.getKey(), entry2.getKey(), entry2.getValue()));
            }
        }

        LOG.info("Successfully save the offsets in checkpoint {} : {}.",
                context.getCheckpointId(), this.currentOffsets);
    }

    @Override
    public void open(org.apache.flink.configuration.Configuration flinkConf) throws Exception {
        eventQueue = new ArrayBlockingQueue<>(configuration.getInteger(Constants.SOURCE_EVENT_QUEUE_CAPACITY));
        getRuntimeContext().getMetricGroup().gauge("source_event_queue_length", () -> eventQueue.size());
        running = true;
        tubeConsumer = new MultiTenancyTubeConsumer(
                configuration,
                currentOffsets,
                getRuntimeContext().getNumberOfParallelSubtasks(),
                throwable -> this.throwable = throwable);
        final MetaManager metaManager = MetaManager.getInstance(configuration);
        metaManager.registerDataFlowInfoListener(new DataFlowInfoListenerImpl());
    }

    @Override
    public void run(SourceContext<SerializedRecord> ctx) throws Exception {
        tubeConsumer.start(ctx);
        // the preview is used to aggregate source events into topic level
        // because it's efficient to do the TubeMQ operation in topic level
        Map<String, TubeSubscriptionDescription> tubeTopicSubscriptionsPreview = null;

        while (running) {
            if (throwable != null) {
                throw new Exception(throwable);
            }
            // TODO, make poll timeout configurable
            final SourceEvent event = eventQueue.poll(1000, TimeUnit.MILLISECONDS);
            if (!running) {
                break;
            }
            if (event != null) {
                if (tubeTopicSubscriptionsPreview == null) {
                    tubeTopicSubscriptionsPreview = cloneTubeTopicSubscriptionInfos(tubeTopicSubscriptions);
                }
                processSourceEvent(event, tubeTopicSubscriptionsPreview);
            } else if (tubeTopicSubscriptionsPreview != null) {
                commitTubeSubscriptionDescriptionPreview(tubeTopicSubscriptionsPreview);
                tubeTopicSubscriptionsPreview = null;
            }
        }
    }

    @VisibleForTesting
    void processSourceEvent(SourceEvent event, Map<String, TubeSubscriptionDescription> tubeTopicSubscriptionsPreview) {
        final String topic = event.getTubeSourceInfo().getTopic();
        final TubeSubscriptionDescription subscriptionInfo = tubeTopicSubscriptionsPreview.get(topic);
        switch (event.getSourceEventType()) {
            case ADDED:
                if (subscriptionInfo != null) {
                    subscriptionInfo.addDataFlow(event.getDataFlowId(), event.getTubeSourceInfo());
                } else {
                    tubeTopicSubscriptionsPreview.put(topic, TubeSubscriptionDescription
                            .generate(event.getDataFlowId(), event.getTubeSourceInfo()));
                }
                break;
            case REMOVED:
                if (subscriptionInfo != null) {
                    subscriptionInfo.removeDataFlow(event.getDataFlowId());
                    if (subscriptionInfo.isEmpty()) {
                        tubeTopicSubscriptionsPreview.remove(topic);
                        try {
                            // do the removal immediately to keep consistence of removing + re-adding
                            // (the offset must be cleaned)
                            tubeConsumer.removeTopic(topic);
                        } catch (Exception e) {
                            LOG.warn("Remove topic of {} failed", topic, e);
                        }
                    }
                }
                break;
            default:
                LOG.error("Unknown source event type {}", event.sourceEventType);
                throw new RuntimeException("Unknown source event type " + event.sourceEventType);
        }
    }

    @VisibleForTesting
    static Map<String, TubeSubscriptionDescription> cloneTubeTopicSubscriptionInfos(
            Map<String, TubeSubscriptionDescription> tubeTopicSubscriptions) {
        final Map<String, TubeSubscriptionDescription> tubeTopicSubscriptionsSnapshot = new HashMap<>();
        tubeTopicSubscriptions.forEach(
                (key, value) -> tubeTopicSubscriptionsSnapshot.put(key, new TubeSubscriptionDescription(value)));
        return tubeTopicSubscriptionsSnapshot;
    }

    /**
     * Finds all difference between preview and current version, and then applies the change.
     *
     * @param tubeTopicSubscriptionsPreview the preview subscriptions
     */
    @VisibleForTesting
    void commitTubeSubscriptionDescriptionPreview(
            Map<String, TubeSubscriptionDescription> tubeTopicSubscriptionsPreview) {
        for (TubeSubscriptionDescription subscriptionDescription : tubeTopicSubscriptionsPreview.values()) {
            final TubeSubscriptionDescription existingSubscriptionDescription = tubeTopicSubscriptions
                    .get(subscriptionDescription.getTopic());
            if (existingSubscriptionDescription == null) {
                try {
                    // a new topic found
                    tubeConsumer.addTopic(subscriptionDescription);
                } catch (Exception e) {
                    LOG.warn("Add topic of {} failed", subscriptionDescription, e);
                }
            } else if (!existingSubscriptionDescription.equals(subscriptionDescription)) {
                try {
                    // the subscription changes, update it
                    tubeConsumer.updateTopic(subscriptionDescription);
                } catch (Exception e) {
                    LOG.warn("Update topic of {} failed", subscriptionDescription, e);
                }
            }
        }
        // we don't need to handle removed topics because it has been done in last step

        // replace the subscriptions with preview
        tubeTopicSubscriptions = tubeTopicSubscriptionsPreview;
    }

    public void cancel() {
        LOG.info("Start cancelling the tubeMQ source");
        this.running = false;
        tubeConsumer.cancel();
    }

    public void close() throws Exception {
        super.close();
        tubeConsumer.close();

        LOG.info("Closed the tubeMQ source.");
    }

    public void queueEvent(SourceEventType eventType, DataFlowInfo dataFlowInfo) throws InterruptedException {
        if (dataFlowInfo.getSourceInfo() instanceof TubeSourceInfo) {
            eventQueue.put(new SourceEvent(eventType, dataFlowInfo.getId(),
                    (TubeSourceInfo) dataFlowInfo.getSourceInfo()));
        } else {
            LOG.warn("Received a non-tubeMQ data flow notification {}: {}", eventType, dataFlowInfo);
        }
    }

    @VisibleForTesting
    void setTubeConsumer(MultiTenancyTubeConsumer tubeConsumer) {
        this.tubeConsumer = tubeConsumer;
    }

    @VisibleForTesting
    void setTubeTopicSubscriptions(Map<String, TubeSubscriptionDescription> tubeTopicSubscriptions) {
        this.tubeTopicSubscriptions = tubeTopicSubscriptions;
    }

    @VisibleForTesting
    Map<String, TubeSubscriptionDescription> getTubeTopicSubscriptions() {
        return tubeTopicSubscriptions;
    }

    private class DataFlowInfoListenerImpl implements DataFlowInfoListener {

        @Override
        public void addDataFlow(DataFlowInfo dataFlowInfo) throws InterruptedException {
            queueEvent(SourceEventType.ADDED, dataFlowInfo);
        }

        @Override
        public void updateDataFlow(DataFlowInfo dataFlowInfo) throws InterruptedException {
            queueEvent(SourceEventType.ADDED, dataFlowInfo);
        }

        @Override
        public void removeDataFlow(DataFlowInfo dataFlowInfo) throws InterruptedException {
            queueEvent(SourceEventType.REMOVED, dataFlowInfo);
        }
    }

    public static class SourceEvent implements Serializable {

        private static final long serialVersionUID = 868933715908030560L;

        private final SourceEventType sourceEventType;

        private final long dataFlowId;

        private final TubeSourceInfo tubeSourceInfo;

        public SourceEvent(SourceEventType sourceEventType, long dataFlowId, TubeSourceInfo tubeSourceInfo) {
            this.sourceEventType = sourceEventType;
            this.dataFlowId = dataFlowId;
            this.tubeSourceInfo = checkNotNull(tubeSourceInfo);
        }

        public SourceEventType getSourceEventType() {
            return sourceEventType;
        }

        public TubeSourceInfo getTubeSourceInfo() {
            return tubeSourceInfo;
        }

        public long getDataFlowId() {
            return dataFlowId;
        }

        @Override
        public String toString() {
            return new ToStringBuilder(this, ToStringStyle.SHORT_PREFIX_STYLE)
                    .append("sourceEventType", sourceEventType)
                    .append("dataFlowId", dataFlowId)
                    .append("tubeSourceInfo", tubeSourceInfo)
                    .toString();
        }
    }
}
