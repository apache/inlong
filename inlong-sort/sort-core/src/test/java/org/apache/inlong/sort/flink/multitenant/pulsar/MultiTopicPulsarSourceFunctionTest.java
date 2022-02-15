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

package org.apache.inlong.sort.flink.multitenant.pulsar;

import static org.junit.Assert.assertEquals;

import java.io.ByteArrayOutputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.KeyedStateStore;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.OperatorStateStore;
import org.apache.flink.api.java.typeutils.runtime.RowSerializer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.core.testutils.CheckedThread;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.MockStreamingRuntimeContext;
import org.apache.flink.types.Row;
import org.apache.inlong.sort.flink.SerializedRecord;
import org.apache.inlong.sort.formats.common.FormatInfo;
import org.apache.inlong.sort.formats.common.RowFormatInfo;
import org.apache.inlong.sort.formats.common.StringFormatInfo;
import org.apache.inlong.sort.util.CommonUtils;
import org.apache.inlong.sort.util.TestMetaManagerUtil;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.testcontainers.containers.PulsarContainer;
import org.testcontainers.utility.DockerImageName;

@Ignore
public class MultiTopicPulsarSourceFunctionTest {

    public static final String TEST_TOPIC = "test_topic";
    public static final String CONSUMER_GROUP = "test";
    private static final int TOTAL_COUNT = 5;
    private static final DockerImageName PULSAR_IMAGE = DockerImageName.parse("apachepulsar/pulsar:2.2.0");
    private PulsarContainer pulsar;
    private TestMetaManagerUtil testMetaManagerUtil;
    private StreamExecutionEnvironment env;

    @Before
    public void startPulsarContainer() throws Exception {
        pulsar = new PulsarContainer(PULSAR_IMAGE);
        pulsar.start();
        producerMessageToPulsar(pulsar.getPulsarBrokerUrl());
        testMetaManagerUtil = new PulsarTestMetaManagerUtil();
        testMetaManagerUtil
                .initDataFlowInfo(pulsar.getHttpServiceUrl(), pulsar.getPulsarBrokerUrl(), TEST_TOPIC, CONSUMER_GROUP);
    }

    @Test
    public void testPulsarSourceFunction() throws Exception {
        MultiTopicPulsarSourceFunction source = new MultiTopicPulsarSourceFunction(
                testMetaManagerUtil.getConfig());
        TestSourceContext<SerializedRecord> sourceContext = new TestSourceContext<>();
        source.setRuntimeContext(
                new MockStreamingRuntimeContext(false, 1, 0));

        source.initializeState(
                new MockFunctionInitializationContext(
                        false,
                        new MockOperatorStateStore(null, null)));
        source.open(new Configuration());
        final CheckedThread runThread =
                new CheckedThread() {
                    @Override
                    public void go() throws Exception {
                        source.run(sourceContext);
                    }
                };
        runThread.start();
        List<SerializedRecord> records = drain(sourceContext, 5);
        assertEquals(5, records.size());
        source.cancel();
        source.close();
        runThread.stop();
    }

    protected void producerMessageToPulsar(String pulsarBrokerUrl) throws Exception {
        RowSerializer rowSerializer = CommonUtils.generateRowSerializer(new RowFormatInfo(
                new String[] {"f1", "f2"},
                new FormatInfo[] {StringFormatInfo.INSTANCE, StringFormatInfo.INSTANCE}));
        DataOutputSerializer dataOutputSerializer = new DataOutputSerializer(1024);

        try (
                PulsarClient client = PulsarClient.builder()
                        .serviceUrl(pulsarBrokerUrl)
                        .build();
                Producer<byte[]> producer = client.newProducer()
                        .topic(TEST_TOPIC)
                        .create()
        ) {
            for (int cnt = 0; cnt < TOTAL_COUNT; cnt++) {
                Row row = Row.of(String.valueOf(cnt), String.valueOf(cnt));
                rowSerializer.serialize(row, dataOutputSerializer);
                ByteArrayOutputStream byt = new ByteArrayOutputStream();
                ObjectOutputStream objectOutputStream = new ObjectOutputStream(byt);
                objectOutputStream.writeObject(new SerializedRecord(1L, 0, dataOutputSerializer.getCopyOfBuffer()));
                producer.send(byt.toByteArray());
            }
        }
    }

    @After
    public void stopPulsarContainer() {
        if (pulsar != null) {
            pulsar.close();
        }
    }

    private static <T> List<T> drain(TestSourceContext<T> sourceContext, int expectedRecordCount)
            throws Exception {
        List<T> allRecords = new ArrayList<>();
        LinkedBlockingQueue<StreamRecord<T>> queue = sourceContext.getCollectedOutputs();
        while (allRecords.size() < expectedRecordCount) {
            StreamRecord<T> record = queue.poll(100, TimeUnit.SECONDS);
            if (record != null) {
                allRecords.add(record.getValue());
            } else {
                throw new RuntimeException(
                        "Can't receive " + expectedRecordCount + " elements before timeout.");
            }
        }

        return allRecords;
    }

    private static class MockOperatorStateStore implements OperatorStateStore {

        private final ListState<?> restoredOffsetListState;
        private final ListState<?> restoredHistoryListState;

        private MockOperatorStateStore(
                ListState<?> restoredOffsetListState, ListState<?> restoredHistoryListState) {
            this.restoredOffsetListState = restoredOffsetListState;
            this.restoredHistoryListState = restoredHistoryListState;
        }

        @Override
        @SuppressWarnings("unchecked")
        public <S> ListState<S> getUnionListState(ListStateDescriptor<S> stateDescriptor)
                throws Exception {
            return null;
        }

        @Override
        public <K, V> BroadcastState<K, V> getBroadcastState(
                MapStateDescriptor<K, V> stateDescriptor) throws Exception {
            throw new UnsupportedOperationException();
        }

        @Override
        public <S> ListState<S> getListState(ListStateDescriptor<S> stateDescriptor)
                throws Exception {
            throw new UnsupportedOperationException();
        }

        @Override
        public Set<String> getRegisteredStateNames() {
            throw new UnsupportedOperationException();
        }

        @Override
        public Set<String> getRegisteredBroadcastStateNames() {
            throw new UnsupportedOperationException();
        }
    }

    private static class MockFunctionInitializationContext
            implements FunctionInitializationContext {

        private final boolean isRestored;
        private final OperatorStateStore operatorStateStore;

        private MockFunctionInitializationContext(
                boolean isRestored, OperatorStateStore operatorStateStore) {
            this.isRestored = isRestored;
            this.operatorStateStore = operatorStateStore;
        }

        @Override
        public boolean isRestored() {
            return isRestored;
        }

        @Override
        public OperatorStateStore getOperatorStateStore() {
            return operatorStateStore;
        }

        @Override
        public KeyedStateStore getKeyedStateStore() {
            throw new UnsupportedOperationException();
        }
    }
}
