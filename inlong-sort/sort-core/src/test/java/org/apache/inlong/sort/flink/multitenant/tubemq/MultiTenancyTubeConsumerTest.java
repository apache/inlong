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

package org.apache.inlong.sort.flink.multitenant.tubemq;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.inlong.sort.configuration.Configuration;
import org.apache.inlong.sort.configuration.Constants;
import org.apache.inlong.sort.flink.SerializedRecord;
import org.apache.inlong.sort.flink.multitenant.tubemq.MultiTenancyTubeConsumer.TubePullThread;
import org.apache.inlong.tubemq.client.config.ConsumerConfig;
import org.apache.inlong.tubemq.client.consumer.ConsumeOffsetInfo;
import org.apache.inlong.tubemq.client.consumer.ConsumerResult;
import org.apache.inlong.tubemq.client.consumer.FetchContext;
import org.apache.inlong.tubemq.client.consumer.PartitionSelectResult;
import org.apache.inlong.tubemq.client.consumer.PullMessageConsumer;
import org.apache.inlong.tubemq.client.exception.TubeClientException;
import org.apache.inlong.tubemq.corebase.Message;
import org.apache.inlong.tubemq.corebase.cluster.BrokerInfo;
import org.apache.inlong.tubemq.corebase.cluster.Partition;
import org.junit.Test;

public class MultiTenancyTubeConsumerTest {

    @Test
    public void testTubePullThread() throws Exception {
        final Configuration config = new Configuration();
        config.setString(Constants.CLUSTER_ID, "ci");
        config.setString(Constants.TUBE_MASTER_ADDRESS, "ma");
        Map<String, Map<String, Long>> topicToOffset = new HashMap<>();
        topicToOffset.put("topic", new HashMap<>());
        MultiTenancyTubeConsumer tubeConsumerWrapper = new MultiTenancyTubeConsumer(
                config, topicToOffset, 1, Throwable::printStackTrace);
        TestContext context = new TestContext();
        tubeConsumerWrapper.start(context);

        PullMessageConsumer mockPullMessageConsumer = mock(PullMessageConsumer.class);
        TubePullThread tubePullThread = tubeConsumerWrapper.new TubePullThread(
                mockPullMessageConsumer, "topic", new ArrayList<>(), topicToOffset.get("topic"), 1024);

        ConsumerResult consumerResult1 = constructConsumerResult(
                "topic", 1, 200, "data1");
        ConsumerResult consumerResult2 = constructConsumerResult(
                "topic", 50, 200, "data2");
        ConsumerResult consumerResult3 = constructConsumerResult(
                "topic", 100, 200, "data3");

        when(mockPullMessageConsumer.getMessage()).thenReturn(
                consumerResult1, consumerResult2, consumerResult3);
        when(mockPullMessageConsumer.confirmConsume("confirmContext", true))
                .thenReturn(consumerResult1, consumerResult2, consumerResult3);
        tubePullThread.start();

        while (true) {
            if (context.getCount() < 3) {
                //noinspection BusyWait
                Thread.sleep(100);
            } else {
                assertEquals(topicToOffset.size(), 1);
                assertEquals((long)topicToOffset.get("topic").get("111:topic:222"), 100L);

                tubePullThread.cancel();
                tubePullThread.interrupt();
                tubePullThread.join();
                break;
            }
        }
    }

    @Test
    public void testSubscribe() throws Exception {
        final Configuration config = new Configuration();
        config.setString(Constants.CLUSTER_ID, "ci");
        final Map<String, Map<String, Long>> topicToOffset = new HashMap<>();
        topicToOffset.put("topic", new HashMap<>());
        final MultiTenancyTubeConsumer tubeConsumerWrapper = new MultiTenancyTubeConsumer(
                config, topicToOffset, 1, Throwable::printStackTrace);
        tubeConsumerWrapper.start(new TestContext());

        final TestingPullMessageConsumer pullMessageConsumer = new TestingPullMessageConsumer();
        final long subscribeTimeout = 1024L;
        final TubePullThread tubePullThread = tubeConsumerWrapper.new TubePullThread(
                pullMessageConsumer, "topic", new ArrayList<>(), topicToOffset.get("topic"), subscribeTimeout);
        tubePullThread.subscribe();
    }

    @Test
    public void testSubscribeFailed() throws Exception {
        final Configuration config = new Configuration();
        config.setString(Constants.CLUSTER_ID, "ci");
        final Map<String, Map<String, Long>> topicToOffset = new HashMap<>();
        topicToOffset.put("topic", new HashMap<>());
        final MultiTenancyTubeConsumer tubeConsumerWrapper = new MultiTenancyTubeConsumer(
                config, topicToOffset, 1, Throwable::printStackTrace);
        tubeConsumerWrapper.start(new TestContext());

        final TestingPullMessageConsumer pullMessageConsumer = new TestingPullMessageConsumer();
        final TubeClientException cause = new TubeClientException("Failed by design");
        pullMessageConsumer.completeSubscribeException = cause;
        final long subscribeTimeout = 1024L;
        final TubePullThread tubePullThread = tubeConsumerWrapper.new TubePullThread(
                pullMessageConsumer, "topic", new ArrayList<>(), topicToOffset.get("topic"), subscribeTimeout);
        final long beforeSubscription = System.currentTimeMillis();
        try {
            tubePullThread.subscribe();
        } catch (TubeClientException e) {
            assertEquals(cause, e);
        }
        final long used = System.currentTimeMillis() - beforeSubscription;
        assertTrue(used >= subscribeTimeout);
    }

    private ConsumerResult constructConsumerResult(
            String topic,
            long currentOffset,
            long maxOffset,
            String data) {
        BrokerInfo brokerInfo = new BrokerInfo(111, "localhost", 8000);
        Partition partition = new Partition(brokerInfo, topic, 222);
        PartitionSelectResult partitionSelectResult = new PartitionSelectResult(
                partition, 1000, true);
        FetchContext fetchContext = new FetchContext(partitionSelectResult);

        List<Message> messageList = new ArrayList<>();
        messageList.add(new Message(topic, data.getBytes()));
        fetchContext.setSuccessProcessResult(currentOffset, "confirmContext", messageList, maxOffset);

        return new ConsumerResult(fetchContext);
    }

    private static class TestContext implements SourceFunction.SourceContext<SerializedRecord> {
        private final Object lock = new Object();

        private int count = 0;

        @Override
        public void collect(SerializedRecord record) {
            count++;
            try {
                Thread.sleep(500);
            } catch (InterruptedException ignored) {
                // do nothing
            }
        }

        @Override
        public void collectWithTimestamp(SerializedRecord record, long l) {

        }

        @Override
        public void emitWatermark(Watermark watermark) {

        }

        @Override
        public void markAsTemporarilyIdle() {

        }

        @Override
        public Object getCheckpointLock() {
            return lock;
        }

        @Override
        public void close() {

        }

        public int getCount() {
            synchronized (lock) {
                return count;
            }
        }
    }

    private static class TestingPullMessageConsumer implements PullMessageConsumer {

        private TubeClientException completeSubscribeException = null;

        @Override
        public boolean isPartitionsReady(long l) {
            throw new UnsupportedOperationException("Not implemented");
        }

        @Override
        public PullMessageConsumer subscribe(String s, TreeSet<String> treeSet) throws TubeClientException {
            return this;
        }

        @Override
        public ConsumerResult getMessage() throws TubeClientException {
            throw new UnsupportedOperationException("Not implemented");
        }

        @Override
        public ConsumerResult confirmConsume(String s, boolean b) throws TubeClientException {
            throw new UnsupportedOperationException("Not implemented");
        }

        @Override
        public String getClientVersion() {
            throw new UnsupportedOperationException("Not implemented");
        }

        @Override
        public String getConsumerId() {
            throw new UnsupportedOperationException("Not implemented");
        }

        @Override
        public boolean isShutdown() {
            throw new UnsupportedOperationException("Not implemented");
        }

        @Override
        public ConsumerConfig getConsumerConfig() {
            throw new UnsupportedOperationException("Not implemented");
        }

        @Override
        public boolean isFilterConsume(String s) {
            throw new UnsupportedOperationException("Not implemented");
        }

        @Override
        public Map<String, ConsumeOffsetInfo> getCurConsumedPartitions() throws TubeClientException {
            throw new UnsupportedOperationException("Not implemented");
        }

        @Override
        public void freezePartitions(List<String> list) throws TubeClientException {
            throw new UnsupportedOperationException("Not implemented");
        }

        @Override
        public void unfreezePartitions(List<String> list) throws TubeClientException {
            throw new UnsupportedOperationException("Not implemented");
        }

        @Override
        public void relAllFrozenPartitions() {
            throw new UnsupportedOperationException("Not implemented");
        }

        @Override
        public Map<String, Long> getFrozenPartInfo() {
            throw new UnsupportedOperationException("Not implemented");
        }

        @Override
        public void completeSubscribe() throws TubeClientException {
            if (completeSubscribeException != null) {
                throw completeSubscribeException;
            }
        }

        @Override
        public void completeSubscribe(String s, int i, boolean b, Map<String, Long> map) throws TubeClientException {
            if (completeSubscribeException != null) {
                throw completeSubscribeException;
            }
        }

        @Override
        public void shutdown() throws Throwable {
            throw new UnsupportedOperationException("Not implemented");
        }
    }
}
