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
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Consumer;
import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.flink.runtime.net.ConnectionUtils;
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext;
import org.apache.flink.util.TimeUtils;
import org.apache.inlong.sort.configuration.Configuration;
import org.apache.inlong.sort.configuration.Constants;
import org.apache.inlong.sort.flink.SerializedRecord;
import org.apache.inlong.sort.flink.TDMsgMixedSerializedRecord;
import org.apache.inlong.sort.meta.MetaManager;
import org.apache.inlong.sort.util.CommonUtils;
import org.apache.inlong.tubemq.client.config.ConsumerConfig;
import org.apache.inlong.tubemq.client.consumer.ConsumerResult;
import org.apache.inlong.tubemq.client.consumer.PullMessageConsumer;
import org.apache.inlong.tubemq.client.exception.TubeClientException;
import org.apache.inlong.tubemq.client.factory.TubeMultiSessionFactory;
import org.apache.inlong.tubemq.corebase.Message;
import org.apache.inlong.tubemq.corebase.TErrCodeConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MultiTenancyTubeConsumer {

    private static final Logger LOG = LoggerFactory.getLogger(MultiTenancyTubeConsumer.class);

    private final Duration messageNotFoundWaitPeriod;

    @Nullable
    private final String globalTubeMasterAddresses;

    private final String clusterId;

    private final boolean consumeFromMax;

    private final String sessionKey;

    private final Consumer<Throwable> exceptionCatcher;

    // topic -> (partition -> offset)
    @GuardedBy("context.getCheckpointLock()")
    private final Map<String, Map<String, Long>> topicToOffset;

    // topic -> TubePullThread
    private final ConcurrentMap<String, TubePullThread> tubePullThreads;

    // topic -> PullMessageConsumer
    private final Map<String, PullMessageConsumer> pullMessageConsumers;

    // masterAddress -> TubeSessionFactory
    private final Map<String, TubeMultiSessionFactory> tubeSessionFactories;

    private final int numTasks;

    private final long subscribeTimeout;

    public static final Set<Integer> TUBE_IGNORE_ERROR_CODE = new HashSet<>(Arrays.asList(
            TErrCodeConstants.BAD_REQUEST,
            TErrCodeConstants.NOT_FOUND,
            TErrCodeConstants.ALL_PARTITION_FROZEN,
            TErrCodeConstants.NO_PARTITION_ASSIGNED,
            TErrCodeConstants.ALL_PARTITION_WAITING,
            TErrCodeConstants.ALL_PARTITION_INUSE));

    private SourceContext<SerializedRecord> context;

    public MultiTenancyTubeConsumer(
            Configuration configuration,
            Map<String, Map<String, Long>> topicToOffset,
            int numTasks,
            Consumer<Throwable> exceptionCatcher) {
        this.messageNotFoundWaitPeriod = TimeUtils.parseDuration(configuration.getString(
                Constants.TUBE_MESSAGE_NOT_FOUND_WAIT_PERIOD));
        this.clusterId = checkNotNull(configuration.getString(Constants.CLUSTER_ID));
        this.consumeFromMax = configuration.getBoolean(Constants.TUBE_BOOTSTRAP_FROM_MAX);
        this.sessionKey = checkNotNull(configuration.getString(Constants.TUBE_SESSION_KEY));
        this.topicToOffset = checkNotNull(topicToOffset);
        this.numTasks = numTasks;
        this.exceptionCatcher = checkNotNull(exceptionCatcher);

        this.globalTubeMasterAddresses = configuration.getString(Constants.TUBE_MASTER_ADDRESS);
        this.subscribeTimeout = configuration.getLong(Constants.TUBE_SUBSCRIBE_RETRY_TIMEOUT);

        this.tubePullThreads = new ConcurrentHashMap<>();
        this.pullMessageConsumers = new HashMap<>();
        this.tubeSessionFactories = new HashMap<>();
    }

    public void start(SourceContext<SerializedRecord> context) throws Exception {
        this.context = checkNotNull(context);
    }

    public void cancel() {
        for (TubePullThread tubePullThread : tubePullThreads.values()) {
            tubePullThread.cancel();
            tubePullThread.interrupt();
        }
    }

    /**
     * Release tubePullThreads.
     * @throws Exception tubeMQ errors
     */
    public void close() throws Exception {
        MetaManager.release();

        cancel();
        for (TubePullThread tubePullThread : tubePullThreads.values()) {
            try {
                tubePullThread.join();
            } catch (InterruptedException e) {
                LOG.error("Could not cancel thread {}", tubePullThread.getName());
            }
        }
        tubePullThreads.clear();

        for (PullMessageConsumer messageConsumer : pullMessageConsumers.values()) {
            try {
                if (!messageConsumer.isShutdown()) {
                    messageConsumer.shutdown();
                }
            } catch (Throwable throwable) {
                LOG.warn("Could not properly shutdown the tubeMQ pull consumer.", throwable);
            }
        }
        pullMessageConsumers.clear();

        for (TubeMultiSessionFactory sessionFactory : tubeSessionFactories.values()) {
            try {
                sessionFactory.shutdown();
            } catch (Throwable throwable) {
                LOG.warn("Could not properly shutdown the tubeMQ session factory.", throwable);
            }
        }
        tubeSessionFactories.clear();
    }

    /**
     * Subscribe to a new topic.
     */
    public void addTopic(TubeSubscriptionDescription subscriptionDescription) throws Exception {
        if (tubePullThreads.containsKey(subscriptionDescription.getTopic())) {
            LOG.warn("Pull thread of topic {} has already been started", subscriptionDescription.getTopic());
            return;
        }
        final String topic = subscriptionDescription.getTopic();
        final List<String> tids = subscriptionDescription.getTids();
        final String consumerGroup = generateConsumerGroupForTopic(topic);

        final String tubeMasterAddresses =
                subscriptionDescription.getMasterAddress() != null ? subscriptionDescription.getMasterAddress()
                        : globalTubeMasterAddresses;
        if (tubeMasterAddresses == null) {
            LOG.warn("No master address provided of {}", topic);
            return;
        }
        final List<Pair<String, Integer>> parsedTubeMasterAddress = CommonUtils
                .parseHostnameAndPort(tubeMasterAddresses);
        if (parsedTubeMasterAddress.isEmpty()) {
            LOG.warn("Invalid master address {} provided of {}", tubeMasterAddresses, topic);
            return;
        }
        final Pair<String, Integer> firstTubeMasterAddress = parsedTubeMasterAddress.get(0);
        // get local address for connecting
        InetSocketAddress firstSocketAddress = new InetSocketAddress(firstTubeMasterAddress.getLeft(),
                firstTubeMasterAddress.getRight());
        InetAddress localAddress = ConnectionUtils.findConnectingAddress(
                firstSocketAddress, 2000L, 400L);
        String localhost = localAddress.getHostAddress();

        final Map<String, Long> partitionToOffset;
        synchronized (context.getCheckpointLock()) {
            if (topicToOffset.containsKey(topic)) {
                partitionToOffset = topicToOffset.get(topic);
            } else {
                partitionToOffset = new HashMap<>();
                topicToOffset.put(topic, partitionToOffset);
            }
        }

        // initialize ConsumerConfig
        ConsumerConfig consumerConfig = new ConsumerConfig(localhost, tubeMasterAddresses, consumerGroup);
        // consumeFromMax works only if there is no offset restored from checkpoint
        if (consumeFromMax && partitionToOffset.isEmpty()) {
            consumerConfig.setConsumeModel(1);
            LOG.info("Would consume {} from max offset", topic);
        } else {
            consumerConfig.setConsumeModel(0);
        }
        consumerConfig.setMsgNotFoundWaitPeriodMs(messageNotFoundWaitPeriod.toMillis());

        // initialize TubeSingleSessionFactory
        final TubeMultiSessionFactory messageSessionFactory;
        if (tubeSessionFactories.containsKey(tubeMasterAddresses)
                && tubeSessionFactories.get(tubeMasterAddresses) != null) {
            messageSessionFactory = tubeSessionFactories.get(tubeMasterAddresses);
        } else {
            messageSessionFactory = new TubeMultiSessionFactory(consumerConfig);
            tubeSessionFactories.put(tubeMasterAddresses, messageSessionFactory);
        }

        // initialize PullMessageConsumer
        PullMessageConsumer messagePullConsumer = messageSessionFactory.createPullConsumer(consumerConfig);
        pullMessageConsumers.put(topic, messagePullConsumer);

        // initialize TubePullThread and run
        TubePullThread tubePullThread = new TubePullThread(messagePullConsumer, topic, tids, partitionToOffset,
                subscribeTimeout);
        tubePullThread.setName("TubePullThread-for-topic-" + topic);
        tubePullThread.start();
        tubePullThreads.put(topic, tubePullThread);

        LOG.info("Add topic \"{}\" successfully!", topic);
    }

    public void updateTopic(TubeSubscriptionDescription subscriptionDescription) throws Exception {
        removeTopic(subscriptionDescription.getTopic(), true);
        addTopic(subscriptionDescription);
    }

    public void removeTopic(String topic) throws Exception {
        removeTopic(topic, false);
    }

    private void removeTopic(String topic, boolean retainOffset) throws Exception {
        if (!retainOffset) {
            synchronized (context.getCheckpointLock()) {
                topicToOffset.remove(topic);
            }
        }
        // stop corresponding pulling threads
        final TubePullThread tubePullThread = tubePullThreads.get(topic);
        if (tubePullThread != null) {
            tubePullThread.cancel();
            tubePullThread.interrupt();
            try {
                tubePullThread.join();
            } catch (InterruptedException e) {
                LOG.error("Could not cancel thread {}", tubePullThread.getName());
            }
            tubePullThreads.remove(topic);
        }

        // shut down corresponding PullMessageConsumers
        final PullMessageConsumer messageConsumer = pullMessageConsumers.get(topic);
        if (messageConsumer != null) {
            try {
                messageConsumer.shutdown();
            } catch (Throwable throwable) {
                LOG.warn("Could not properly shutdown the tubeMQ pull consumer.", throwable);
            }
            pullMessageConsumers.remove(topic);
        }

        LOG.info("Remove topic \"{}\" successfully", topic);
    }

    public Map<String, Map<String, Long>> getTopicToOffset() {
        return topicToOffset;
    }

    private String generateConsumerGroupForTopic(String topic) {
        return clusterId + "_" + topic + "_consumer_group";
    }

    public class TubePullThread extends Thread {

        private volatile boolean running = true;

        private final PullMessageConsumer pullMessageConsumer;

        private final String topic;

        private final List<String> tids;

        private final long subscribeTimeout;

        @GuardedBy("context.getCheckpointLock()")
        private final Map<String, Long> partitionToOffset;

        public TubePullThread(PullMessageConsumer pullMessageConsumer, String topic,
                List<String> tids, Map<String, Long> partitionToOffset, long subscribeTimeout) {
            this.pullMessageConsumer = checkNotNull(pullMessageConsumer);
            this.topic = checkNotNull(topic);
            this.tids = checkNotNull(tids);
            this.partitionToOffset = checkNotNull(partitionToOffset);
            this.subscribeTimeout = subscribeTimeout;
        }

        @Override
        public void run() {
            try {
                LOG.warn("Start pull thread of {}", topic);
                doRun();
            } catch (InterruptedException e) {
                // ignore interruption from cancelling
                if (running) {
                    exceptionCatcher.accept(e);
                    LOG.warn("TubeMQ pull thread has been interrupted.", e);
                }
            } catch (Throwable t) {
                exceptionCatcher.accept(t);
                LOG.warn("Error occurred in tubeMQ pull thread.", t);
            } finally {
                LOG.info("TubeMQ pull thread of {} stops", topic);
            }
        }

        public void cancel() {
            this.running = false;
        }

        @VisibleForTesting
        void subscribe() throws Exception {
            final long startSubscription = System.currentTimeMillis();
            long retryInterval = 100;
            TubeClientException lastException = null;
            boolean subscribeSuccessfully = false;

            pullMessageConsumer.subscribe(topic, new TreeSet<>(tids));
            while (running && System.currentTimeMillis() - startSubscription < subscribeTimeout) {
                try {
                    pullMessageConsumer.completeSubscribe(sessionKey, numTasks, true, partitionToOffset);
                    subscribeSuccessfully = true;
                    LOG.info("Subscribe topic {} with tids {} successfully", topic, tids);
                    break;
                } catch (TubeClientException e) {
                    lastException = e;
                    LOG.warn("Subscribe tubeMQ failed, would retry after {} ms", retryInterval, e);
                    //noinspection BusyWait
                    Thread.sleep(retryInterval);
                    retryInterval *= 2;
                }
            }
            if (!subscribeSuccessfully) {
                throw lastException != null ? lastException : new Exception("Subscribe tubeMQ failed");
            }
        }

        private void doRun() throws Exception {

            subscribe();

            while (running) {
                final ConsumerResult consumeResult;
                try {
                    consumeResult = pullMessageConsumer.getMessage();
                } catch (TubeClientException e) {
                    LOG.error("Pull message from tubeMQ failed! ", e);
                    continue;
                }

                if (!consumeResult.isSuccess()) {
                    if (!TUBE_IGNORE_ERROR_CODE.contains(consumeResult.getErrCode())) {
                        LOG.info("Could not consume messages from tubeMQ (errcode: {}, errmsg: {}).",
                                consumeResult.getErrCode(), consumeResult.getErrMsg());
                    }
                    continue;
                }

                synchronized (context.getCheckpointLock()) {
                    for (Message message : consumeResult.getMessageList()) {
                        // TODO, optimize for single tid or no tid topic
                        context.collect(new TDMsgMixedSerializedRecord(
                                topic, System.currentTimeMillis(), message.getData()));
                    }
                    final String partitionKey = consumeResult.getPartitionKey();
                    final long offset = consumeResult.getCurrOffset();
                    partitionToOffset.put(partitionKey, offset);

                    if (LOG.isDebugEnabled()) {
                        LOG.debug("Update offset, topic {}, partition {}, offset {}",
                                topic, partitionKey, offset);
                    }
                }

                try {
                    final ConsumerResult confirmResult = pullMessageConsumer
                            .confirmConsume(consumeResult.getConfirmContext(), true);
                    if (!confirmResult.isSuccess() && !TUBE_IGNORE_ERROR_CODE
                            .contains(confirmResult.getErrCode())) {
                        LOG.warn("Could not confirm messages to tubeMQ (errcode: {}, errmsg: {}).",
                                confirmResult.getErrCode(), confirmResult.getErrMsg());
                    }
                } catch (TubeClientException e) {
                    LOG.error("Confirm consume failed! ", e);
                }
            }
        }
    }
}
