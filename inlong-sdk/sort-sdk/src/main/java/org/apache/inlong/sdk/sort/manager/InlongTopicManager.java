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

package org.apache.inlong.sdk.sort.manager;

import org.apache.inlong.sdk.sort.api.ClientContext;
import org.apache.inlong.sdk.sort.api.InlongTopicTypeEnum;
import org.apache.inlong.sdk.sort.api.QueryConsumeConfig;
import org.apache.inlong.sdk.sort.api.TopicFetcher;
import org.apache.inlong.sdk.sort.api.TopicFetcherBuilder;
import org.apache.inlong.sdk.sort.api.TopicManager;
import org.apache.inlong.sdk.sort.entity.CacheZoneCluster;
import org.apache.inlong.sdk.sort.entity.ConsumeConfig;
import org.apache.inlong.sdk.sort.entity.InLongTopic;
import org.apache.inlong.sdk.sort.fetcher.tube.TubeConsumerCreator;
import org.apache.inlong.tubemq.client.config.TubeClientConfig;
import org.apache.inlong.tubemq.client.factory.MessageSessionFactory;
import org.apache.inlong.tubemq.client.factory.TubeSingleSessionFactory;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.client.api.Authentication;
import org.apache.pulsar.client.api.AuthenticationFactory;
import org.apache.pulsar.client.api.PulsarClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * Inlong manager that maintain the single topic fetchers.
 * It is suitable to the cases that each topic has its own configurations.
 * And each consumer only consume the very one topic.
 * InlongMultiTopicManager was used since 1.9.0
 */
public class InlongTopicManager extends TopicManager {

    private static final Logger LOGGER = LoggerFactory.getLogger(InlongTopicManager.class);

    private final ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();
    private final Map<String, TopicFetcher> fetchers = new ConcurrentHashMap<>();
    private static final Map<String, PulsarClient> pulsarClients = new ConcurrentHashMap<>();
    private final Map<String, TubeConsumerCreator> tubeFactories = new ConcurrentHashMap<>();

    protected final ForkJoinPool pool;

    private volatile boolean stopAssign = false;

    private Collection<InLongTopic> assignedTopics;

    public InlongTopicManager(ClientContext context, QueryConsumeConfig queryConsumeConfig) {
        super(context, queryConsumeConfig);
        executor.scheduleWithFixedDelay(this::updateMetaData, 0L,
                context.getConfig().getUpdateMetaDataIntervalSec(), TimeUnit.SECONDS);
        pool = new ForkJoinPool(context.getConfig().getThreadPoolSize());
    }

    @Override
    public boolean clean() {
        String sortTaskId = context.getConfig().getSortTaskId();
        try {
            LOGGER.info("start to clean topic manager, sortTaskId={}", sortTaskId);
            stopAssign = true;
            closeAllFetchers();
            // closeAllPulsarClients();
            closeAllTubeFactories();
            LOGGER.info("success to clean topic manager, sortTaskId={}", sortTaskId);
            return true;
        } catch (Exception e) {
            LOGGER.error("failed to clean topic manager, sortTaskId={}", sortTaskId, e);
        } finally {
            fetchers.clear();
            pulsarClients.clear();
            tubeFactories.clear();
            stopAssign = false;
        }
        return false;
    }

    @Override
    public TopicFetcher removeTopic(String topicKey) {
        LOGGER.info("start to close fetcher key={} ", topicKey);
        TopicFetcher topicFetcher = fetchers.remove(topicKey);
        if (topicFetcher != null) {
            try {
                topicFetcher.close();
                context.addTopicOfflineCount(1);
            } catch (Exception e) {
                LOGGER.error("close fetcher failed, key={}", topicKey, e);
            }
        }
        return topicFetcher;
    }

    private void closeAllFetchers() {
        pool.submit(() -> fetchers.keySet()
                .stream()
                .parallel()
                .forEach(this::removeTopic));
    }

    private void closeAllPulsarClients() {
        pool.submit(() -> pulsarClients.keySet()
                .stream()
                .parallel()
                .forEach(this::closePulsarClient));
    }

    private void closeAllTubeFactories() {
        pool.submit(() -> tubeFactories.keySet()
                .stream()
                .parallel()
                .forEach(this::closeTubeFactory));
    }

    private TubeConsumerCreator closeTubeFactory(String clusterId) {
        LOGGER.info("start to close tube creator id = {}", clusterId);
        TubeConsumerCreator creator = tubeFactories.remove(clusterId);
        try {
            if (creator != null) {
                creator.getMessageSessionFactory().shutdown();
            }
        } catch (Exception e) {
            LOGGER.error("close tube factory failed, client id = {}", clusterId);
        }
        return creator;
    }

    private PulsarClient closePulsarClient(String clusterId) {
        LOGGER.info("start to close pulsar client id = {}", clusterId);
        PulsarClient client = pulsarClients.remove(clusterId);
        try {
            if (client != null) {
                client.close();
            }
        } catch (Exception e) {
            LOGGER.error("close pulsar client failed, client id = {}", clusterId);
        }
        return client;
    }

    @Override
    public TopicFetcher addTopic(InLongTopic topic) {
        checkAndOnlineCluster(topic);
        return onlineNewTopic(topic);
    }

    @Override
    public TopicFetcher removeTopic(InLongTopic topic, boolean closeFetcher) {
        LOGGER.info("start to remove topicKey={}", topic.getTopicKey());
        TopicFetcher result = fetchers.remove(topic.getTopicKey());
        if (result != null && closeFetcher) {
            result.close();
        }
        return result;
    }

    @Override
    public TopicFetcher getFetcher(String fetchKey) {
        return fetchers.get(fetchKey);
    }

    @Override
    public Collection<TopicFetcher> getAllFetchers() {
        return new ArrayList<>(fetchers.values());
    }

    @Override
    public Set<String> getManagedInLongTopics() {
        return new HashSet<>(fetchers.keySet());
    }

    @Override
    public void offlineAllTopicsAndPartitions() {
        stopAssign = true;
        closeAllFetchers();
    }

    @Override
    public void close() {
        if (!executor.isShutdown()) {
            executor.shutdown();
        }
        clean();
    }

    @Override
    public void restartAssigned() {
        stopAssign = false;
    }

    @Override
    public void stopAssigned() {
        stopAssign = true;
    }

    private void updateMetaData() {
        LOGGER.debug("InLongTopicManager doWork");
        if (stopAssign) {
            LOGGER.warn("assign is stopped");
            return;
        }
        // get sortTask conf from manager
        if (queryConsumeConfig != null) {
            long start = System.currentTimeMillis();
            context.addRequestManager();
            ConsumeConfig consumeConfig = queryConsumeConfig
                    .queryCurrentConsumeConfig(context.getConfig().getSortTaskId());
            if (consumeConfig != null) {
                this.assignedTopics = new HashSet<>(context.getConfig().getConsumerSubset(consumeConfig.getTopics()));
                handleUpdatedConsumeConfig();
            } else {
                LOGGER.warn("subscribedInfo is null");
                context.addRequestManagerFail(System.currentTimeMillis() - start);
            }
        } else {
            LOGGER.error("subscribedMetaDataInfo is null");
        }
    }

    private void handleUpdatedConsumeConfig() {
        LOGGER.info("start to handle updated consume config");
        if (CollectionUtils.isEmpty(assignedTopics)) {
            LOGGER.warn("assignedTopics is null or empty, do nothing");
            return;
        }
        this.onlinePulsarClients();
        this.onlineTubeFactories();
        this.offlineRemovedTopics();
        this.onlineNewTopics();
        this.updateCurrentTopics();
        this.offlinePulsarClients();
        this.offlineTubeFactories();
        LOGGER.info("end to handle updated consume config");
    }

    private void offlineTubeFactories() {
        List<CacheZoneCluster> assignedTubeClusters = this.getCacheZoneClusters(InlongTopicTypeEnum.TUBE);
        Set<String> intersection = assignedTubeClusters.stream()
                .map(CacheZoneCluster::getClusterId)
                .filter(tubeFactories::containsKey)
                .collect(Collectors.toSet());
        pool.submit(() -> {
            Set<String> currentCluster = new HashSet<>(tubeFactories.keySet());
            currentCluster.stream().parallel()
                    .filter(id -> !intersection.contains(id))
                    .forEach(this::offlineTubeFactory);
        });
    }

    private void offlineTubeFactory(String clientId) {
        TubeConsumerCreator client = tubeFactories.remove(clientId);
        if (client != null) {
            LOGGER.info("start to close tube clientId={}", clientId);
            try {
                client.getMessageSessionFactory().shutdown();
                LOGGER.info("success to close tube clientId={}", clientId);
            } catch (Exception e) {
                LOGGER.warn("failed to close tube clientId={}", clientId);
            }
        } else {
            LOGGER.warn("when close tube client, find no client id={}", clientId);
        }
    }

    private void offlinePulsarClients() {
        List<CacheZoneCluster> assignedPulsarClusters = this.getCacheZoneClusters(InlongTopicTypeEnum.PULSAR);
        Set<String> intersection = assignedPulsarClusters.stream()
                .map(CacheZoneCluster::getClusterId)
                .filter(pulsarClients::containsKey)
                .collect(Collectors.toSet());
        pool.submit(() -> {
            Set<String> currentCluster = new HashSet<>(pulsarClients.keySet());
            currentCluster.stream().parallel()
                    .filter(id -> !intersection.contains(id))
                    .forEach(this::offlinePulsarClient);
        });
    }

    private void offlinePulsarClient(String clientId) {
        PulsarClient client = pulsarClients.remove(clientId);
        if (client != null) {
            LOGGER.info("start to close pulsar clientId={}", clientId);
            try {
                client.close();
                LOGGER.info("success to close pulsar clientId={}", clientId);
            } catch (Exception e) {
                LOGGER.warn("failed to close pulsar clientId={}", clientId);
            }
        } else {
            LOGGER.warn("when close pulsar client, find no client id={}", clientId);
        }
    }

    private void onlineTubeFactories() {
        List<CacheZoneCluster> assignedTubeClusters = this.getCacheZoneClusters(InlongTopicTypeEnum.TUBE);
        List<CacheZoneCluster> newClusters = assignedTubeClusters.stream()
                .filter(cluster -> !tubeFactories.containsKey(cluster.getClusterId()))
                .collect(Collectors.toList());
        pool.submit(() -> newClusters.stream().parallel().forEach(this::createTubeConsumerCreator));
    }

    private void createTubeConsumerCreator(CacheZoneCluster cluster) {
        LOGGER.info("start to init tube creator for cluster={}", cluster);
        if (cluster.getBootstraps() != null) {
            try {
                TubeClientConfig tubeConfig = new TubeClientConfig(cluster.getBootstraps());
                MessageSessionFactory messageSessionFactory = new TubeSingleSessionFactory(tubeConfig);
                TubeConsumerCreator tubeConsumerCreator = new TubeConsumerCreator(messageSessionFactory,
                        tubeConfig);
                TubeConsumerCreator oldCreator = tubeFactories.putIfAbsent(cluster.getClusterId(), tubeConsumerCreator);
                if (oldCreator != null) {
                    LOGGER.warn("close new tube creator for cluster={}", cluster);
                    tubeConsumerCreator.getMessageSessionFactory().shutdown();
                }
            } catch (Exception e) {
                LOGGER.error("create tube creator error for cluster={}", cluster, e);
                return;
            }
            LOGGER.info("success to init tube creatorfor cluster={}", cluster);
        } else {
            LOGGER.error("bootstrap is null for cluster={}", cluster);
        }
    }

    private void onlinePulsarClients() {
        List<CacheZoneCluster> assignedPulsarClusters = this.getCacheZoneClusters(InlongTopicTypeEnum.PULSAR);
        List<CacheZoneCluster> newClusters = assignedPulsarClusters.stream()
                .filter(cluster -> !pulsarClients.containsKey(cluster.getClusterId()))
                .collect(Collectors.toList());
        pool.submit(() -> newClusters.stream().parallel().forEach(this::createPulsarClient));
    }

    private void createPulsarClient(CacheZoneCluster cluster) {
        LOGGER.info("start to init pulsar client for cluster={}", cluster);
        String clientKey = cluster.getBootstraps();
        if (clientKey == null) {
            LOGGER.error("bootstrap is null for cluster={}", cluster);
            return;
        }
        if (pulsarClients.containsKey(clientKey)) {
            LOGGER.info("Repeat to init pulsar client for cluster={}", cluster);
            return;
        }
        try {
            String token = cluster.getToken();
            Authentication auth = null;
            if (StringUtils.isNoneBlank(token)) {
                auth = AuthenticationFactory.token(token);
            }
            PulsarClient pulsarClient = PulsarClient.builder()
                    .serviceUrl(cluster.getBootstraps())
                    .authentication(auth)
                    .build();
            LOGGER.info("create pulsar client succ cluster:{}, token:{}",
                    cluster.getClusterId(),
                    cluster.getToken());
            PulsarClient oldClient = pulsarClients.putIfAbsent(cluster.getClusterId(), pulsarClient);
            if (oldClient != null && !oldClient.isClosed()) {
                LOGGER.warn("close new pulsar client for cluster={}", cluster);
                pulsarClient.close();
            }
        } catch (Exception e) {
            LOGGER.error("create pulsar client error for cluster={}", cluster, e);
            return;
        }
        LOGGER.info("success to init pulsar client for cluster={}", cluster);
    }

    private List<CacheZoneCluster> getCacheZoneClusters(InlongTopicTypeEnum type) {
        return assignedTopics.stream()
                .filter(topic -> type.getName().equalsIgnoreCase(topic.getTopicType()))
                .map(InLongTopic::getInLongCluster)
                .distinct()
                .collect(Collectors.toList());
    }

    private void checkAndOnlineCluster(InLongTopic topic) {
        switch (topic.getTopicType().toLowerCase()) {
            case "pulsar":
                if (!pulsarClients.containsKey(topic.getInLongCluster().getClusterId())) {
                    createPulsarClient(topic.getInLongCluster());
                }
                return;
            case "tube":
                if (!tubeFactories.containsKey(topic.getInLongCluster().getClusterId())) {
                    createTubeConsumerCreator(topic.getInLongCluster());
                }
                return;
            default:
                LOGGER.error("do not support type={}", topic.getTopicType());
        }
    }

    private TopicFetcher onlineNewTopic(InLongTopic topic) {
        try {
            if (InlongTopicTypeEnum.PULSAR.getName().equalsIgnoreCase(topic.getTopicType())) {
                LOGGER.info("the topic is pulsar {}", topic);
                return TopicFetcherBuilder.newPulsarBuilder()
                        .pulsarClient(pulsarClients.get(topic.getInLongCluster().getClusterId()))
                        .topic(topic)
                        .context(context)
                        .subscribe();
            } else if (InlongTopicTypeEnum.KAFKA.getName().equalsIgnoreCase(topic.getTopicType())) {
                LOGGER.info("the topic is kafka {}", topic);
                return TopicFetcherBuilder.newKafkaBuilder()
                        .bootstrapServers(topic.getInLongCluster().getBootstraps())
                        .topic(topic)
                        .context(context)
                        .subscribe();
            } else if (InlongTopicTypeEnum.TUBE.getName().equalsIgnoreCase(topic.getTopicType())) {
                LOGGER.info("the topic is tube {}", topic);
                return TopicFetcherBuilder.newTubeBuilder()
                        .tubeConsumerCreater(tubeFactories.get(topic.getInLongCluster().getClusterId()))
                        .topic(topic)
                        .context(context)
                        .subscribe();
            } else {
                LOGGER.error("topic type not support " + topic.getTopicType());
                return null;
            }
        } catch (Exception e) {
            LOGGER.error("failed to subscribe new topic={}", topic, e);
            return null;
        }

    }

    private void onlineNewTopics() {
        pool.submit(() -> getOnlineTopics().stream().parallel().forEach(this::addTopic));
    }

    private void updateCurrentTopics() {
        pool.submit(() -> getUpdateTopics().stream().parallel().forEach(this::updateTopic));
    }

    private void updateTopic(InLongTopic topic) {
        TopicFetcher fetcher = fetchers.get(topic.getTopicKey());
        if (fetcher == null) {
            LOGGER.warn("when update topic, find no topic={}", topic);
            return;
        }
        fetcher.updateTopics(Collections.singletonList(topic));
    }

    private List<InLongTopic> getOnlineTopics() {
        return assignedTopics.stream()
                .filter(topic -> !fetchers.containsKey(topic.getTopicKey()))
                .distinct()
                .collect(Collectors.toList());
    }

    private void offlineRemovedTopics() {
        pool.submit(() -> getOfflineTopics().stream().parallel()
                .map(InLongTopic::getTopicKey)
                .forEach(this::removeTopic));
    }

    private List<InLongTopic> getOfflineTopics() {
        Set<String> intersection = assignedTopics.stream()
                .map(InLongTopic::getTopicKey)
                .filter(fetchers::containsKey)
                .collect(Collectors.toSet());

        return assignedTopics.stream()
                .filter(topic -> !intersection.contains(topic.getTopicKey()))
                .distinct()
                .collect(Collectors.toList());

    }

    private List<InLongTopic> getUpdateTopics() {
        return assignedTopics.stream()
                .filter(topic -> fetchers.containsKey(topic.getTopicKey()))
                .distinct()
                .collect(Collectors.toList());
    }
}
