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

package org.apache.inlong.sdk.sort.impl;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import org.apache.inlong.sdk.sort.api.ClientContext;
import org.apache.inlong.sdk.sort.api.InlongTopicFetcher;
import org.apache.inlong.sdk.sort.api.InlongTopicManager;
import org.apache.inlong.sdk.sort.api.InlongTopicTypeEnum;
import org.apache.inlong.sdk.sort.api.QueryConsumeConfig;
import org.apache.inlong.sdk.sort.entity.ConsumeConfig;
import org.apache.inlong.sdk.sort.entity.InlongTopic;
import org.apache.inlong.sdk.sort.impl.kafka.InlongKafkaFetcherImpl;
import org.apache.inlong.sdk.sort.impl.pulsar.InlongPulsarFetcherImpl;
import org.apache.inlong.sdk.sort.impl.tube.InlongTubeFetcherImpl;
import org.apache.inlong.sdk.sort.impl.tube.TubeConsumerCreater;
import org.apache.inlong.sdk.sort.util.PeriodicTask;
import org.apache.inlong.sdk.sort.util.StringUtil;
import org.apache.inlong.tubemq.client.config.TubeClientConfig;
import org.apache.inlong.tubemq.client.factory.MessageSessionFactory;
import org.apache.inlong.tubemq.client.factory.TubeSingleSessionFactory;
import org.apache.pulsar.client.api.AuthenticationFactory;
import org.apache.pulsar.client.api.PulsarClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class InlongTopicManagerImpl extends InlongTopicManager {

    private final Logger logger = LoggerFactory.getLogger(InlongTopicManagerImpl.class);

    private final ConcurrentHashMap<String, InlongTopicFetcher> fetchers
            = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, PulsarClient> pulsarClients = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, TubeConsumerCreater> tubeFactories = new ConcurrentHashMap<>();

    private final PeriodicTask updateMetaDataWorker;
    private volatile List<String> toBeSelectFetchers = new ArrayList<>();
    private boolean stopAssign = false;

    public InlongTopicManagerImpl(ClientContext context, QueryConsumeConfig queryConsumeConfig) {
        super(context, queryConsumeConfig);
        updateMetaDataWorker = new UpdateMetaDataThread(context.getConfig().getUpdateMetaDataIntervalSec(),
                TimeUnit.SECONDS);
        String threadName = "sortsdk_inlongtopic_manager_" + context.getConfig().getSortTaskId()
                + "_" + StringUtil.formatDate(new Date(), "yyyy-MM-dd HH:mm:ss");
        updateMetaDataWorker.start(threadName);
    }

    private void updateToBeSelectFetchers(Collection<String> c) {
        toBeSelectFetchers = new ArrayList<>(c);
    }

    private boolean initFetcher(InlongTopicFetcher fetcher, InlongTopic inlongTopic) {
        if (InlongTopicTypeEnum.PULSAR.getName().equalsIgnoreCase(inlongTopic.getTopicType())) {
            logger.info("create fetcher topic is pulsar {}", inlongTopic);
            return fetcher.init(pulsarClients.get(inlongTopic.getInlongCluster().getClusterId()));
        } else if (InlongTopicTypeEnum.KAFKA.getName().equalsIgnoreCase(inlongTopic.getTopicType())) {
            logger.info("create fetcher topic is kafka {}", inlongTopic);
            return fetcher.init(inlongTopic.getInlongCluster().getBootstraps());
        } else if (InlongTopicTypeEnum.TUBE.getName().equalsIgnoreCase(inlongTopic.getTopicType())) {
            logger.info("create fetcher topic is tube {}", inlongTopic);
            return fetcher.init(tubeFactories.get(inlongTopic.getInlongCluster().getClusterId()));
        } else {
            logger.error("create fetcher topic type not support " + inlongTopic.getTopicType());
            return false;
        }
    }

    @Override
    public InlongTopicFetcher addFetcher(InlongTopic inlongTopic) {

        try {
            InlongTopicFetcher result = fetchers.get(inlongTopic.getTopicKey());
            if (result == null) {
                // create fetcher (pulsar,tube,kafka)
                InlongTopicFetcher inlongTopicFetcher = createInlongTopicFetcher(inlongTopic);
                InlongTopicFetcher preValue = fetchers.putIfAbsent(inlongTopic.getTopicKey(), inlongTopicFetcher);
                logger.info("addFetcher :{}", inlongTopic.getTopicKey());
                if (preValue != null) {
                    result = preValue;
                    if (inlongTopicFetcher != null) {
                        inlongTopicFetcher.close();
                    }
                    logger.info("addFetcher create same fetcher {}", inlongTopic);
                } else {
                    result = inlongTopicFetcher;
                    if (result != null
                            && !initFetcher(result, inlongTopic)) {
                        logger.info("addFetcher init fail {}", inlongTopic.getTopicKey());
                        result.close();
                        result = null;
                    }
                }
            }
            return result;
        } finally {
            updateToBeSelectFetchers(fetchers.keySet());
        }
    }

    /**
     * create fetcher (pulsar,tube,kafka)
     *
     * @param inlongTopic {@link InlongTopic}
     * @return {@link InlongTopicFetcher}
     */
    private InlongTopicFetcher createInlongTopicFetcher(InlongTopic inlongTopic) {
        if (InlongTopicTypeEnum.PULSAR.getName().equalsIgnoreCase(inlongTopic.getTopicType())) {
            logger.info("the topic is pulsar {}", inlongTopic);
            return new InlongPulsarFetcherImpl(inlongTopic, context);
        } else if (InlongTopicTypeEnum.KAFKA.getName().equalsIgnoreCase(inlongTopic.getTopicType())) {
            logger.info("the topic is kafka {}", inlongTopic);
            return new InlongKafkaFetcherImpl(inlongTopic, context);
        } else if (InlongTopicTypeEnum.TUBE.getName().equalsIgnoreCase(inlongTopic.getTopicType())) {
            logger.info("the topic is tube {}", inlongTopic);
            return new InlongTubeFetcherImpl(inlongTopic, context);
        } else {
            logger.error("topic type not support " + inlongTopic.getTopicType());
            return null;
        }
    }

    @Override
    public InlongTopicFetcher removeFetcher(InlongTopic inlongTopic, boolean closeFetcher) {
        InlongTopicFetcher result = fetchers.remove(inlongTopic.getTopicKey());
        if (result != null && closeFetcher) {
            result.close();
        }
        return result;
    }

    @Override
    public InlongTopicFetcher getFetcher(String fetchKey) {
        return fetchers.get(fetchKey);
    }

    @Override
    public Set<String> getManagedInlongTopics() {
        return new HashSet<>(fetchers.keySet());
    }

    @Override
    public Collection<InlongTopicFetcher> getAllFetchers() {
        return fetchers.values();
    }

    /**
     * offline all inlong topic
     */
    @Override
    public void offlineAllTopicsAndPartitions() {
        String subscribeId = context.getConfig().getSortTaskId();
        try {
            logger.info("start offline {}", subscribeId);
            stopAssign = true;
            closeAllFetcher();
            logger.info("close finished {}", subscribeId);
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
    }

    @Override
    public void close() {
        if (updateMetaDataWorker != null) {
            updateMetaDataWorker.stop();
        }
    }

    @Override
    public boolean clean() {
        String sortTaskId = context.getConfig().getSortTaskId();
        try {
            logger.info("start close {}", sortTaskId);

            if (updateMetaDataWorker != null) {
                updateMetaDataWorker.stop();
            }

            closeFetcher();
            closePulsarClient();
            closeTubeSessionFactory();
            logger.info("close finished {}", sortTaskId);
            return true;
        } catch (Throwable th) {
            logger.error("close error " + sortTaskId, th);
        }
        return false;
    }

    private void closeAllFetcher() {
        closeFetcher();
    }

    private void closeFetcher() {
        Set<Entry<String, InlongTopicFetcher>> entries = fetchers.entrySet();
        for (Entry<String, InlongTopicFetcher> entry : entries) {
            String fetchKey = entry.getKey();
            InlongTopicFetcher inlongTopicFetcher = entry.getValue();
            boolean succ = false;
            if (inlongTopicFetcher != null) {
                try {
                    succ = inlongTopicFetcher.close();
                } catch (Exception e) {
                    logger.error(e.getMessage(), e);
                }
            }
            logger.info(" close fetcher{} {}", fetchKey, succ);
        }
    }

    private void closePulsarClient() {
        for (Map.Entry<String, PulsarClient> entry : pulsarClients.entrySet()) {
            PulsarClient pulsarClient = entry.getValue();
            String key = entry.getKey();
            try {
                if (pulsarClient != null) {
                    pulsarClient.close();
                }
            } catch (Exception e) {
                logger.error("close PulsarClient" + key + " error.", e);
            }
        }
        pulsarClients.clear();
    }

    private void closeTubeSessionFactory() {
        for (Map.Entry<String, TubeConsumerCreater> entry : tubeFactories.entrySet()) {
            MessageSessionFactory tubeMessageSessionFactory = entry.getValue().getMessageSessionFactory();
            String key = entry.getKey();
            try {
                if (tubeMessageSessionFactory != null) {
                    tubeMessageSessionFactory.shutdown();
                }
            } catch (Exception e) {
                logger.error("close MessageSessionFactory" + key + " error.", e);
            }
        }
        tubeFactories.clear();
    }

    private List<String> getNewTopics(List<InlongTopic> newSubscribedInlongTopics) {
        if (newSubscribedInlongTopics != null && newSubscribedInlongTopics.size() > 0) {
            List<String> newTopics = new ArrayList<>();
            for (InlongTopic inlongTopic : newSubscribedInlongTopics) {
                newTopics.add(inlongTopic.getTopicKey());
            }
            return newTopics;
        }
        return null;
    }

    private void handleCurrentConsumeConfig(List<InlongTopic> currentConsumeConfig) {
        if (null == currentConsumeConfig) {
            logger.warn("List<InlongTopic> currentConsumeConfig is null");
            return;
        }

        List<InlongTopic> newConsumeConfig = new ArrayList<>(currentConsumeConfig);
        logger.debug("newConsumeConfig List:{}", Arrays.toString(newConsumeConfig.toArray()));
        List<String> newTopics = getNewTopics(newConsumeConfig);
        logger.debug("newTopics :{}", Arrays.toString(newTopics.toArray()));

        List<String> oldInlongTopics = new ArrayList<>(fetchers.keySet());
        logger.debug("oldInlongTopics :{}", Arrays.toString(oldInlongTopics.toArray()));
        //get need be offlined topics
        oldInlongTopics.removeAll(newTopics);
        logger.debug("removed oldInlongTopics :{}", Arrays.toString(oldInlongTopics.toArray()));

        //get new topics
        newTopics.removeAll(new ArrayList<>(fetchers.keySet()));
        logger.debug("really new topics :{}", Arrays.toString(newTopics.toArray()));
        //offline need be offlined topics
        offlineRmovedTopic(oldInlongTopics);
        //online new topics
        onlineNewTopic(newConsumeConfig, newTopics);
    }

    /**
     * offline inlong topic which not belong the sortTaskId
     *
     * @param oldInlongTopics {@link List}
     */
    private void offlineRmovedTopic(List<String> oldInlongTopics) {
        for (String fetchKey : oldInlongTopics) {
            logger.info("offlineRmovedTopic {}", fetchKey);
            InlongTopic inlongTopic = fetchers.get(fetchKey).getInlongTopic();
            InlongTopicFetcher inlongTopicFetcher = fetchers.getOrDefault(fetchKey, null);
            if (inlongTopicFetcher != null) {
                inlongTopicFetcher.close();
            }
            fetchers.remove(fetchKey);
            if (context != null && context.getStatManager() != null && inlongTopic != null) {
                context.getStatManager()
                        .getStatistics(context.getConfig().getSortTaskId(),
                                inlongTopic.getInlongCluster().getClusterId(),
                                inlongTopic.getTopic())
                        .addTopicOfflineTimes(1);
            } else {
                logger.error("context == null or context.getStatManager() == null or inlongTopic == null :{}",
                        inlongTopic);
            }
        }
    }

    /**
     * online new inlong topic
     *
     * @param newSubscribedInlongTopics List
     * @param reallyNewTopic List
     */
    private void onlineNewTopic(List<InlongTopic> newSubscribedInlongTopics, List<String> reallyNewTopic) {
        for (InlongTopic inlongTopic : newSubscribedInlongTopics) {
            if (!reallyNewTopic.contains(inlongTopic.getTopicKey())) {
                logger.debug("!reallyNewTopic.contains(inlongTopic.getTopicKey())");
                continue;
            }
            onlineTopic(inlongTopic);
        }
    }

    private void onlineTopic(InlongTopic inlongTopic) {
        if (InlongTopicTypeEnum.PULSAR.getName().equalsIgnoreCase(inlongTopic.getTopicType())) {
            logger.info("the topic is pulsar:{}", inlongTopic);
            onlinePulsarTopic(inlongTopic);
        } else if (InlongTopicTypeEnum.KAFKA.getName().equalsIgnoreCase(inlongTopic.getTopicType())) {
            logger.info("the topic is kafka:{}", inlongTopic);
            onlineKafkaTopic(inlongTopic);
        } else if (InlongTopicTypeEnum.TUBE.getName().equalsIgnoreCase(inlongTopic.getTopicType())) {
            logger.info("the topic is tube:{}", inlongTopic);
            onlineTubeTopic(inlongTopic);
        } else {
            logger.error("topic type:{} not support", inlongTopic.getTopicType());
        }
    }

    private void onlinePulsarTopic(InlongTopic inlongTopic) {
        if (!checkAndCreateNewPulsarClient(inlongTopic)) {
            logger.error("checkAndCreateNewPulsarClient error:{}", inlongTopic);
            return;
        }
        createNewFetcher(inlongTopic);
    }

    private boolean checkAndCreateNewPulsarClient(InlongTopic inlongTopic) {
        if (!pulsarClients.containsKey(inlongTopic.getInlongCluster().getClusterId())) {
            if (inlongTopic.getInlongCluster().getBootstraps() != null) {
                try {
                    PulsarClient pulsarClient = PulsarClient.builder()
                            .serviceUrl(inlongTopic.getInlongCluster().getBootstraps())
                            .authentication(AuthenticationFactory.token(inlongTopic.getInlongCluster().getToken()))
                            .build();
                    pulsarClients.put(inlongTopic.getInlongCluster().getClusterId(), pulsarClient);
                    logger.debug("create pulsar client succ {}",
                            new String[]{inlongTopic.getInlongCluster().getClusterId(),
                                    inlongTopic.getInlongCluster().getBootstraps(),
                                    inlongTopic.getInlongCluster().getToken()});
                } catch (Exception e) {
                    logger.error("create pulsar client error {}", inlongTopic);
                    logger.error(e.getMessage(), e);
                    return false;
                }
            } else {
                logger.error("bootstrap is null {}", inlongTopic.getInlongCluster());
                return false;
            }
        }
        logger.info("create pulsar client true {}", inlongTopic);
        return true;
    }

    private boolean checkAndCreateNewTubeSessionFactory(InlongTopic inlongTopic) {
        if (!tubeFactories.containsKey(inlongTopic.getInlongCluster().getClusterId())) {
            if (inlongTopic.getInlongCluster().getBootstraps() != null) {
                try {
                    //create MessageSessionFactory
                    TubeClientConfig tubeConfig = new TubeClientConfig(inlongTopic.getInlongCluster().getBootstraps());
                    MessageSessionFactory messageSessionFactory = new TubeSingleSessionFactory(tubeConfig);
                    TubeConsumerCreater tubeConsumerCreater = new TubeConsumerCreater(messageSessionFactory,
                            tubeConfig);
                    tubeFactories.put(inlongTopic.getInlongCluster().getClusterId(), tubeConsumerCreater);
                    logger.debug("create tube client succ {} {} {}",
                            new String[]{inlongTopic.getInlongCluster().getClusterId(),
                                    inlongTopic.getInlongCluster().getBootstraps(),
                                    inlongTopic.getInlongCluster().getToken()});
                } catch (Exception e) {
                    logger.error("create tube client error {}", inlongTopic);
                    logger.error(e.getMessage(), e);
                    return false;
                }
            } else {
                logger.info("bootstrap is null {}", inlongTopic.getInlongCluster());
                return false;
            }
        }
        logger.info("create pulsar client true {}", inlongTopic);
        return true;
    }

    private void onlineKafkaTopic(InlongTopic inlongTopic) {
        createNewFetcher(inlongTopic);
    }

    private void onlineTubeTopic(InlongTopic inlongTopic) {
        if (!checkAndCreateNewTubeSessionFactory(inlongTopic)) {
            logger.error("checkAndCreateNewPulsarClient error:{}", inlongTopic);
            return;
        }
        createNewFetcher(inlongTopic);
    }

    private void createNewFetcher(InlongTopic inlongTopic) {
        if (!fetchers.containsKey(inlongTopic.getTopicKey())) {
            logger.info("begin add Fetcher:{}", inlongTopic.getTopicKey());
            if (context != null && context.getStatManager() != null) {
                context.getStatManager()
                        .getStatistics(context.getConfig().getSortTaskId(),
                                inlongTopic.getInlongCluster().getClusterId(), inlongTopic.getTopic())
                        .addTopicOnlineTimes(1);
                InlongTopicFetcher fetcher = addFetcher(inlongTopic);
                if (fetcher == null) {
                    fetchers.remove(inlongTopic.getTopicKey());
                    logger.error("add fetcher error:{}", inlongTopic.getTopicKey());
                }
            } else {
                logger.error("context == null or context.getStatManager() == null");
            }
        }
    }

    private class UpdateMetaDataThread extends PeriodicTask {

        public UpdateMetaDataThread(long runInterval, TimeUnit timeUnit) {
            super(runInterval, timeUnit, context.getConfig());
        }

        @Override
        protected void doWork() {
            logger.debug("InlongTopicManagerImpl doWork");
            if (stopAssign) {
                logger.warn("assign is stoped");
                return;
            }
            //get sortTask conf from manager
            if (queryConsumeConfig != null) {
                long start = System.currentTimeMillis();
                context.getStatManager().getStatistics(context.getConfig().getSortTaskId())
                        .addRequestManagerTimes(1);
                ConsumeConfig consumeConfig = queryConsumeConfig
                        .queryCurrentConsumeConfig(context.getConfig().getSortTaskId());
                context.getStatManager().getStatistics(context.getConfig().getSortTaskId())
                        .addRequestManagerTimeCost(System.currentTimeMillis() - start);

                if (consumeConfig != null) {
                    handleCurrentConsumeConfig(consumeConfig.getTopics());
                } else {
                    logger.warn("subscribedInfo is null");
                    context.getStatManager().getStatistics(context.getConfig().getSortTaskId())
                            .addRequestManagerFailTimes(1);
                }
            } else {
                logger.error("subscribedMetaDataInfo is null");
            }
        }
    }
}
