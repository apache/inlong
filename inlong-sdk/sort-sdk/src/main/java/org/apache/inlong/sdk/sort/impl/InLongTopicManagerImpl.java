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
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import org.apache.inlong.sdk.sort.api.ClientContext;
import org.apache.inlong.sdk.sort.api.InLongTopicFetcher;
import org.apache.inlong.sdk.sort.api.InLongTopicManager;
import org.apache.inlong.sdk.sort.api.InlongTopicTypeEnum;
import org.apache.inlong.sdk.sort.api.QueryConsumeConfig;
import org.apache.inlong.sdk.sort.entity.ConsumeConfig;
import org.apache.inlong.sdk.sort.entity.InLongTopic;
import org.apache.inlong.sdk.sort.impl.pulsar.InLongPulsarFetcherImpl;
import org.apache.inlong.sdk.sort.util.PeriodicTask;
import org.apache.inlong.sdk.sort.util.StringUtil;
import org.apache.pulsar.client.api.AuthenticationFactory;
import org.apache.pulsar.client.api.PulsarClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class InLongTopicManagerImpl extends InLongTopicManager {

    private final Logger logger = LoggerFactory.getLogger(InLongTopicManagerImpl.class);

    private final ConcurrentHashMap<String, InLongTopicFetcher> fetchers
            = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, PulsarClient> pulsarClients = new ConcurrentHashMap<>();

    private final PeriodicTask updateMetaDataWorker;
    private volatile List<String> toBeSelectFetchers = new ArrayList<>();
    private boolean stopAssign = false;

    public InLongTopicManagerImpl(ClientContext context, QueryConsumeConfig queryConsumeConfig) {
        super(context, queryConsumeConfig);
        updateMetaDataWorker = new UpdateMetaDataThread(context.getConfig().getUpdateMetaDataIntervalSec(),
                TimeUnit.SECONDS);
        String threadName = "sortsdk_inlongtopic_manager_"
                + StringUtil.formatDate(new Date(), "yyyy-MM-dd HH:mm:ss");
        updateMetaDataWorker.start(threadName);
    }

    private void updateToBeSelectFetchers(Collection<String> c) {
        toBeSelectFetchers = new ArrayList<>(c);
    }

    @Override
    public InLongTopicFetcher addFetcher(InLongTopic inLongTopic) {
        try {
            InLongTopicFetcher result = fetchers.get(inLongTopic.getTopicKey());
            if (result == null) {
                InLongTopicFetcher inLongTopicFetcher = new InLongPulsarFetcherImpl(inLongTopic, context);
                InLongTopicFetcher preValue = fetchers.putIfAbsent(inLongTopic.getTopicKey(), inLongTopicFetcher);
                logger.info("addFetcher :{}", inLongTopic.getTopicKey());
                if (preValue != null) {
                    result = preValue;
                    inLongTopicFetcher.close();
                    logger.info("addFetcher create same fetcher {}", inLongTopic);
                } else {
                    result = inLongTopicFetcher;
                    PulsarClient pulsarClient = pulsarClients.get(inLongTopic.getInLongCluster().getClusterId());
                    if (null == pulsarClient) {
                        logger.error("pulsar client is null:{}", inLongTopic.getInLongCluster().getClusterId());
                        return null;
                    }

                    if (!result.init(pulsarClient)) {
                        logger.info("addFetcher init fail:{}", inLongTopic.getTopicKey());
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

    @Override
    public InLongTopicFetcher removeFetcher(InLongTopic inLongTopic, boolean closeFetcher) {
        InLongTopicFetcher result = fetchers.remove(inLongTopic.getTopicKey());
        if (result != null && closeFetcher) {
            result.close();
        }
        return result;
    }

    @Override
    public InLongTopicFetcher getFetcher(String fetchKey) {
        return fetchers.get(fetchKey);
    }

    @Override
    public Set<String> getManagedInLongTopics() {
        return new HashSet<>(fetchers.keySet());
    }

    @Override
    public Collection<InLongTopicFetcher> getAllFetchers() {
        return fetchers.values();
    }

    /**
     * offline all inlong topic
     */
    @Override
    public void offlineAllTp() {
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
    public boolean clean() {
        boolean result = false;
        String sortTaskId = context.getConfig().getSortTaskId();
        try {
            logger.info("start close {}", sortTaskId);

            if (updateMetaDataWorker != null) {
                updateMetaDataWorker.stop();
            }

            closeFetcher();
            result = true;
            logger.info("close finished {} {}", sortTaskId, result);
        } catch (Throwable th) {
            logger.info("close error {} {}", sortTaskId, th);

        }
        return result;
    }

    private void closeAllFetcher() {
        closeFetcher();
    }

    private void closeFetcher() {
        Set<Entry<String, InLongTopicFetcher>> entries = fetchers.entrySet();
        for (Entry<String, InLongTopicFetcher> entry : entries) {
            String fetchKey = entry.getKey();
            InLongTopicFetcher inLongTopicFetcher = entry.getValue();
            boolean succ = false;
            if (inLongTopicFetcher != null) {
                try {
                    succ = inLongTopicFetcher.close();
                } catch (Exception e) {
                    logger.error(e.getMessage(), e);
                }
            }
            logger.info(" close {} {}", fetchKey, succ);
        }
    }

    private List<String> getNewTopics(List<InLongTopic> newSubscribedInLongTopics) {
        if (newSubscribedInLongTopics != null && newSubscribedInLongTopics.size() > 0) {
            List<String> newTopics = new ArrayList<>();
            for (InLongTopic inLongTopic : newSubscribedInLongTopics) {
                newTopics.add(inLongTopic.getTopicKey());
            }
            return newTopics;
        }
        return null;
    }

    private void handleCurrentConsumeConfig(List<InLongTopic> currentConsumeConfig) {
        if (null == currentConsumeConfig) {
            return;
        }

        List<InLongTopic> newConsumeConfig = new ArrayList<>(currentConsumeConfig);
        logger.info("newConsumeConfig List:{}", Arrays.toString(newConsumeConfig.toArray()));
        List<String> newTopics = getNewTopics(newConsumeConfig);
        logger.info("newTopics :{}", Arrays.toString(newTopics.toArray()));

        List<String> oldInLongTopics = new ArrayList<>(fetchers.keySet());
        logger.info("oldInLongTopics :{}", Arrays.toString(oldInLongTopics.toArray()));
        //get need be offlined topics
        oldInLongTopics.removeAll(newTopics);
        logger.info("removed oldInLongTopics :{}", Arrays.toString(oldInLongTopics.toArray()));

        //get new topics
        newTopics.removeAll(new ArrayList<>(fetchers.keySet()));
        logger.info("really new topics :{}", Arrays.toString(newTopics.toArray()));
        //offline need be offlined topics
        offlineRmovedTopic(oldInLongTopics);
        //online new topics
        onlineNewTopic(newConsumeConfig, newTopics);
    }

    /**
     * offline inlong topic which not belong the sortTaskId
     *
     * @param oldInLongTopics List
     */
    private void offlineRmovedTopic(List<String> oldInLongTopics) {
        for (String fetchKey : oldInLongTopics) {
            logger.info("offlineRmovedTopic :{}", fetchKey);
            InLongTopic inLongTopic = fetchers.get(fetchKey).getInLongTopic();
            InLongTopicFetcher inLongTopicFetcher = fetchers.getOrDefault(fetchKey, null);
            if (inLongTopicFetcher != null) {
                inLongTopicFetcher.close();
            }
            fetchers.remove(fetchKey);
            if (context != null && context.getStatManager() != null && inLongTopic != null) {
                context.getStatManager()
                        .getStatistics(context.getConfig().getSortTaskId(),
                                inLongTopic.getInLongCluster().getClusterId(),
                                inLongTopic.getTopic())
                        .addTopicOfflineTimes(1);
            } else {
                logger.error("context == null or context.getStatManager() == null or inLongTopic == null :{}",
                        inLongTopic);
            }
        }
    }

    /**
     * online new inlong topic
     *
     * @param newSubscribedInLongTopics List
     * @param reallyNewTopic List
     */
    private void onlineNewTopic(List<InLongTopic> newSubscribedInLongTopics, List<String> reallyNewTopic) {
        for (InLongTopic inLongTopic : newSubscribedInLongTopics) {
            if (!reallyNewTopic.contains(inLongTopic.getTopicKey())) {
                continue;
            }
            onlineTopic(inLongTopic);
        }
    }

    private void onlineTopic(InLongTopic inLongTopic) {
        if (InlongTopicTypeEnum.PULSAR.getName().equals(inLongTopic.getTopicType())) {
            onlinePulsarTopic(inLongTopic);
        } else if (InlongTopicTypeEnum.KAFKA.getName().equals(inLongTopic.getTopicType())) {
            onlineKafkaTopic(inLongTopic);
        } else if (InlongTopicTypeEnum.TUBE.getName().equals(inLongTopic.getTopicType())) {
            onlineTubeTopic(inLongTopic);
        } else {
            logger.error("topic type:{} not support", inLongTopic.getTopicType());
        }
    }

    private void onlinePulsarTopic(InLongTopic inLongTopic) {
        if (!checkAndCreateNewPulsarClient(inLongTopic)) {
            return;
        }

        if (!fetchers.containsKey(inLongTopic.getTopicKey())) {
            logger.info("begin add Fetcher:{}", inLongTopic.getTopicKey());
            if (context != null && context.getStatManager() != null) {
                context.getStatManager()
                        .getStatistics(context.getConfig().getSortTaskId(),
                                inLongTopic.getInLongCluster().getClusterId(), inLongTopic.getTopic())
                        .addTopicOnlineTimes(1);
                InLongTopicFetcher fetcher = addFetcher(inLongTopic);
                if (fetcher == null) {
                    fetchers.remove(inLongTopic.getTopicKey());
                    logger.error("add fetcher error:{}", inLongTopic.getTopicKey());
                }
            } else {
                logger.error("context == null or context.getStatManager() == null");
            }
        }
    }

    private boolean checkAndCreateNewPulsarClient(InLongTopic inLongTopic) {
        if (!pulsarClients.containsKey(inLongTopic.getInLongCluster().getClusterId())) {
            if (inLongTopic.getInLongCluster().getBootstraps() != null) {
                try {
                    PulsarClient pulsarClient = PulsarClient.builder()
                            .serviceUrl(inLongTopic.getInLongCluster().getBootstraps())
                            .authentication(AuthenticationFactory.token(inLongTopic.getInLongCluster().getToken()))
                            .build();
                    pulsarClients.put(inLongTopic.getInLongCluster().getClusterId(), pulsarClient);
                    logger.info("create pulsar client succ {} {} {}", inLongTopic.getInLongCluster().getClusterId(),
                            inLongTopic.getInLongCluster().getBootstraps(),
                            inLongTopic.getInLongCluster().getToken());
                } catch (Exception e) {
                    logger.error(e.getMessage(), e);
                    return false;
                }
            } else {
                logger.info("bootstrap is null {}", inLongTopic.getInLongCluster());
                return false;
            }
        }
        return true;
    }

    private void onlineKafkaTopic(InLongTopic inLongTopic) {
    }

    private void onlineTubeTopic(InLongTopic inLongTopic) {
    }

    private class UpdateMetaDataThread extends PeriodicTask {

        public UpdateMetaDataThread(long runInterval, TimeUnit timeUnit) {
            super(runInterval, timeUnit, context.getConfig());
        }

        @Override
        protected void doWork() {
            logger.debug("InLongTopicManagerImpl doWork");
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
