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

package org.apache.inlong.store.service;

import com.google.gson.Gson;
import org.apache.commons.lang.StringUtils;
import org.apache.inlong.audit.protocol.AuditData;
import org.apache.inlong.store.config.PulsarConfig;
import org.apache.inlong.store.config.StoreConfig;
import org.apache.inlong.store.db.dao.AuditDataDao;
import org.apache.inlong.store.db.entities.AuditDataPo;
import org.apache.inlong.store.db.entities.ESDataPo;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageListener;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.SubscriptionType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

@Service
public class AuditMsgConsumerServer implements InitializingBean {

    private static final Logger LOG = LoggerFactory
            .getLogger(AuditMsgConsumerServer.class);

    @Autowired
    private PulsarConfig pulsarConfig;

    @Autowired
    private AuditDataDao auditDataDao;

    @Autowired
    private ElasticsearchService esService;

    @Autowired
    private StoreConfig storeConfig;

    private PulsarClient pulsarClient;

    private Gson gson = new Gson();

    private ConcurrentHashMap<String, List<Consumer<byte[]>>> topicConsumerMap =
            new ConcurrentHashMap<String, List<Consumer<byte[]>>>();

    public void afterPropertiesSet() throws Exception {
        pulsarClient = getOrCreatePulsarClient(pulsarConfig.getPulsarServerUrl());
        if (storeConfig.isElasticsearchStore()) {
            esService.startTimerRoutine();
        }
        updateConcurrentConsumer(pulsarClient);
    }

    private PulsarClient getOrCreatePulsarClient(String pulsarServerUrl) {
        LOG.info("start consumer pulsarServerUrl = {}", pulsarServerUrl);
        PulsarClient pulsarClient = null;
        try {
            pulsarClient = PulsarClient.builder().serviceUrl(pulsarServerUrl)
                    .connectionTimeout(pulsarConfig.getClientOperationTimeoutSecond(),
                            TimeUnit.SECONDS).build();
        } catch (PulsarClientException e) {
            LOG.error("getOrCreatePulsarClient has pulsar {} err {}", pulsarServerUrl, e);
        }
        return pulsarClient;
    }

    protected void updateConcurrentConsumer(PulsarClient pulsarClient) {
        List<Consumer<byte[]>> list =
                topicConsumerMap.computeIfAbsent(pulsarConfig.getPulsarTopic(),
                        key -> new ArrayList<Consumer<byte[]>>());
        int currentConsumerNum = list.size();
        int createNum = pulsarConfig.getConcurrentConsumerNum() - currentConsumerNum;
        /*
         * add consumer
         */
        if (createNum > 0) {
            for (int i = 0; i < pulsarConfig.getConcurrentConsumerNum(); i++) {
                Consumer<byte[]> consumer = createConsumer(pulsarClient, pulsarConfig.getPulsarTopic());
                if (consumer != null) {
                    list.add(consumer);
                }
            }
        } else if (createNum < 0) {
            /*
             * delete consumer
             */
            int removeIndex = currentConsumerNum - 1;
            for (int i = createNum; i < 0; i++) {
                Consumer<byte[]> consumer = list.remove(removeIndex);
                consumer.closeAsync();
                removeIndex -= 1;
            }
        }
    }

    protected Consumer<byte[]> createConsumer(PulsarClient pulsarClient, String topic) {
        Consumer<byte[]> consumer = null;
        if (pulsarClient != null && StringUtils.isNotEmpty(topic)) {
            LOG.info("createConsumer has topic {}, subName {}", topic,
                    pulsarConfig.getPulsarConsumerSubName());
            try {
                consumer = pulsarClient.newConsumer()
                        .subscriptionName(pulsarConfig.getPulsarConsumerSubName())
                        .subscriptionType(SubscriptionType.Shared)
                        .topic(topic)
                        .receiverQueueSize(pulsarConfig.getConsumerReceiveQueueSize())
                        .enableRetry(pulsarConfig.isPulsarConsumerEnableRetry())
                        .messageListener(new MessageListener<byte[]>() {
                            public void received(Consumer<byte[]> consumer, Message<byte[]> msg) {
                                try {
                                    handleMessage(msg);
                                    consumer.acknowledge(msg);
                                } catch (Exception e) {
                                    LOG.error("Consumer has exception topic {}, subName {}, ex {}",
                                            topic,
                                            pulsarConfig.getPulsarConsumerSubName(),
                                            e);
                                    if (pulsarConfig.isPulsarConsumerEnableRetry()) {
                                        try {
                                            consumer.reconsumeLater(msg, 10, TimeUnit.SECONDS);
                                        } catch (PulsarClientException pulsarClientException) {
                                            LOG.error("Consumer reconsumeLater has exception "
                                                            + "topic {}, subName {}, ex {}",
                                                    topic,
                                                    pulsarConfig.getPulsarConsumerSubName(),
                                                    pulsarClientException);
                                        }
                                    } else {
                                        consumer.negativeAcknowledge(msg);
                                    }
                                }
                            }
                        })
                        .subscribe();
            } catch (PulsarClientException e) {
                LOG.error("createConsumer has topic {}, subName {}, err {}", topic,
                        pulsarConfig.getPulsarConsumerSubName(), e);
            }
        }
        return consumer;
    }

    protected void handleMessage(Message<byte[]> msg) throws Exception {
        String body = new String(msg.getData(), "UTF-8");
        AuditData msgBody = gson.fromJson(body, AuditData.class);
        if (storeConfig.isMysqlStore()) {
            AuditDataPo po = new AuditDataPo();
            po.setIp(msgBody.getIp());
            po.setThreadId(msgBody.getThreadId());
            po.setDockerId(msgBody.getDockerId());
            po.setPacketId(msgBody.getPacketId());
            po.setSdkTs(new Date(msgBody.getSdkTs()));
            po.setLogTs(new Date(msgBody.getLogTs()));
            po.setAuditId(msgBody.getAuditId());
            po.setCount(msgBody.getCount());
            po.setDelay(msgBody.getDelay());
            po.setInlongGroupId(msgBody.getInlongGroupId());
            po.setInlongStreamId(msgBody.getInlongStreamId());
            po.setSize(msgBody.getSize());
            auditDataDao.insert(po);
        }
        if (storeConfig.isElasticsearchStore()) {
            ESDataPo esPo = new ESDataPo();
            esPo.setIp(msgBody.getIp());
            esPo.setThreadId(msgBody.getThreadId());
            esPo.setDockerId(msgBody.getDockerId());
            esPo.setSdkTs(new Date(msgBody.getSdkTs()).getTime());
            esPo.setLogTs(new Date(msgBody.getLogTs()));
            esPo.setAuditId(msgBody.getAuditId());
            esPo.setCount(msgBody.getCount());
            esPo.setDelay(msgBody.getDelay());
            esPo.setInlongGroupId(msgBody.getInlongGroupId());
            esPo.setInlongStreamId(msgBody.getInlongStreamId());
            esPo.setSize(msgBody.getSize());
            esPo.setPacketId(msgBody.getPacketId());
            esService.insertData(esPo);
        }
    }

}
