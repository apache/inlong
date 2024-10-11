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

package org.apache.inlong.agent.plugin.sources;

import org.apache.inlong.agent.conf.InstanceProfile;
import org.apache.inlong.agent.except.FileException;
import org.apache.inlong.agent.plugin.sources.file.AbstractSource;
import org.apache.inlong.agent.plugin.sources.file.extend.DefaultExtendedHandler;

import org.apache.commons.lang3.ObjectUtils;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.apache.pulsar.client.api.SubscriptionType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.apache.inlong.agent.constant.TaskConstants.RESTORE_FROM_DB;
import static org.apache.inlong.agent.constant.TaskConstants.TASK_PULSAR_RESET_TIME;
import static org.apache.inlong.agent.constant.TaskConstants.TASK_PULSAR_SERVICE_URL;
import static org.apache.inlong.agent.constant.TaskConstants.TASK_PULSAR_SUBSCRIPTION;
import static org.apache.inlong.agent.constant.TaskConstants.TASK_PULSAR_SUBSCRIPTION_POSITION;
import static org.apache.inlong.agent.constant.TaskConstants.TASK_PULSAR_SUBSCRIPTION_TYPE;

public class PulsarSource extends AbstractSource {

    private static final Logger LOGGER = LoggerFactory.getLogger(PulsarSource.class);
    private String topic;
    private String serviceUrl;
    private String subscription;
    private String subscriptionType;
    private PulsarClient pulsarClient;
    private Long timestamp;
    private final static String PULSAR_SUBSCRIPTION_PREFIX = "inlong-agent-";
    private final static String SUBSCRIPTION_CUSTOM = "Custom";
    private boolean isRestoreFromDB = false;
    private Consumer<byte[]> consumer;
    private long offset = 0L;

    public PulsarSource() {
    }

    @Override
    protected void initExtendClass() {
        extendClass = DefaultExtendedHandler.class.getCanonicalName();
    }

    @Override
    protected void initSource(InstanceProfile profile) {
        try {
            LOGGER.info("PulsarSource init: {}", profile.toJsonStr());
            topic = profile.getInstanceId();
            serviceUrl = profile.get(TASK_PULSAR_SERVICE_URL);
            subscription = profile.get(TASK_PULSAR_SUBSCRIPTION, PULSAR_SUBSCRIPTION_PREFIX + inlongStreamId);
            subscriptionType = profile.get(TASK_PULSAR_SUBSCRIPTION_TYPE, SubscriptionType.Shared.name());
            timestamp = profile.getLong(TASK_PULSAR_RESET_TIME, 0);
            pulsarClient = PulsarClient.builder().serviceUrl(serviceUrl).build();
            isRestoreFromDB = profile.getBoolean(RESTORE_FROM_DB, false);
            consumer = getConsumer();
        } catch (Exception ex) {
            stopRunning();
            throw new FileException("error init stream for " + topic, ex);
        }
    }

    @Override
    protected String getThreadName() {
        return "pulsar-source-" + taskId + "-" + instanceId;
    }

    @Override
    protected boolean doPrepareToRead() {
        return true;
    }

    @Override
    protected List<SourceData> readFromSource() {
        List<SourceData> dataList = new ArrayList<>();
        org.apache.pulsar.client.api.Message<byte[]> message = null;
        try {
            message = consumer.receive(0, TimeUnit.MILLISECONDS);
        } catch (PulsarClientException e) {
            LOGGER.error("read from pulsar error", e);
        }
        if (!ObjectUtils.isEmpty(message)) {
            offset = message.getSequenceId();
            dataList.add(new SourceData(message.getValue(), new String(message.getMessageId().toByteArray(),
                    StandardCharsets.UTF_8)));
            try {
                consumer.acknowledge(message);
            } catch (PulsarClientException e) {
                LOGGER.error("ack pulsar error", e);
            }
        }

        return dataList;
    }

    private Consumer<byte[]> getConsumer() {
        Consumer<byte[]> consumer = null;
        try {
            String position = profile.get(TASK_PULSAR_SUBSCRIPTION_POSITION, SubscriptionInitialPosition.Latest.name());
            if (position.equals(SUBSCRIPTION_CUSTOM)) {
                consumer = pulsarClient.newConsumer(Schema.BYTES)
                        .topic(topic)
                        .subscriptionName(subscription)
                        .subscriptionType(SubscriptionType.valueOf(subscriptionType))
                        .subscribe();
                if (!isRestoreFromDB) {
                    if (timestamp == 0L) {
                        LOGGER.error("Reset consume but timestamp is 0L");
                    } else {
                        consumer.seek(timestamp);
                        LOGGER.info("Reset consume from {}", timestamp);
                    }
                }
            } else {
                consumer = pulsarClient.newConsumer(Schema.BYTES)
                        .topic(topic)
                        .subscriptionName(subscription)
                        .subscriptionInitialPosition(SubscriptionInitialPosition.valueOf(position))
                        .subscriptionType(SubscriptionType.valueOf(subscriptionType))
                        .subscribe();
                LOGGER.info("Skip to reset consume");
            }
            return consumer;
        } catch (PulsarClientException | IllegalArgumentException e) {
            if (consumer != null) {
                try {
                    consumer.close();
                } catch (PulsarClientException ex) {
                    LOGGER.error("close consumer error", e);
                }
            }
            LOGGER.error("get consumer error", e);
        }
        return null;
    }

    @Override
    protected void printCurrentState() {
        LOGGER.info("pulsar topic is {}, offset is {}", topic, offset);
    }

    @Override
    protected boolean isRunnable() {
        return runnable;
    }

    @Override
    protected void releaseSource() {
        if (consumer != null) {
            try {
                consumer.close();
            } catch (PulsarClientException e) {
                LOGGER.error("close consumer error", e);
            }
        }
    }

    @Override
    public boolean sourceExist() {
        return true;
    }
}
