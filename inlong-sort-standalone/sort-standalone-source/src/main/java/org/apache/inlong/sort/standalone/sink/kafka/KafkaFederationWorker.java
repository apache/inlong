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

package org.apache.inlong.sort.standalone.sink.kafka;

import org.apache.inlong.sort.standalone.channel.ProfileEvent;
import org.apache.inlong.sort.standalone.metrics.SortMetricItem;
import org.apache.inlong.sort.standalone.utils.Constants;
import org.apache.inlong.sort.standalone.utils.InlongLoggerFactory;

import com.google.common.base.Preconditions;
import org.apache.commons.lang3.StringUtils;
import org.apache.flume.Channel;
import org.apache.flume.Event;
import org.apache.flume.Transaction;
import org.apache.flume.lifecycle.LifecycleState;
import org.slf4j.Logger;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/** Worker of */
public class KafkaFederationWorker extends Thread {

    public static final Logger LOG = InlongLoggerFactory.getLogger(KafkaFederationWorker.class);

    private final String workerName;
    private final KafkaFederationSinkContext context;

    private final KafkaProducerFederation producerFederation;
    private final Map<String, String> dimensions = new ConcurrentHashMap<>();
    private LifecycleState status;

    /**
     * Constructor of KafkaFederationWorker
     *
     * @param sinkName Name of sink.
     * @param workerIndex Index of this worker thread.
     * @param context Context of kafka sink.
     */
    public KafkaFederationWorker(
            String sinkName, int workerIndex, KafkaFederationSinkContext context) {
        super();
        this.workerName = sinkName + "-" + workerIndex;
        this.context = Preconditions.checkNotNull(context);
        this.producerFederation = new KafkaProducerFederation(String.valueOf(workerIndex), this.context);
        this.status = LifecycleState.IDLE;
        this.dimensions.put(SortMetricItem.KEY_CLUSTER_ID, this.context.getClusterId());
        this.dimensions.put(SortMetricItem.KEY_TASK_NAME, this.context.getTaskName());
        this.dimensions.put(SortMetricItem.KEY_SINK_ID, this.context.getSinkName());
    }

    /** Entrance of KafkaFederationWorker */
    @Override
    public void start() {
        LOG.info("start a new kafka worker {}", this.workerName);
        this.producerFederation.start();
        this.status = LifecycleState.START;
        super.start();
    }

    /** Close */
    public void close() {
        // close all producers
        LOG.info("close a kafka worker {}", this.workerName);
        this.producerFederation.close();
        this.status = LifecycleState.STOP;
    }

    @Override
    public void run() {
        LOG.info("worker {} start to run, the state is {}", this.workerName, status.name());
        while (status != LifecycleState.STOP) {
            KafkaTransaction ktx = KafkaTransaction.builder().dataFlowIds(new HashSet<>()).build();
            try {
                Channel channel = context.getChannel();
                Transaction tx = channel.getTransaction();
                tx.begin();
                ktx.setTx(tx);
                Event rowEvent = channel.take();

                // if event is null, close tx and sleep for a while.
                if (rowEvent == null) {
                    ktx.negativeAck();
                    sleepOneInterval();
                    continue;
                }
                // if event is not ProfileEvent
                if (!(rowEvent instanceof ProfileEvent)) {
                    ktx.negativeAck();
                    LOG.error("The type of row event is not compatible with ProfileEvent");
                    continue;
                }

                ProfileEvent profileEvent = (ProfileEvent) rowEvent;
                ktx.setProfileEvent(profileEvent);
                // if there is not config
                List<KafkaIdConfig> idConfigs = this.context.getIdConfig(profileEvent.getUid());
                if (idConfigs == null) {
                    this.context.addSendResultMetric(profileEvent, profileEvent.getUid(),
                            false, System.currentTimeMillis());
                    ktx.negativeAck();
                    continue;
                }
                // if there is multi-output
                idConfigs.forEach(v -> ktx.getDataFlowIds().add(v.getDataFlowId()));
                this.processEvent(ktx, idConfigs, profileEvent);
            } catch (Exception e) {
                LOG.error(e.getMessage(), e);
                if (ktx.getProfileEvent() != null) {
                    this.context.addSendResultMetric(ktx.getProfileEvent(),
                            ktx.getProfileEvent().getUid(),
                            false, System.currentTimeMillis());
                } else {
                    SortMetricItem metricItem = this.context.getMetricItemSet().findMetricItem(dimensions);
                    metricItem.sendFailCount.incrementAndGet();
                }
                ktx.negativeAck();
            }
        }
    }

    private boolean processEvent(KafkaTransaction ktx, List<KafkaIdConfig> idConfigs, ProfileEvent profileEvent)
            throws IOException {
        for (KafkaIdConfig idConfig : idConfigs) {
            String topic = idConfig.getTopic();
            if (StringUtils.isBlank(topic)) {
                LOG.error("can not find the topic,dataFlowId:{},uid:{}", idConfig.getDataFlowId(), idConfig.getUid());
                this.context.addSendResultMetric(profileEvent, idConfig.getDataFlowId(),
                        false, System.currentTimeMillis());
                // ktx.negativeAck();
                // return false;
                ktx.ack(idConfig.getDataFlowId());
            } else {
                profileEvent.getHeaders().put(Constants.TOPIC, topic);
                this.context.addSendMetric(profileEvent, idConfig.getDataFlowId());
                this.producerFederation.send(ktx, profileEvent, idConfig);
            }
        }
        return true;
    }

    /** sleepOneInterval */
    private void sleepOneInterval() {
        try {
            Thread.sleep(context.getProcessInterval());
        } catch (InterruptedException e1) {
            LOG.error(e1.getMessage(), e1);
        }
    }
}
