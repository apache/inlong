/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
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

import com.google.common.base.Preconditions;
import org.apache.flume.Channel;
import org.apache.flume.Event;
import org.apache.flume.Transaction;
import org.apache.flume.lifecycle.LifecycleState;
import org.apache.inlong.sort.standalone.channel.ProfileEvent;
import org.apache.inlong.sort.standalone.config.pojo.InlongId;
import org.apache.inlong.sort.standalone.metrics.SortMetricItem;
import org.apache.inlong.sort.standalone.utils.Constants;
import org.apache.inlong.sort.standalone.utils.InlongLoggerFactory;
import org.apache.pulsar.shade.org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class KafkaFederationWorker extends Thread {
    public static final Logger LOG = InlongLoggerFactory.getLogger(KafkaFederationWorker.class);

    private final String workerName;
    private final KafkaFederationSinkContext context;

    private KafkaProducerFederation producerFederation;
    private LifecycleState status;
    private Map<String, String> dimensions = new ConcurrentHashMap<>();

    /**
     * constructor of KafkaFederationWorker
     *
     * @param sinkName
     * @param workerIndex
     * @param context
     */
    public KafkaFederationWorker(
            String sinkName, int workerIndex, KafkaFederationSinkContext context) {
        super();
        this.workerName = sinkName + "-" + workerIndex;
        this.context = Preconditions.checkNotNull(context);
        this.producerFederation =
                new KafkaProducerFederation(String.valueOf(workerIndex), this.context);
        this.status = LifecycleState.IDLE;
        this.dimensions.put(SortMetricItem.KEY_CLUSTER_ID, this.context.getClusterId());
        this.dimensions.put(SortMetricItem.KEY_TASK_NAME, this.context.getTaskName());
        this.dimensions.put(SortMetricItem.KEY_SINK_ID, this.context.getSinkName());
    }

    /** entrance of KafkaFederationWorker */
    @Override
    public void start() {
        LOG.info("start a new kafka worker {}", this.workerName);
        this.producerFederation.start();
        this.status = LifecycleState.START;
        super.start();
    }

    /** close */
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
            Transaction tx = null;
            try {
                Channel channel = context.getChannel();
                if (channel == null) {
                    LOG.error("in kafka worker, channel is null ");
                    break;
                }
                tx = channel.getTransaction();
                tx.begin();
                Event rowEvent = channel.take();
                if (rowEvent == null) {
                    tx.commit();
                    tx.close();
                    sleepOneInterval();
                    continue;
                }
                ProfileEvent event = (ProfileEvent) rowEvent;
                this.fillTopic(event);
                SortMetricItem.fillInlongId(event, dimensions);
                this.dimensions.put(
                        SortMetricItem.KEY_SINK_DATA_ID, event.getHeaders().get(Constants.TOPIC));
                long msgTime = event.getRawLogTime();
                long auditFormatTime = msgTime - msgTime % 60000L;
                dimensions.put(SortMetricItem.KEY_MESSAGE_TIME, String.valueOf(auditFormatTime));
                SortMetricItem metricItem =
                        this.context.getMetricItemSet().findMetricItem(dimensions);
                metricItem.sendCount.incrementAndGet();
                metricItem.sendSize.addAndGet(event.getBody().length);
                this.producerFederation.send(event, tx);
            } catch (Exception e) {
                LOG.error(e.getMessage(), e);
                if (tx != null) {
                    tx.rollback();
                    tx.close();
                }
                // metric
                SortMetricItem metricItem =
                        this.context.getMetricItemSet().findMetricItem(dimensions);
                metricItem.sendFailCount.incrementAndGet();
                sleepOneInterval();
            }
        }
    }

    /**
     * fillTopic
     *
     * @param event
     */
    private void fillTopic(Event event) {
        Map<String, String> headers = event.getHeaders();
        String inlongGroupId = headers.get(Constants.INLONG_GROUP_ID);
        String inlongStreamId = headers.get(Constants.INLONG_STREAM_ID);
        String uid = InlongId.generateUid(inlongGroupId, inlongStreamId);
        String topic = this.context.getTopic(uid);
        if (!StringUtils.isBlank(topic)) {
            headers.put(Constants.TOPIC, topic);
        }
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
