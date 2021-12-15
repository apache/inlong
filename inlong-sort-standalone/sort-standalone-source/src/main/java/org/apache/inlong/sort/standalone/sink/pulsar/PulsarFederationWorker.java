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

package org.apache.inlong.sort.standalone.sink.pulsar;

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.flume.Channel;
import org.apache.flume.Event;
import org.apache.flume.Transaction;
import org.apache.flume.lifecycle.LifecycleState;
import org.apache.inlong.sort.standalone.config.pojo.InlongId;
import org.apache.inlong.sort.standalone.metrics.SortMetricItem;
import org.apache.inlong.sort.standalone.utils.Constants;
import org.apache.inlong.sort.standalone.utils.InlongLoggerFactory;
import org.slf4j.Logger;

/**
 * 
 * PulsarFederationWorker
 */
public class PulsarFederationWorker extends Thread {

    public static final Logger LOG = InlongLoggerFactory.getLogger(PulsarFederationWorker.class);

    private final String workerName;
    private final PulsarFederationSinkContext context;

    private PulsarProducerFederation producerFederation;
    private LifecycleState status;
    private Map<String, String> dimensions;

    /**
     * Constructor
     * 
     * @param sinkName
     * @param workerIndex
     * @param context
     */
    public PulsarFederationWorker(String sinkName, int workerIndex, PulsarFederationSinkContext context) {
        super();
        this.workerName = sinkName + "-worker-" + workerIndex;
        this.context = context;
        this.producerFederation = new PulsarProducerFederation(workerName, this.context);
        this.status = LifecycleState.IDLE;
        this.dimensions = new HashMap<>();
        this.dimensions.put(SortMetricItem.KEY_CLUSTER_ID, this.context.getClusterId());
        this.dimensions.put(SortMetricItem.KEY_TASK_NAME, this.context.getTaskName());
        this.dimensions.put(SortMetricItem.KEY_SINK_ID, this.context.getSinkName());
    }

    /**
     * start
     */
    @Override
    public void start() {
        this.producerFederation.start();
        this.status = LifecycleState.START;
        super.start();
    }

    /**
     * 
     * close
     */
    public void close() {
        // close all producers
        this.producerFederation.close();
        this.status = LifecycleState.STOP;
    }

    /**
     * run
     */
    @Override
    public void run() {
        LOG.info(String.format("start PulsarSetWorker:%s", this.workerName));
        while (status != LifecycleState.STOP) {
            Channel channel = context.getChannel();
            Transaction tx = channel.getTransaction();
            tx.begin();
            try {
                Event event = channel.take();
                if (event == null) {
                    tx.commit();
                    sleepOneInterval();
                    continue;
                }
                // fill topic
                this.fillTopic(event);
                // metric
                SortMetricItem.fillInlongId(event, dimensions);
                this.dimensions.put(SortMetricItem.KEY_SINK_DATA_ID, event.getHeaders().get(Constants.TOPIC));
                SortMetricItem metricItem = this.context.getMetricItemSet().findMetricItem(dimensions);
                metricItem.sendCount.incrementAndGet();
                metricItem.sendSize.addAndGet(event.getBody().length);
                // send
                this.producerFederation.send(event, tx);
            } catch (Throwable t) {
                LOG.error("Process event failed!" + this.getName(), t);
                try {
                    tx.rollback();
                    tx.close();
                    // metric
                    SortMetricItem metricItem = this.context.getMetricItemSet().findMetricItem(dimensions);
                    metricItem.readFailCount.incrementAndGet();
                    sleepOneInterval();
                } catch (Throwable e) {
                    LOG.error("Channel take transaction rollback exception:" + getName(), e);
                }
            }
        }
    }

    /**
     * fillTopic
     * 
     * @param currentRecord
     */
    private void fillTopic(Event currentRecord) {
        Map<String, String> headers = currentRecord.getHeaders();
        String inlongGroupId = headers.get(Constants.INLONG_GROUP_ID);
        String inlongStreamId = headers.get(Constants.INLONG_STREAM_ID);
        String uid = InlongId.generateUid(inlongGroupId, inlongStreamId);
        String topic = this.context.getTopic(uid);
        if (!StringUtils.isBlank(topic)) {
            headers.put(Constants.TOPIC, topic);
        }
    }

    /**
     * sleepOneInterval
     */
    private void sleepOneInterval() {
        try {
            Thread.sleep(context.getProcessInterval());
        } catch (InterruptedException e1) {
            LOG.error(e1.getMessage(), e1);
        }
    }
}
