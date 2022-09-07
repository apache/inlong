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

package org.apache.inlong.dataproxy.sink.mqzone;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.flume.Channel;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.Sink;
import org.apache.flume.Transaction;
import org.apache.flume.conf.Configurable;
import org.apache.flume.sink.AbstractSink;
import org.apache.inlong.dataproxy.dispatch.DispatchManager;
import org.apache.inlong.dataproxy.dispatch.DispatchProfile;
import org.apache.inlong.dataproxy.sink.pulsar.PulsarClientService;
import org.apache.inlong.sdk.commons.protocol.ProxyEvent;
import org.apache.inlong.sdk.commons.protocol.ProxyPackEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractZoneSink extends AbstractSink implements Configurable {
    public static final Logger LOG = LoggerFactory.getLogger(AbstractZoneSink.class);

    protected Context parentContext;
    protected AbstractZoneSinkContext context;
    protected List<AbstactZoneWorker> workers = new ArrayList<>();
    // message group
    protected DispatchManager dispatchManager;
    protected ArrayList<LinkedBlockingQueue<DispatchProfile>> dispatchQueues = new ArrayList<>();
    // scheduled thread pool
    // reload
    // dispatch
    protected ScheduledExecutorService scheduledPool;

    /**
     * configure
     *
     * @param context
     */
    @Override
    public void configure(Context context) {
        LOG.info("start to configure:{}, context:{}.", this.getClass().getSimpleName(), context.toString());
        this.parentContext = context;
    }

    public void start(ZoneWorkerCalculator zoneWorkerCalculator) {
        try {
            if (getChannel() == null) {
                LOG.error("channel is null");
            }
            this.context.start();
            for (int i = 0; i < context.getMaxThreads(); i++) {
                LinkedBlockingQueue<DispatchProfile> dispatchQueue = new LinkedBlockingQueue<>();
                dispatchQueues.add(dispatchQueue);
            }
            this.dispatchManager = new DispatchManager(parentContext, dispatchQueues);
            this.scheduledPool = Executors.newScheduledThreadPool(2);
            // dispatch
            this.scheduledPool.scheduleWithFixedDelay(new Runnable() {

                                                          public void run() {
                                                              dispatchManager.setNeedOutputOvertimeData();
                                                          }
                                                      }, this.dispatchManager.getDispatchTimeout(),
                    this.dispatchManager.getDispatchTimeout(),
                    TimeUnit.MILLISECONDS);
            // create worker
            for (int i = 0; i < context.getMaxThreads(); i++) {
                AbstactZoneWorker worker = zoneWorkerCalculator.calculator(this.getName(), i, context);
                worker.start();
                this.workers.add(worker);
            }
        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
        }
        super.start();
    }

    @Deprecated
    public void diffSetPublish(PulsarClientService pulsarClientService, Set<String> originalSet, Set<String> endSet) {
        return;
    }

    @Deprecated
    public void diffUpdatePulsarClient(PulsarClientService pulsarClientService, Map<String, String> originalCluster,
                                       Map<String, String> endCluster) {
        this.workers.forEach(worker -> {
            worker.zoneProducer.reload();
        });
    }

    /**
     * stop
     */
    @Override
    public void stop() {
        for (AbstactZoneWorker worker : workers) {
            try {
                worker.close();
            } catch (Throwable e) {
                LOG.error(e.getMessage(), e);
            }
        }
        this.context.close();
        super.stop();
    }

    /**
     * process
     *
     * @return                        Status
     * @throws EventDeliveryException
     */
    @Override
    public Sink.Status process() throws EventDeliveryException {
        this.dispatchManager.outputOvertimeData();
        Channel channel = getChannel();
        Transaction tx = channel.getTransaction();
        tx.begin();
        try {
            Event event = channel.take();
            if (event == null) {
                tx.commit();
                return Sink.Status.BACKOFF;
            }
            // ProxyEvent
            if (event instanceof ProxyEvent) {
                ProxyEvent proxyEvent = (ProxyEvent) event;
                this.dispatchManager.addEvent(proxyEvent);
                tx.commit();
                return Sink.Status.READY;
            }
            // ProxyPackEvent
            if (event instanceof ProxyPackEvent) {
                ProxyPackEvent packEvent = (ProxyPackEvent) event;
                this.dispatchManager.addPackEvent(packEvent);
                tx.commit();
                return Sink.Status.READY;
            }
            tx.commit();
            this.context.addSendFailMetric();
            return Sink.Status.READY;
        } catch (Throwable t) {
            LOG.error("Process event failed!" + this.getName(), t);
            try {
                tx.rollback();
            } catch (Throwable e) {
                LOG.error("Channel take transaction rollback exception:" + getName(), e);
            }
            return Sink.Status.BACKOFF;
        } finally {
            tx.close();
        }
    }
}
