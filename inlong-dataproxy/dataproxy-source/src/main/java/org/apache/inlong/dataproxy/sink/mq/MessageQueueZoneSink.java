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

package org.apache.inlong.dataproxy.sink.mq;

import org.apache.inlong.dataproxy.config.ConfigManager;
import org.apache.inlong.dataproxy.config.holder.ConfigUpdateCallback;
import org.apache.inlong.dataproxy.sink.common.SinkContext;
import org.apache.inlong.dataproxy.utils.BufferQueue;
import org.apache.inlong.sdk.commons.protocol.ProxyEvent;
import org.apache.inlong.sdk.commons.protocol.ProxyPackEvent;

import org.apache.flume.Channel;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.Transaction;
import org.apache.flume.conf.Configurable;
import org.apache.flume.event.SimpleEvent;
import org.apache.flume.sink.AbstractSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * MessageQueueZoneSink
 */
public class MessageQueueZoneSink extends AbstractSink implements Configurable, ConfigUpdateCallback {

    public static final Logger LOG = LoggerFactory.getLogger(MessageQueueZoneSink.class);

    private final long MQ_CLUSTER_STATUS_CHECK_DUR_MS = 2000L;

    private Context parentContext;
    private MessageQueueZoneSinkContext context;
    private List<MessageQueueZoneWorker> workers = new ArrayList<>();
    // message group
    private BatchPackManager dispatchManager;
    private BufferQueue<PackProfile> dispatchQueue;
    // scheduled thread pool
    // reload
    // dispatch
    private ScheduledExecutorService scheduledPool;

    private MessageQueueZoneProducer zoneProducer;
    // configure change notify
    private final Object syncLock = new Object();
    private final AtomicLong lastNotifyTime = new AtomicLong(0);
    // changeListerThread
    private Thread configListener;
    private volatile boolean isShutdown = false;
    // whether mq cluster connected
    private volatile boolean mqClusterStarted = false;
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

    /**
     * start
     */
    @Override
    public void start() {
        try {
            ConfigManager.getInstance().regMetaConfigChgCallback(this);
            // build dispatch queue
            this.dispatchQueue = SinkContext.createBufferQueue();
            this.context = new MessageQueueZoneSinkContext(getName(), parentContext, getChannel(), this.dispatchQueue);
            if (getChannel() == null) {
                LOG.error(getName() + "'s channel is null");
            }
            this.context.start();
            this.dispatchManager = new BatchPackManager(parentContext, dispatchQueue);
            this.scheduledPool = Executors.newScheduledThreadPool(2);
            // dispatch
            this.scheduledPool.scheduleWithFixedDelay(new Runnable() {

                public void run() {
                    dispatchManager.setNeedOutputOvertimeData();
                    zoneProducer.clearExpiredProducers();
                }
            }, this.dispatchManager.getDispatchTimeout(), this.dispatchManager.getDispatchTimeout(),
                    TimeUnit.MILLISECONDS);
            // create producer
            this.zoneProducer = new MessageQueueZoneProducer(this, this.context);
            this.zoneProducer.start();
            // start configure change listener thread
            this.configListener = new Thread(new ConfigChangeProcessor());
            this.configListener.setName(getName() + " configure listener");
            this.configListener.start();
            // create worker
            for (int i = 0; i < context.getMaxThreads(); i++) {
                MessageQueueZoneWorker worker = new MessageQueueZoneWorker(this.getName(), i, context, zoneProducer);
                worker.start();
                this.workers.add(worker);
            }
        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
        }
        super.start();
    }

    /**
     * stop
     */
    @Override
    public void stop() {
        this.isShutdown = true;
        // stop configure listener thread
        if (this.configListener != null) {
            try {
                this.configListener.interrupt();
                configListener.join();
                this.configListener = null;
            } catch (Throwable ee) {
                //
            }
        }
        // stop queue worker
        for (MessageQueueZoneWorker worker : workers) {
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
    public Status process() throws EventDeliveryException {
        // wait mq cluster started
        while (!mqClusterStarted) {
            try {
                Thread.sleep(MQ_CLUSTER_STATUS_CHECK_DUR_MS);
            } catch (InterruptedException e1) {
                return Status.BACKOFF;
            } catch (Throwable e2) {
                //
            }
        }
        this.dispatchManager.outputOvertimeData();
        Channel channel = getChannel();
        Transaction tx = channel.getTransaction();
        tx.begin();
        try {
            Event event = channel.take();
            // no data
            if (event == null) {
                tx.commit();
                return Status.BACKOFF;
            }
            // ProxyEvent
            if (event instanceof ProxyEvent) {
                ProxyEvent proxyEvent = (ProxyEvent) event;
                this.dispatchManager.addEvent(proxyEvent);
                tx.commit();
                return Status.READY;
            }
            // ProxyPackEvent
            if (event instanceof ProxyPackEvent) {
                ProxyPackEvent packEvent = (ProxyPackEvent) event;
                this.dispatchManager.addPackEvent(packEvent);
                tx.commit();
                return Status.READY;
            }
            // SimpleEvent, send as is
            if (event instanceof SimpleEvent) {
                SimpleEvent simpleEvent = (SimpleEvent) event;
                this.dispatchManager.addSimpleEvent(simpleEvent);
                tx.commit();
                return Status.READY;
            }
            tx.commit();
            this.context.addSendFailMetric();
            return Status.READY;
        } catch (Throwable t) {
            LOG.error("Process event failed!" + this.getName(), t);
            try {
                tx.rollback();
            } catch (Throwable e) {
                LOG.error("Channel take transaction rollback exception:" + getName(), e);
            }
            return Status.BACKOFF;
        } finally {
            tx.close();
        }
    }

    @Override
    public void update() {
        if (zoneProducer == null) {
            return;
        }
        lastNotifyTime.set(System.currentTimeMillis());
        syncLock.notifyAll();
    }

    public boolean isMqClusterStarted() {
        return mqClusterStarted;
    }

    public void setMQClusterStarted() {
        this.mqClusterStarted = true;
    }

    private class ConfigChangeProcessor implements Runnable {

        @Override
        public void run() {
            long lastCheckTime;
            while (!isShutdown) {
                try {
                    syncLock.wait();
                } catch (InterruptedException e) {
                    LOG.error("{} config-change processor meet interrupt, exit!", getName());
                    break;
                } catch (Throwable e2) {
                    //
                }
                if (zoneProducer == null) {
                    continue;
                }
                do {
                    lastCheckTime = lastNotifyTime.get();
                    zoneProducer.reloadMetaConfig();
                } while (lastCheckTime != lastNotifyTime.get());
            }
        }
    }
}
