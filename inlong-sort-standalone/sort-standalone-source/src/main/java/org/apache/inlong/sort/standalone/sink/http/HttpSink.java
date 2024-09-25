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

package org.apache.inlong.sort.standalone.sink.http;

import org.apache.inlong.sort.standalone.channel.ProfileEvent;
import org.apache.inlong.sort.standalone.config.holder.CommonPropertiesHolder;
import org.apache.inlong.sort.standalone.dispatch.DispatchManager;
import org.apache.inlong.sort.standalone.dispatch.DispatchProfile;
import org.apache.inlong.sort.standalone.sink.SinkContext;
import org.apache.inlong.sort.standalone.utils.BufferQueue;

import org.apache.flume.Channel;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.Transaction;
import org.apache.flume.conf.Configurable;
import org.apache.flume.sink.AbstractSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class HttpSink extends AbstractSink implements Configurable {

    public static final Logger LOG = LoggerFactory.getLogger(HttpSink.class);

    private Context parentContext;
    // dispatch
    private DispatchManager dispatchManager;
    private BufferQueue<DispatchProfile> dispatchQueue;
    private ScheduledExecutorService scheduledPool;
    private HttpSinkContext context;
    // workers
    private List<HttpChannelWorker> workers = new ArrayList<>();

    @Override
    public void start() {
        super.start();
        try {
            // dispatch
            LinkedBlockingQueue<DispatchProfile> bufferQueue = new LinkedBlockingQueue<>();
            int maxBufferQueueSizeKb = CommonPropertiesHolder.getInteger(SinkContext.KEY_MAX_BUFFERQUEUE_SIZE_KB,
                    SinkContext.DEFAULT_MAX_BUFFERQUEUE_SIZE_KB);
            this.dispatchQueue = new BufferQueue<>(maxBufferQueueSizeKb, bufferQueue);
            this.dispatchManager = new DispatchManager(parentContext, bufferQueue);
            this.scheduledPool = Executors.newSingleThreadScheduledExecutor();
            this.scheduledPool.scheduleWithFixedDelay(new Runnable() {

                public void run() {
                    dispatchManager.setNeedOutputOvertimeData();
                }
            }, this.dispatchManager.getDispatchTimeout(), this.dispatchManager.getDispatchTimeout(),
                    TimeUnit.MILLISECONDS);
            // send queue
            this.context = new HttpSinkContext(getName(), parentContext, getChannel(), dispatchQueue);
            this.context.start();
            for (int i = 0; i < context.getMaxThreads(); i++) {
                HttpChannelWorker worker = new HttpChannelWorker(context, i);
                this.workers.add(worker);
                worker.start();
            }
        } catch (Exception e) {
            LOG.error("Failed to start HttpSink '{}': {}", this.getName(), e.getMessage());
        }
    }

    @Override
    public void stop() {
        super.stop();
        try {
            this.context.close();
            for (HttpChannelWorker worker : this.workers) {
                worker.close();
            }
            this.workers.clear();
        } catch (Exception e) {
            LOG.error("Failed to stop HttpSink '{}': {}", this.getName(), e.getMessage());
        }
    }

    @Override
    public void configure(Context context) {
        LOG.info("Start to configure:{}, context:{}.", this.getName(), context.toString());
        this.parentContext = context;
    }

    @Override
    public Status process() throws EventDeliveryException {
        dispatchManager.outputOvertimeData();
        Channel channel = getChannel();
        Transaction tx = channel.getTransaction();
        tx.begin();
        try {
            Event event = channel.take();
            if (event == null) {
                tx.commit();
                return Status.BACKOFF;
            }
            if (event instanceof ProfileEvent) {
                ProfileEvent profileEvent = (ProfileEvent) event;
                this.dispatchQueue.acquire(profileEvent.getBody().length);
                this.dispatchManager.addEvent(profileEvent);
                tx.commit();
                return Status.READY;
            } else if (event instanceof DispatchProfile) {
                DispatchProfile dispatchProfile = (DispatchProfile) event;
                for (ProfileEvent profileEvent : dispatchProfile.getEvents()) {
                    this.dispatchQueue.acquire(profileEvent.getBody().length);
                    this.dispatchManager.addEvent(profileEvent);
                }
                tx.commit();
                return Status.READY;
            } else {
                LOG.error("event is not ProfileEvent or DispatchProfile,class:{}", event.getClass());
                tx.commit();
                this.context.addSendFailMetric();
                return Status.READY;
            }
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
}
