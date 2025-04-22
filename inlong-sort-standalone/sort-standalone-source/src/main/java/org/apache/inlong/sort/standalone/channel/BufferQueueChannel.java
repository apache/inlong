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

package org.apache.inlong.sort.standalone.channel;

import org.apache.inlong.sort.standalone.config.holder.CommonPropertiesHolder;
import org.apache.inlong.sort.standalone.utils.BufferQueue;
import org.apache.inlong.sort.standalone.utils.InlongLoggerFactory;
import org.apache.inlong.sort.standalone.utils.SizeSemaphore;

import com.google.common.base.Preconditions;
import org.apache.flume.ChannelException;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.Transaction;
import org.apache.flume.channel.AbstractChannel;
import org.slf4j.Logger;

import java.util.Date;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.atomic.AtomicLong;

/**
 *
 * BufferQueueChannel
 */
public class BufferQueueChannel extends AbstractChannel {

    public static final Logger LOG = InlongLoggerFactory.getLogger(BufferQueueChannel.class);

    public static final String KEY_MAX_BUFFERQUEUE_SIZE_KB = "maxBufferQueueSizeKb";
    public static final String KEY_RELOADINTERVAL = "reloadInterval";
    public static final String KEY_TASK_NAME = "taskName";
    public static final int DEFAULT_MAX_BUFFERQUEUE_SIZE_KB = 128 * 1024;

    // global buffer size
    private static SizeSemaphore globalBufferQueueSizeKb;
    private BufferQueue<ProfileEvent> bufferQueue;
    private ThreadLocal<ProfileTransaction> currentTransaction = new ThreadLocal<ProfileTransaction>();
    protected Timer channelTimer;
    private AtomicLong takeCounter = new AtomicLong(0);
    private AtomicLong putCounter = new AtomicLong(0);
    private String taskName;

    /**
     * Constructor
     */
    public BufferQueueChannel() {
        Context context = CommonPropertiesHolder.getContext();
        SizeSemaphore globalSize = getGlobalBufferQueueSizeKb(context);
        this.bufferQueue = new BufferQueue<>(globalSize.maxSize() / 3, globalSize);
    }

    /**
     * put
     *
     * @param  event
     * @throws ChannelException
     */
    @Override
    public void put(Event event) throws ChannelException {
        putCounter.incrementAndGet();
        int eventSize = event.getBody().length;
        this.bufferQueue.acquire(eventSize);
        ProfileTransaction transaction = currentTransaction.get();
        Preconditions.checkState(transaction != null, "No transaction exists for this thread");
        if (event instanceof ProfileEvent) {
            ProfileEvent profile = (ProfileEvent) event;
            transaction.doPut(profile);
        } else {
            ProfileEvent profile = new ProfileEvent(event.getHeaders(), event.getBody());
            transaction.doPut(profile);
        }
    }

    /**
     * take
     *
     * @return                  Event
     * @throws ChannelException
     */
    @Override
    public Event take() throws ChannelException {
        ProfileEvent event = this.bufferQueue.pollRecord();
        if (event != null) {
            ProfileTransaction transaction = currentTransaction.get();
            Preconditions.checkState(transaction != null, "No transaction exists for this thread");
            transaction.doTake(event);
            takeCounter.incrementAndGet();
        }
        return event;
    }

    /**
     * getTransaction
     *
     * @return
     */
    @Override
    public Transaction getTransaction() {
        ProfileTransaction newTransaction = new ProfileTransaction(this.bufferQueue);
        this.currentTransaction.set(newTransaction);
        return newTransaction;
    }

    /**
     * start
     */
    @Override
    public void start() {
        super.start();
        try {
            this.setReloadTimer();
        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
        }
    }

    @Override
    public synchronized void stop() {
        super.stop();
        ProfileEvent event = this.bufferQueue.pollRecord();
        while (event != null) {
            this.bufferQueue.release(event.getBody().length);
            event = this.bufferQueue.pollRecord();
        }
    }

    /**
     * setReloadTimer
     */
    protected void setReloadTimer() {
        channelTimer = new Timer(true);
        long reloadInterval = CommonPropertiesHolder.getLong(KEY_RELOADINTERVAL, 60000L);
        TimerTask channelTask = new TimerTask() {

            public void run() {
                LOG.info("taskName:{},queueSize:{},availablePermits:{},put:{},take:{}",
                        taskName,
                        bufferQueue.size(),
                        bufferQueue.availablePermits(),
                        putCounter.getAndSet(0),
                        takeCounter.getAndSet(0));
            }
        };
        channelTimer.schedule(channelTask,
                new Date(System.currentTimeMillis() + reloadInterval),
                reloadInterval);
    }

    /**
     * configure
     *
     * @param context
     */
    @Override
    public void configure(Context context) {
        this.taskName = context.getString(KEY_TASK_NAME);
    }

    /**
     * getGlobalBufferQueueSizeKb
     *
     * @return
     */
    public static SizeSemaphore getGlobalBufferQueueSizeKb(Context context) {
        if (globalBufferQueueSizeKb != null) {
            return globalBufferQueueSizeKb;
        }
        synchronized (LOG) {
            if (globalBufferQueueSizeKb != null) {
                return globalBufferQueueSizeKb;
            }
            int maxBufferQueueSizeKb = context.getInteger(KEY_MAX_BUFFERQUEUE_SIZE_KB, DEFAULT_MAX_BUFFERQUEUE_SIZE_KB);
            globalBufferQueueSizeKb = new SizeSemaphore(maxBufferQueueSizeKb, SizeSemaphore.ONEKB);
            return globalBufferQueueSizeKb;
        }
    }
}
