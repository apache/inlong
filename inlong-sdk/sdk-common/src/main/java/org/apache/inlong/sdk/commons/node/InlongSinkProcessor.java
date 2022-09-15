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

package org.apache.inlong.sdk.commons.node;

import org.apache.flume.Context;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.Sink;
import org.apache.flume.Sink.Status;
import org.apache.flume.SinkProcessor;
import org.apache.flume.lifecycle.LifecycleState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * InlongSinkProcessor
 * 
 */
public class InlongSinkProcessor implements SinkProcessor {

    public static final Logger LOG = LoggerFactory.getLogger(InlongSinkProcessor.class);
    public static final String KEY_BACKOFFSLEEPINCREMENT_MS = "backoffSleepIncrementMs";
    public static final long DEFAULT_BACKOFFSLEEPINCREMENT_MS = 10;
    public static final String KEY_MAXBACKOFFSLEEP_MS = "maxBackoffSleepMs";
    public static final long DEFAULT_MAXBACKOFFSLEEP_MS = 5000;

    private Sink sink;
    private LifecycleState lifecycleState;
    private AtomicBoolean shouldStop = new AtomicBoolean(false);
    private long backoffSleepIncrementMs = DEFAULT_BACKOFFSLEEPINCREMENT_MS;
    private long maxBackoffSleepMs = DEFAULT_MAXBACKOFFSLEEP_MS;
    private AtomicLong consecutive = new AtomicLong(0L);

    /**
     * start
     */
    @Override
    public void start() {
        sink.start();
        lifecycleState = LifecycleState.START;
    }

    /**
     * stop
     */
    @Override
    public void stop() {
        sink.stop();
        lifecycleState = LifecycleState.STOP;
        shouldStop.set(true);
    }

    /**
     * getLifecycleState
     * @return
     */
    @Override
    public LifecycleState getLifecycleState() {
        return lifecycleState;
    }

    /**
     * configure
     * @param context
     */
    @Override
    public void configure(Context context) {
        this.backoffSleepIncrementMs = context.getLong(KEY_BACKOFFSLEEPINCREMENT_MS,
                DEFAULT_BACKOFFSLEEPINCREMENT_MS);
        this.maxBackoffSleepMs = context.getLong(KEY_MAXBACKOFFSLEEP_MS,
                DEFAULT_MAXBACKOFFSLEEP_MS);
    }

    /**
     * process
     */
    @Override
    public Status process() throws EventDeliveryException {
        while (!shouldStop.get()) {
            try {
                if (sink.process().equals(Sink.Status.BACKOFF)) {
                    long currentSleepMs = consecutive.incrementAndGet() * backoffSleepIncrementMs;
                    if (currentSleepMs > maxBackoffSleepMs) {
                        break;
                    }
                    Thread.sleep(currentSleepMs);
                } else {
                    consecutive.set(0L);
                }
            } catch (Exception e) {
                LOG.error(e.getMessage(), e);
                try {
                    Thread.sleep(maxBackoffSleepMs);
                    break;
                } catch (InterruptedException ex) {
                    Thread.currentThread().interrupt();
                }
            }
        }
        return Sink.Status.BACKOFF;
    }

    /**
     * setSinks
     * @param sinks
     */
    @Override
    public void setSinks(List<Sink> sinks) {
        sink = sinks.get(0);
    }
}
