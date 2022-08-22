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

import org.apache.commons.lang3.math.NumberUtils;
import org.apache.flume.Channel;
import org.apache.flume.CounterGroup;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.Sink;
import org.apache.flume.SinkProcessor;
import org.apache.flume.SinkRunner;
import org.apache.flume.Source;
import org.apache.flume.lifecycle.LifecycleState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * <p>
 * A driver for {@linkplain Sink sinks} that polls them, attempting to
 * {@linkplain Sink#process() process} events if any are available in the
 * {@link Channel}.
 * </p>
 *
 * <p>
 * Note that, unlike {@linkplain Source sources}, all sinks are polled.
 * </p>
 *
 * @see org.apache.flume.Sink
 * @see org.apache.flume.SourceRunner
 */
public class InlongSinkRunner extends SinkRunner {

    public static final Logger LOG = LoggerFactory.getLogger(InlongSinkRunner.class);
    public static final String KEY_BACKOFFSLEEPINCREMENT_MS = "backoffSleepIncrementMs";
    public static final long DEFAULT_BACKOFFSLEEPINCREMENT_MS = 10;
    public static final String KEY_MAXBACKOFFSLEEP_MS = "maxBackoffSleepMs";
    public static final long DEFAULT_MAXBACKOFFSLEEP_MS = 5000;

    private Map<String, String> commonProperties;
    private CounterGroup inlongCounterGroup;
    private InlongPollingRunner inlongRunner;
    private Thread inlongRunnerThread;
    private LifecycleState inlongLifecycleState;

    /**
     * Constructor
     * @param commonProperties
     */
    public InlongSinkRunner(Map<String, String> commonProperties) {
        super();
        this.commonProperties = commonProperties;
        inlongCounterGroup = new CounterGroup();
        inlongLifecycleState = LifecycleState.IDLE;
    }

    /**
     * Constructor
     * @param policy
     * @param commonProperties
     */
    public InlongSinkRunner(SinkProcessor policy, Map<String, String> commonProperties) {
        this(commonProperties);
        setSink(policy);
    }

    /**
     * start
     */
    @Override
    public void start() {
        SinkProcessor policy = getPolicy();

        policy.start();

        long backoffSleepIncrementMs = NumberUtils.toLong(
                commonProperties.get(KEY_BACKOFFSLEEPINCREMENT_MS),
                DEFAULT_BACKOFFSLEEPINCREMENT_MS);
        long maxBackoffSleepMs = NumberUtils.toLong(
                commonProperties.get(KEY_MAXBACKOFFSLEEP_MS),
                DEFAULT_MAXBACKOFFSLEEP_MS);
        inlongRunner = new InlongPollingRunner(backoffSleepIncrementMs, maxBackoffSleepMs);

        inlongRunner.policy = policy;
        inlongRunner.counterGroup = inlongCounterGroup;
        inlongRunner.shouldStop = new AtomicBoolean();

        inlongRunnerThread = new Thread(inlongRunner);
        inlongRunnerThread.setName("SinkRunner-PollingRunner-" +
                policy.getClass().getSimpleName());
        inlongRunnerThread.start();

        inlongLifecycleState = LifecycleState.START;
    }

    /**
     * stop
     */
    @Override
    public void stop() {

        if (inlongRunnerThread != null) {
            inlongRunner.shouldStop.set(true);
            inlongRunnerThread.interrupt();

            while (inlongRunnerThread.isAlive()) {
                try {
                    LOG.debug("Waiting for runner thread to exit");
                    inlongRunnerThread.join(500);
                } catch (InterruptedException e) {
                    LOG.debug("Interrupted while waiting for runner thread to exit. Exception follows.",
                            e);
                }
            }
        }

        getPolicy().stop();
        inlongLifecycleState = LifecycleState.STOP;
    }

    /**
     * toString
     * @return
     */
    @Override
    public String toString() {
        return "SinkRunner: { policy:" + getPolicy() + " counterGroup:"
                + inlongCounterGroup + " }";
    }

    /**
     * getLifecycleState
     * @return
     */
    @Override
    public LifecycleState getLifecycleState() {
        return inlongLifecycleState;
    }

    /**
     * {@link Runnable} that {@linkplain SinkProcessor#process() polls} a
     * {@link SinkProcessor} and manages event delivery notification,
     * {@link Sink.Status BACKOFF} delay handling, etc.
     */
    public static class InlongPollingRunner implements Runnable {

        private SinkProcessor policy;
        private AtomicBoolean shouldStop;
        private CounterGroup counterGroup;
        private long backoffSleepIncrementMs = DEFAULT_BACKOFFSLEEPINCREMENT_MS;
        private long maxBackoffSleepMs = DEFAULT_MAXBACKOFFSLEEP_MS;

        /**
        * Constructor
        */
        public InlongPollingRunner(long backoffSleepIncrementMs, long maxBackoffSleepMs) {
            this.backoffSleepIncrementMs = backoffSleepIncrementMs;
            this.maxBackoffSleepMs = maxBackoffSleepMs;
        }

        /**
        * run
        */
        @Override
        public void run() {
            LOG.debug("Polling sink runner starting");

            while (!shouldStop.get()) {
                try {
                    if (policy.process().equals(Sink.Status.BACKOFF)) {
                        counterGroup.incrementAndGet("runner.backoffs");

                        Thread.sleep(Math.min(
                                counterGroup.incrementAndGet("runner.backoffs.consecutive")
                                        * backoffSleepIncrementMs,
                                maxBackoffSleepMs));
                    } else {
                        counterGroup.set("runner.backoffs.consecutive", 0L);
                    }
                } catch (InterruptedException e) {
                    LOG.debug("Interrupted while processing an event. Exiting.");
                    counterGroup.incrementAndGet("runner.interruptions");
                } catch (Exception e) {
                    LOG.error("Unable to deliver event. Exception follows.", e);
                    if (e instanceof EventDeliveryException) {
                        counterGroup.incrementAndGet("runner.deliveryErrors");
                    } else {
                        counterGroup.incrementAndGet("runner.errors");
                    }
                    try {
                        Thread.sleep(maxBackoffSleepMs);
                    } catch (InterruptedException ex) {
                        Thread.currentThread().interrupt();
                    }
                }
            }
            LOG.debug("Polling runner exiting. Metrics:{}", counterGroup);
        }

    }
}
