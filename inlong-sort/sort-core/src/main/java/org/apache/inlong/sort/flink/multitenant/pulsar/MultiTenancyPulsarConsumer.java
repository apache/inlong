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

package org.apache.inlong.sort.flink.multitenant.pulsar;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Consumer;
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext;
import org.apache.inlong.sort.flink.pulsar.PulsarSourceFunction;
import org.apache.inlong.sort.flink.multitenant.tubemq.MultiTenancyTubeConsumer;
import org.apache.inlong.sort.meta.MetaManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MultiTenancyPulsarConsumer<T> {

    private static final Logger LOG = LoggerFactory.getLogger(MultiTenancyTubeConsumer.class);

    private final ConcurrentMap<Long, PulsarPullThread> pulsarPullThreads;

    private final Map<Long, PulsarSourceFunction<T>> pulsarSourceFunctions;

    private final Consumer<Throwable> exceptionCatcher;

    private SourceContext<T> context;

    public MultiTenancyPulsarConsumer(Consumer<Throwable> exceptionCatcher) {
        this.exceptionCatcher = checkNotNull(exceptionCatcher);
        this.pulsarPullThreads = new ConcurrentHashMap<>();
        this.pulsarSourceFunctions = new HashMap<>();
    }

    public void start(SourceContext<T> context) throws Exception {
        this.context = checkNotNull(context);
    }

    public void cancel() {
        for (PulsarPullThread pulsarPullThread : pulsarPullThreads.values()) {
            pulsarPullThread.cancel();
            pulsarPullThread.interrupt();
        }
    }

    /**
     * Release pulsarPullThreads.
     *
     * @throws Exception
     */
    public void close() throws Exception {
        MetaManager.release();
        cancel();
        for (PulsarPullThread pulsarPullThread : pulsarPullThreads.values()) {
            try {
                pulsarPullThread.join();
            } catch (InterruptedException e) {
                LOG.error("Could not cancel thread {}", pulsarPullThread.getName());
            }
        }
        pulsarPullThreads.clear();

        for (PulsarSourceFunction<T> sourceFunction : pulsarSourceFunctions.values()) {
            try {
                sourceFunction.close();
            } catch (Throwable throwable) {
                LOG.warn("Could not properly shutdown the pulsar pull consumer.", throwable);
            }
        }
        pulsarSourceFunctions.clear();
    }

    /**
     * Add a new pulsar source.
     */
    public void addPulsarSource(long dataflowId, PulsarSourceFunction<T> pulsarSourceFunction) throws Exception {
        if (pulsarPullThreads.containsKey(dataflowId)) {
            LOG.warn("Pull thread of dataflow-id {} has already been started", dataflowId);
            return;
        }
        PulsarPullThread pulsarPullThread = new PulsarPullThread(pulsarSourceFunction);
        pulsarPullThread.setName("PulsarPullThread-for-dataflowId-" + dataflowId);
        pulsarPullThread.start();
        pulsarPullThreads.put(dataflowId, pulsarPullThread);
        pulsarSourceFunctions.put(dataflowId, pulsarSourceFunction);

        LOG.info("Add pulsar source \"{}\" successfully!", dataflowId);
    }

    public void updatePulsarSource(long dataflowId, PulsarSourceFunction<T> pulsarSourceFunction) throws Exception {
        removePulsarSource(dataflowId);
        addPulsarSource(dataflowId, pulsarSourceFunction);
    }

    public void removePulsarSource(long dataflowId) {
        PulsarPullThread pulsarPullThread = pulsarPullThreads.get(dataflowId);
        if (pulsarPullThread != null) {
            pulsarPullThread.cancel();
            pulsarPullThread.interrupt();
            try {
                pulsarPullThread.join();
            } catch (InterruptedException e) {
                LOG.error("Could not cancel thread {}", pulsarPullThread.getName());
            }
            pulsarPullThreads.remove(dataflowId);
        }
        PulsarSourceFunction<T> sourceFunction = pulsarSourceFunctions.get(dataflowId);
        if (sourceFunction != null) {
            try {
                sourceFunction.close();
            } catch (Exception e) {
                LOG.error("Could not properly shutdown the pulsar source function.", e);
            }
            pulsarSourceFunctions.remove(dataflowId);
        }
    }

    public Collection<PulsarSourceFunction<T>> getCurrentSourceFunction() {
        return pulsarSourceFunctions.values();
    }

    public class PulsarPullThread extends Thread {

        private volatile boolean running = true;
        private final PulsarSourceFunction<T> sourceFunction;

        public PulsarPullThread(PulsarSourceFunction<T> sourceFunction) {
            this.sourceFunction = checkNotNull(sourceFunction);
        }

        @Override
        public void run() {
            try {
                sourceFunction.run(context);
            } catch (InterruptedException e) {
                // ignore interruption from cancelling
                if (running) {
                    exceptionCatcher.accept(e);
                    LOG.warn("pulsar pull thread has been interrupted.", e);
                }
            } catch (Throwable t) {
                exceptionCatcher.accept(t);
                LOG.warn("Error occurred in pulsar pull thread.", t);
            } finally {
                LOG.info("pulsar pull thread stops");
            }
        }

        public void cancel() {
            this.running = false;
            sourceFunction.cancel();
        }
    }
}
