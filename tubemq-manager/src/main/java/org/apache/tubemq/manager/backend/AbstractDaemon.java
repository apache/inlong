/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.tubemq.manager.backend;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Abstract daemon with a batch of working thread.
 */
public abstract class AbstractDaemon implements ThreadStartAndStop {
    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractDaemon.class);

    // worker thread pool
    private final ExecutorService workerServices;
    private final List<CompletableFuture<?>> workerFutures;
    private boolean runnable = true;

    public AbstractDaemon() {
        this.workerServices = Executors
                .newCachedThreadPool(new TubeMQManagerFactory(this.getClass().getSimpleName()));
        this.workerFutures = new ArrayList<>();
    }

    /**
     * Whether threads can in running state with while loop.
     *
     * @return - true if threads can run
     */
    public boolean isRunnable() {
        return runnable;
    }

    /**
     * Stop running threads.
     */
    public void stopRunningThreads() {
        runnable = false;
    }

    /**
     * Submit work thread to thread pool.
     *
     * @param worker - work thread
     */
    public void submitWorker(Runnable worker) {
        CompletableFuture<?> future = CompletableFuture.runAsync(worker, this.workerServices);
        workerFutures.add(future);
        LOGGER.info("{} running worker number is {}", this.getClass().getName(),
                workerFutures.size());
    }

    /**
     * Wait for threads finish.
     */
    public void join() {
        for (CompletableFuture<?> future : workerFutures) {
            future.join();
        }
    }

    /**
     * Stop thread pool and running threads if they're in the running state.
     *
     * @param timeout - max wait time
     * @param timeUnit - time unit
     */
    public void waitForTerminate(long timeout, TimeUnit timeUnit) throws Exception {
        // stopping working threads.
        if (isRunnable()) {
            stopRunningThreads();
            workerServices.shutdown();
            workerServices.awaitTermination(timeout, timeUnit);
        }
    }
}
