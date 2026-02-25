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

package org.apache.inlong.manager.service.resource.queue.pulsar;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * QueryCountDownLatch for managing query task and data completion.
 * <p>
 * This class provides two types of countdown:
 * <ul>
 *   <li>Task countdown: tracks the number of tasks completed (regardless of success or failure)</li>
 *   <li>Data countdown: tracks the number of data items retrieved</li>
 * </ul>
 * The flagLatch is released when either all tasks complete or enough data is collected.
 */
public class QueryCountDownLatch {

    private final CountDownLatch dataLatch;
    private final CountDownLatch taskLatch;
    private final CountDownLatch flagLatch;

    public QueryCountDownLatch(int dataSize, int taskSize) {
        this.dataLatch = new CountDownLatch(dataSize);
        this.taskLatch = new CountDownLatch(taskSize);
        this.flagLatch = new CountDownLatch(1);
    }

    /**
     * Called when a task completes (regardless of success or failure).
     * This should be called in a finally block to ensure it's always executed.
     */
    public void taskCountDown() {
        this.taskLatch.countDown();
        checkAndRelease();
    }

    /**
     * Called when data items are successfully retrieved.
     *
     * @param dataDownSize the number of data items retrieved
     */
    public void dataCountDown(int dataDownSize) {
        for (int i = 0; i < dataDownSize; i++) {
            this.dataLatch.countDown();
        }
        checkAndRelease();
    }

    /**
     * Check if the flagLatch should be released.
     * Release when all tasks complete or enough data is collected.
     */
    private synchronized void checkAndRelease() {
        if (this.flagLatch.getCount() > 0
                && (this.taskLatch.getCount() == 0 || this.dataLatch.getCount() == 0)) {
            this.flagLatch.countDown();
        }
    }

    public void await() throws InterruptedException {
        this.flagLatch.await();
    }

    public boolean await(long timeout, TimeUnit unit) throws InterruptedException {
        return this.flagLatch.await(timeout, unit);
    }
}
