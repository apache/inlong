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
 * QueryCountDownLatch
 */
public class QueryCountDownLatch {

    private CountDownLatch dataLatch;
    private CountDownLatch taskLatch;
    private CountDownLatch flagLatch;

    public QueryCountDownLatch(int dataSize, int taskSize) {
        this.dataLatch = new CountDownLatch(dataSize);
        this.taskLatch = new CountDownLatch(taskSize);
        this.flagLatch = new CountDownLatch(1);
    }

    public void countDown(int dataDownSize) {
        this.taskLatch.countDown();
        for (int i = 0; i < dataDownSize; i++) {
            this.dataLatch.countDown();
        }
        if (this.taskLatch.getCount() == 0 || this.dataLatch.getCount() == 0) {
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
