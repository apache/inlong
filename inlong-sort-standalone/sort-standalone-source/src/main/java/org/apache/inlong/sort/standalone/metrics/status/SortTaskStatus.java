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

package org.apache.inlong.sort.standalone.metrics.status;

import lombok.Data;

import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicLong;

/**
 * SortTaskStatus
 */
@Data
public class SortTaskStatus {

    private String taskName;
    private boolean hasFirstSuccess = false;
    private Semaphore firstSuccessPermit = new Semaphore(1);
    private AtomicLong sendCount = new AtomicLong(0);
    private AtomicLong sendFailCount = new AtomicLong(0);
    private long previousPauseSortTaskTime = Long.MIN_VALUE;

    public SortTaskStatus(String taskName) {
        this.taskName = taskName;
    }

    public void tryFirstSend() throws InterruptedException {
        firstSuccessPermit.acquire(1);
    }

    public void firstSuccess() {
        hasFirstSuccess = true;
        firstSuccessPermit.release(1);
    }

    public boolean needPauseSortTask(int failCountLimit, int failCountPercentLimit) {
        long sendFail = this.sendFailCount.getAndSet(0);
        long send = this.sendCount.getAndSet(0);
        if (sendFail < failCountLimit) {
            return false;
        }
        if (send <= 0) {
            return false;
        }
        long rate = sendFail * 100 / send;
        if (rate < failCountPercentLimit) {
            return false;
        }
        this.previousPauseSortTaskTime = System.currentTimeMillis();
        return true;
    }

    public boolean canResumeSortTask(long pauseIntervalMs) {
        long currentTime = System.currentTimeMillis();
        return currentTime > this.previousPauseSortTaskTime + pauseIntervalMs;
    }
}
