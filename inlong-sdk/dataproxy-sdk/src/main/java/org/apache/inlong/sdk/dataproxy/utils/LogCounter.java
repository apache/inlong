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

package org.apache.inlong.sdk.dataproxy.utils;

import java.util.concurrent.atomic.AtomicLong;

public class LogCounter {

    private final AtomicLong counter = new AtomicLong(0);

    private long start = 10L;
    private long control = 100000L;
    private long reset = 60 * 1000L;

    private AtomicLong lastLogTime = new AtomicLong(System.currentTimeMillis());

    public LogCounter(long start, long control, long reset) {
        this.start = start;
        this.control = control;
        this.reset = reset;
    }

    public boolean shouldPrint() {
        long curTime = lastLogTime.get();
        if (System.currentTimeMillis() - curTime > reset) {
            if (lastLogTime.compareAndSet(curTime, System.currentTimeMillis())) {
                counter.set(0);
            }
        }
        return counter.incrementAndGet() <= start || counter.get() % control == 0;
    }
}
