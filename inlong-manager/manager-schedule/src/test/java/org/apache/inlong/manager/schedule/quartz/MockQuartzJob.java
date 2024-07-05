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

package org.apache.inlong.manager.schedule.quartz;

import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

public class MockQuartzJob implements Job {

    private static final Logger LOGGER = LoggerFactory.getLogger(MockQuartzJob.class);

    public static CountDownLatch countDownLatch;
    private static AtomicInteger counter = new AtomicInteger(0);

    @Override
    public void execute(JobExecutionContext context) throws JobExecutionException {
        if (countDownLatch.getCount() > 0) {
            countDownLatch.countDown();
        }
        LOGGER.info("MockJob executed {} times ", counter.incrementAndGet());
        LOGGER.info("Fire time: {}, previous fire time: {} next fire time: {}",
                context.getScheduledFireTime(), context.getPreviousFireTime(), context.getNextFireTime());
    }

    public static void setCount(int count) {
        countDownLatch = new CountDownLatch(count);
        counter.set(0);
        LOGGER.info("MockJob has been reset.");
    }
}
