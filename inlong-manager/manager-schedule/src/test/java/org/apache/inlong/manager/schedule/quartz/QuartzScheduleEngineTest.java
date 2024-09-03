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

import org.apache.inlong.manager.pojo.schedule.ScheduleInfo;
import org.apache.inlong.manager.schedule.BaseScheduleTest;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.quartz.JobKey;

import java.util.concurrent.TimeUnit;

import static org.apache.inlong.manager.schedule.ScheduleUnit.SECOND;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class QuartzScheduleEngineTest extends BaseScheduleTest {

    private static QuartzScheduleEngine scheduleEngine;

    @BeforeAll
    public static void initScheduleEngine() throws Exception {
        scheduleEngine = new QuartzScheduleEngine();
        scheduleEngine.start();
    }

    @Test
    @Timeout(30)
    public void testRegisterScheduleInfo() throws Exception {
        // 1. test for normal schedule
        ScheduleInfo scheduleInfo = genDefaultScheduleInfo();
        testRegister(scheduleInfo, false);

        // 2. test for cron schedule
        scheduleInfo = genDefaultCronScheduleInfo();
        testRegister(scheduleInfo, true);
    }

    private void testRegister(ScheduleInfo scheduleInfo, boolean isCrontab) throws Exception {
        // cal total schedule times
        long expectCount = calculateScheduleTimes(scheduleInfo, isCrontab);
        // set countdown latch
        MockQuartzJob.setCount((int) expectCount);
        // register schedule info
        scheduleEngine.handleRegister(scheduleInfo, MockQuartzJob.class);
        // check job exist
        assertEquals(1, scheduleEngine.getScheduledJobSet().size());
        JobKey jobKey = new JobKey(scheduleInfo.getInlongGroupId());
        boolean exist = scheduleEngine.getScheduler().checkExists(jobKey);
        assertTrue(exist);
        MockQuartzJob.countDownLatch.await();

        // not job exist after scheduled
        await().atMost(3, TimeUnit.SECONDS).untilAsserted(() -> {
            assertEquals(0, scheduleEngine.getScheduledJobSet().size());
            assertFalse(scheduleEngine.getScheduler().checkExists(jobKey));
        });
    }

    @Test
    @Timeout(30)
    public void testUnRegisterScheduleInfo() throws Exception {
        // 1. test for normal schedule
        ScheduleInfo scheduleInfo = genDefaultScheduleInfo();
        testUnRegister(scheduleInfo, false);

        // 2. test for cron schedule, gen cron schedule info, */2 * * * * ?
        scheduleInfo = genDefaultCronScheduleInfo();
        testUnRegister(scheduleInfo, true);
    }

    private void testUnRegister(ScheduleInfo scheduleInfo, boolean isCrontab) throws Exception {
        // cal total schedule times
        long expectCount = calculateScheduleTimes(scheduleInfo, isCrontab);

        MockQuartzJob.setCount((int) (expectCount / 2));
        // register schedule info
        scheduleEngine.handleRegister(scheduleInfo, MockQuartzJob.class);
        // check job exist
        assertEquals(1, scheduleEngine.getScheduledJobSet().size());
        JobKey jobKey = new JobKey(scheduleInfo.getInlongGroupId());
        boolean exist = scheduleEngine.getScheduler().checkExists(jobKey);
        assertTrue(exist);
        MockQuartzJob.countDownLatch.await();

        // un-register before trigger finalized
        scheduleEngine.handleUnregister(scheduleInfo.getInlongGroupId());
        // not job exist after un-register
        assertEquals(0, scheduleEngine.getScheduledJobSet().size());
        exist = scheduleEngine.getScheduler().checkExists(jobKey);
        assertFalse(exist);
    }

    @Test
    @Timeout(50)
    public void testUpdateScheduleInfo() throws Exception {
        // 1. test for normal schedule
        ScheduleInfo scheduleInfo =
                genNormalScheduleInfo(GROUP_ID, SECOND.getUnit(), DEFAULT_INTERVAL, DEFAULT_SPAN_IN_MS);
        ScheduleInfo scheduleInfoToUpdate =
                genNormalScheduleInfo(GROUP_ID, SECOND.getUnit(), DEFAULT_INTERVAL / 2, DEFAULT_SPAN_IN_MS);
        testUpdate(scheduleInfo, scheduleInfoToUpdate, false);

        // 2. test for cron schedule
        scheduleInfo = genCronScheduleInfo(GROUP_ID, CRON_EXPRESSION_EVERY_TWO_SECONDS, DEFAULT_SPAN_IN_MS);
        scheduleInfoToUpdate = genCronScheduleInfo(GROUP_ID, CRON_EXPRESSION_PER_SECONDS, DEFAULT_SPAN_IN_MS);
        testUpdate(scheduleInfo, scheduleInfoToUpdate, true);
    }

    public void testUpdate(ScheduleInfo scheduleInfo, ScheduleInfo scheduleInfoToUpdate, boolean isCrontab)
            throws Exception {
        // cal total schedule times
        long expectCount = calculateScheduleTimes(scheduleInfo, isCrontab);
        MockQuartzJob.setCount((int) (expectCount / 2));
        // register schedule info
        scheduleEngine.handleRegister(scheduleInfo, MockQuartzJob.class);
        // check job exist
        assertEquals(1, scheduleEngine.getScheduledJobSet().size());
        JobKey jobKey = new JobKey(scheduleInfo.getInlongGroupId());
        boolean exist = scheduleEngine.getScheduler().checkExists(jobKey);
        assertTrue(exist);
        MockQuartzJob.countDownLatch.await();

        // update schedule before trigger finalized
        expectCount = calculateScheduleTimes(scheduleInfoToUpdate, isCrontab);
        MockQuartzJob.setCount((int) expectCount);
        scheduleEngine.handleUpdate(scheduleInfoToUpdate, MockQuartzJob.class);

        // job scheduled after updated
        assertEquals(1, scheduleEngine.getScheduledJobSet().size());
        exist = scheduleEngine.getScheduler().checkExists(jobKey);
        assertTrue(exist);

        MockQuartzJob.countDownLatch.await();

        // not job exist after scheduled
        await().atMost(3, TimeUnit.SECONDS).untilAsserted(() -> {
            assertEquals(0, scheduleEngine.getScheduledJobSet().size());
            assertFalse(scheduleEngine.getScheduler().checkExists(jobKey));
        });
    }
}
