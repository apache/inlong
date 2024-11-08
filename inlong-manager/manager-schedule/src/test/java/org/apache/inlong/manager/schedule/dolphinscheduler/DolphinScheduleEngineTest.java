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

package org.apache.inlong.manager.schedule.dolphinscheduler;

import org.apache.inlong.manager.pojo.schedule.ScheduleInfo;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.Timeout;

import static org.apache.inlong.manager.schedule.dolphinscheduler.DolphinSchedulerContainerEnvConstants.INLONG_DS_TEST_ADDRESS;
import static org.apache.inlong.manager.schedule.dolphinscheduler.DolphinSchedulerContainerEnvConstants.INLONG_DS_TEST_PASSWORD;
import static org.apache.inlong.manager.schedule.dolphinscheduler.DolphinSchedulerContainerEnvConstants.INLONG_DS_TEST_PORT;
import static org.apache.inlong.manager.schedule.dolphinscheduler.DolphinSchedulerContainerEnvConstants.INLONG_DS_TEST_USERNAME;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class DolphinScheduleEngineTest extends DolphinScheduleContainerTestEnv {

    private static DolphinScheduleEngine dolphinScheduleEngine;

    @BeforeAll
    public static void initDolphinSchedulerEngine() throws Exception {
        envSetUp();
        assertTrue(dolphinSchedulerContainer.isRunning(), "DolphinScheduler container should be running");

        dolphinScheduleEngine = new DolphinScheduleEngine(INLONG_DS_TEST_ADDRESS, INLONG_DS_TEST_PORT,
                INLONG_DS_TEST_USERNAME, INLONG_DS_TEST_PASSWORD, DS_URL, DS_TOKEN);

        dolphinScheduleEngine.start();
    }

    @Test
    @Order(1)
    @Timeout(30)
    public void testRegisterScheduleInfo() {
        // 1. test for normal schedule
        ScheduleInfo scheduleInfo = genDefaultScheduleInfo();
        testRegister(scheduleInfo);

        // 2. test for cron schedule
        scheduleInfo = genDefaultCronScheduleInfo();
        testRegister(scheduleInfo);
    }

    private void testRegister(ScheduleInfo scheduleInfo) {
        // register schedule info
        dolphinScheduleEngine.handleRegister(scheduleInfo);
        assertEquals(1, dolphinScheduleEngine.getScheduledProcessMap().size());
    }

    @Test
    @Order(2)
    @Timeout(30)
    public void testUnRegisterScheduleInfo() {
        // 1. test for normal schedule
        ScheduleInfo scheduleInfo = genDefaultScheduleInfo();
        testUnRegister(scheduleInfo);

        // 2. test for cron schedule, gen cron schedule info, */2 * * * * ?
        scheduleInfo = genDefaultCronScheduleInfo();
        testUnRegister(scheduleInfo);
    }

    private void testUnRegister(ScheduleInfo scheduleInfo) {
        // register schedule info
        dolphinScheduleEngine.handleRegister(scheduleInfo);
        assertEquals(1, dolphinScheduleEngine.getScheduledProcessMap().size());

        // Un-register schedule info
        dolphinScheduleEngine.handleUnregister(scheduleInfo.getInlongGroupId());
        assertEquals(0, dolphinScheduleEngine.getScheduledProcessMap().size());
    }

    @Test
    @Order(3)
    @Timeout(30)
    public void testUpdateScheduleInfo() {
        // 1. test for normal schedule
        ScheduleInfo scheduleInfo = genDefaultScheduleInfo();
        testRegister(scheduleInfo);

        // 2. test for cron schedule, gen cron schedule info, */2 * * * * ?
        scheduleInfo = genDefaultCronScheduleInfo();
        testUpdate(scheduleInfo);
    }

    private void testUpdate(ScheduleInfo scheduleInfo) {
        // register schedule info
        dolphinScheduleEngine.handleUpdate(scheduleInfo);
        assertEquals(1, dolphinScheduleEngine.getScheduledProcessMap().size());
    }

    @AfterAll
    public static void testStopEngine() {
        dolphinScheduleEngine.stop();
        envShutDown();
    }
}
