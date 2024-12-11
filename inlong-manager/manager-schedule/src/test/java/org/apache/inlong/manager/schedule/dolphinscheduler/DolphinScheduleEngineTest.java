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
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.Timeout;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.ComponentScan;

import javax.annotation.Resource;

import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
@SpringBootTest(classes = DolphinScheduleEngineTest.class)
@ComponentScan(basePackages = "org.apache.inlong.manager")
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class DolphinScheduleEngineTest extends DolphinScheduleContainerTestEnv {

    @Resource
    private DolphinScheduleEngine dolphinScheduleEngine;

    @BeforeAll
    public void beforeAll() {
        dolphinSchedulerContainer.setPortBindings(Arrays.asList("12345:12345", "25333:25333"));
        dolphinSchedulerContainer.start();
        assertTrue(dolphinSchedulerContainer.isRunning(), "DolphinScheduler container should be running");

        String token = accessToken();
        dolphinScheduleEngine.setToken(token);
    }

    @AfterAll
    public void afterAll() {
        dolphinScheduleEngine.stop();
        if (dolphinSchedulerContainer != null) {
            dolphinSchedulerContainer.stop();
        }
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
}
