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

package org.apache.inlong.manager.schedule.airflow;

import org.apache.inlong.manager.pojo.schedule.ScheduleInfo;
import org.apache.inlong.manager.schedule.BaseScheduleTest;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.ComponentScan;

import static org.apache.inlong.manager.schedule.airflow.AirflowContainerEnv.CORN_POSTFIX;
import static org.apache.inlong.manager.schedule.airflow.AirflowContainerEnv.NORMAL_POSTFIX;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Slf4j
@EnableConfigurationProperties
@ComponentScan(basePackages = "org.apache.inlong.manager")
@SpringBootTest(classes = AirflowScheduleEngineTest.class)
public class AirflowScheduleEngineTest {

    @Autowired
    private AirflowScheduleEngine scheduleEngine;
    private static BaseScheduleTest baseScheduleTest = new BaseScheduleTest();

    @BeforeAll
    public static void initScheduleEngine() {
        try {
            AirflowContainerEnv.setUp();
        } catch (Exception e) {
            log.error("Airflow runtime environment creation failed.", e);
            throw new RuntimeException(
                    String.format("Airflow runtime environment creation failed: %s", e.getMessage()));
        }
    }

    @Test
    @Order(1)
    public void testRegisterScheduleInfo() {
        // 1. test for normal schedule
        ScheduleInfo scheduleInfo = baseScheduleTest.genDefaultScheduleInfo();
        String groupId = scheduleInfo.getInlongGroupId() + NORMAL_POSTFIX + System.currentTimeMillis();
        scheduleInfo.setInlongGroupId(groupId);
        assertTrue(scheduleEngine.handleRegister(scheduleInfo));

        // 2. test for cron schedule
        scheduleInfo = baseScheduleTest.genDefaultCronScheduleInfo();
        groupId = scheduleInfo.getInlongGroupId() + CORN_POSTFIX + System.currentTimeMillis();
        scheduleInfo.setInlongGroupId(groupId);
        assertTrue(scheduleEngine.handleRegister(scheduleInfo));
    }

    @Test
    @Order(2)
    public void testUpdateScheduleInfo() {
        // 1. test for normal schedule
        ScheduleInfo scheduleInfo = baseScheduleTest.genDefaultScheduleInfo();
        String groupId = scheduleInfo.getInlongGroupId() + NORMAL_POSTFIX;
        scheduleInfo.setInlongGroupId(groupId);
        assertTrue(scheduleEngine.handleUpdate(scheduleInfo));

        // 2. test for cron schedule, gen cron schedule info, */2 * * * * ?
        scheduleInfo = baseScheduleTest.genDefaultCronScheduleInfo();
        groupId = scheduleInfo.getInlongGroupId() + CORN_POSTFIX;
        scheduleInfo.setInlongGroupId(groupId);
        assertTrue(scheduleEngine.handleUpdate(scheduleInfo));
    }

    @Test
    @Order(3)
    public void testUnRegisterScheduleInfo() {
        // 1. test for normal schedule
        ScheduleInfo scheduleInfo = baseScheduleTest.genDefaultScheduleInfo();
        String groupId = scheduleInfo.getInlongGroupId() + NORMAL_POSTFIX;
        scheduleInfo.setInlongGroupId(groupId);
        assertTrue(scheduleEngine.handleUnregister(scheduleInfo.getInlongGroupId()));

        // 2. test for cron schedule, gen cron schedule info, */2 * * * * ?
        scheduleInfo = baseScheduleTest.genDefaultCronScheduleInfo();
        groupId = scheduleInfo.getInlongGroupId() + CORN_POSTFIX;
        scheduleInfo.setInlongGroupId(groupId);
        assertTrue(scheduleEngine.handleUnregister(scheduleInfo.getInlongGroupId()));
    }
}
