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

package org.apache.inlong.manager.schedule.util;

import org.apache.inlong.manager.pojo.schedule.ScheduleInfo;
import org.apache.inlong.manager.schedule.BaseScheduleTest;
import org.apache.inlong.manager.schedule.exception.QuartzScheduleException;
import org.apache.inlong.manager.schedule.quartz.MockQuartzJob;

import org.junit.jupiter.api.Test;
import org.quartz.CronScheduleBuilder;
import org.quartz.CronTrigger;
import org.quartz.JobDetail;
import org.quartz.JobKey;
import org.quartz.ScheduleBuilder;
import org.quartz.SimpleScheduleBuilder;
import org.quartz.SimpleTrigger;
import org.quartz.Trigger;
import org.quartz.TriggerKey;

import java.util.Date;

import static org.apache.inlong.manager.schedule.ScheduleUnit.DAY;
import static org.apache.inlong.manager.schedule.ScheduleUnit.HOUR;
import static org.apache.inlong.manager.schedule.ScheduleUnit.MINUTE;
import static org.apache.inlong.manager.schedule.ScheduleUnit.MONTH;
import static org.apache.inlong.manager.schedule.ScheduleUnit.ONE_ROUND;
import static org.apache.inlong.manager.schedule.ScheduleUnit.WEEK;
import static org.apache.inlong.manager.schedule.ScheduleUnit.YEAR;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ScheduleUtilsTest extends BaseScheduleTest {

    @Test
    public void testGenScheduleBuilder() {
        ScheduleBuilder<SimpleTrigger> builder =
                ScheduleUtils.genSimpleQuartzScheduleBuilder(DEFAULT_INTERVAL, YEAR.getUnit());
        assertNotNull(builder);
        assertInstanceOf(SimpleScheduleBuilder.class, builder);

        builder = ScheduleUtils.genSimpleQuartzScheduleBuilder(DEFAULT_INTERVAL, MONTH.getUnit());
        assertNotNull(builder);
        assertInstanceOf(SimpleScheduleBuilder.class, builder);

        builder = ScheduleUtils.genSimpleQuartzScheduleBuilder(DEFAULT_INTERVAL, WEEK.getUnit());
        assertNotNull(builder);
        assertInstanceOf(SimpleScheduleBuilder.class, builder);

        builder = ScheduleUtils.genSimpleQuartzScheduleBuilder(DEFAULT_INTERVAL, DAY.getUnit());
        assertNotNull(builder);
        assertInstanceOf(SimpleScheduleBuilder.class, builder);

        builder = ScheduleUtils.genSimpleQuartzScheduleBuilder(DEFAULT_INTERVAL, HOUR.getUnit());
        assertNotNull(builder);
        assertInstanceOf(SimpleScheduleBuilder.class, builder);

        builder = ScheduleUtils.genSimpleQuartzScheduleBuilder(DEFAULT_INTERVAL, MINUTE.getUnit());
        assertNotNull(builder);
        assertInstanceOf(SimpleScheduleBuilder.class, builder);

        builder = ScheduleUtils.genSimpleQuartzScheduleBuilder(DEFAULT_INTERVAL, ONE_ROUND.getUnit());
        assertNotNull(builder);
        assertInstanceOf(SimpleScheduleBuilder.class, builder);

        try {
            ScheduleUtils.genSimpleQuartzScheduleBuilder(DEFAULT_INTERVAL, ILLEGAL_TIMEUNIT);
        } catch (Exception e) {
            assertInstanceOf(QuartzScheduleException.class, e);
        }

        ScheduleBuilder<CronTrigger> cronBuilder = ScheduleUtils.genCronQuartzScheduleBuilder(
                CRON_EXPRESSION_EVERY_TWO_SECONDS);
        assertNotNull(cronBuilder);
        assertInstanceOf(CronScheduleBuilder.class, cronBuilder);

        try {
            ScheduleUtils.genCronQuartzScheduleBuilder(ILLEGAL_CRON_EXPRESSION);
        } catch (Exception e) {
            String errorMsg = e.getMessage();
            assertTrue(errorMsg.contains(ILLEGAL_CRON_EXPRESSION));
        }
    }

    @Test
    public void testGenJobDetail() {
        ScheduleInfo scheduleInfo = genDefaultScheduleInfo();
        JobDetail jobDetail =
                ScheduleUtils.genQuartzJobDetail(scheduleInfo, MockQuartzJob.class, null, null, null, null);
        assertNotNull(jobDetail);

        JobKey jobKey = jobDetail.getKey();
        assertNotNull(jobKey);

        String identity = jobKey.getName();
        assertEquals(scheduleInfo.getInlongGroupId(), identity);
    }

    @Test
    public void testGenCronTrigger() {
        // normal
        ScheduleInfo scheduleInfo = genDefaultScheduleInfo();
        JobDetail jobDetail =
                ScheduleUtils.genQuartzJobDetail(scheduleInfo, MockQuartzJob.class, null, null, null, null);

        Trigger trigger = ScheduleUtils.genQuartzTrigger(jobDetail, scheduleInfo);
        assertNotNull(trigger);

        TriggerKey triggerKey = trigger.getKey();
        assertNotNull(triggerKey);
        String identity = triggerKey.getName();
        assertEquals(scheduleInfo.getInlongGroupId(), identity);

        ScheduleBuilder<? extends Trigger> scheduleBuilder = trigger.getScheduleBuilder();
        assertInstanceOf(SimpleScheduleBuilder.class, scheduleBuilder);

        Date startDate = trigger.getStartTime();
        assertNotNull(startDate);
        assertEquals(startDate.getTime(), scheduleInfo.getStartTime().getTime());

        Date endDate = trigger.getEndTime();
        assertNotNull(endDate);
        assertEquals(endDate.getTime(), scheduleInfo.getEndTime().getTime());

        // cron
        scheduleInfo = genDefaultCronScheduleInfo();
        jobDetail = ScheduleUtils.genQuartzJobDetail(scheduleInfo, MockQuartzJob.class, null, null, null, null);

        trigger = ScheduleUtils.genQuartzTrigger(jobDetail, scheduleInfo);
        assertNotNull(trigger);

        triggerKey = trigger.getKey();
        assertNotNull(triggerKey);
        identity = triggerKey.getName();
        assertEquals(scheduleInfo.getInlongGroupId(), identity);

        scheduleBuilder = trigger.getScheduleBuilder();
        assertInstanceOf(CronScheduleBuilder.class, scheduleBuilder);

        startDate = trigger.getStartTime();
        assertNotNull(startDate);
        assertEquals(startDate.getTime(), scheduleInfo.getStartTime().getTime());

        endDate = trigger.getEndTime();
        assertNotNull(endDate);
        assertEquals(endDate.getTime(), scheduleInfo.getEndTime().getTime());

    }
}
