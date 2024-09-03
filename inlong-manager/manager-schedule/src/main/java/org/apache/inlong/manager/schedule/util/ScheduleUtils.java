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
import org.apache.inlong.manager.schedule.ScheduleType;
import org.apache.inlong.manager.schedule.ScheduleUnit;
import org.apache.inlong.manager.schedule.exception.QuartzScheduleException;

import org.apache.commons.lang3.StringUtils;
import org.quartz.CronScheduleBuilder;
import org.quartz.CronTrigger;
import org.quartz.Job;
import org.quartz.JobBuilder;
import org.quartz.JobDetail;
import org.quartz.ScheduleBuilder;
import org.quartz.SimpleScheduleBuilder;
import org.quartz.SimpleTrigger;
import org.quartz.Trigger;
import org.quartz.TriggerBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Timestamp;
import java.util.Date;

/**
 * Tools for schedule.
 * */
public class ScheduleUtils {

    private static final Logger LOGGER = LoggerFactory.getLogger(ScheduleUtils.class);

    public static final String MANAGER_HOST = "HOST";
    public static final String MANAGER_PORT = "PORT";
    public static final String USERNAME = "USERNAME";
    public static final String PASSWORD = "PASSWORD";
    public static final String END_TIME = "END_TIME";

    public static JobDetail genQuartzJobDetail(ScheduleInfo scheduleInfo, Class<? extends Job> clz,
            String host, Integer port, String username, String password) {

        return JobBuilder.newJob(clz)
                .withIdentity(scheduleInfo.getInlongGroupId())
                .usingJobData(MANAGER_HOST, host)
                .usingJobData(MANAGER_PORT, port)
                .usingJobData(USERNAME, username)
                .usingJobData(PASSWORD, password)
                .usingJobData(END_TIME, scheduleInfo.getEndTime().getTime())
                .build();
    }

    public static Trigger genQuartzTrigger(JobDetail jobDetail, ScheduleInfo scheduleInfo) {
        String key = jobDetail.getKey().getName();
        Timestamp startTime = scheduleInfo.getStartTime();
        Timestamp endTime = scheduleInfo.getEndTime();
        int scheduleType = scheduleInfo.getScheduleType();
        ScheduleType type = ScheduleType.fromCode(scheduleType);
        if (type == null) {
            throw new QuartzScheduleException("Invalid schedule type: " + scheduleType);
        }
        LOGGER.info("Creating quartz trigger for key : {}, startTime : {}, endTime : {}, scheduleTYpe : {}, "
                + "scheduleUnit : {}, scheduleInterval : {}, crontabExpression : {}",
                key, startTime, endTime, type.name(),
                scheduleInfo.getScheduleUnit(),
                scheduleInfo.getScheduleInterval(),
                scheduleInfo.getCrontabExpression());
        switch (type) {
            case NORMAL:
                return TriggerBuilder.newTrigger()
                        .withIdentity(key)
                        .startAt(new Date(startTime.getTime()))
                        .endAt(new Date(endTime.getTime()))
                        .withSchedule(genSimpleQuartzScheduleBuilder(scheduleInfo.getScheduleInterval(),
                                scheduleInfo.getScheduleUnit()))
                        .forJob(jobDetail).build();
            case CRONTAB:
                return TriggerBuilder.newTrigger()
                        .withIdentity(key)
                        .startAt(new Date(startTime.getTime()))
                        .endAt(new Date(endTime.getTime()))
                        .withSchedule(genCronQuartzScheduleBuilder(scheduleInfo.getCrontabExpression()))
                        .forJob(jobDetail).build();
            default:
                throw new QuartzScheduleException("Unknown schedule type: " + scheduleType);
        }
    }

    // Y=year, M=month, W=week, D=day, H=hour, I=minute, O=oneround
    public static ScheduleBuilder<SimpleTrigger> genSimpleQuartzScheduleBuilder(int interval, String scheduleUnit) {
        if (StringUtils.isBlank(scheduleUnit)) {
            throw new QuartzScheduleException("Schedule unit cannot be empty");
        }
        ScheduleUnit unit = ScheduleUnit.getScheduleUnit(scheduleUnit);
        if (unit == null) {
            throw new QuartzScheduleException("Unknown schedule unit: " + scheduleUnit);
        }
        switch (unit) {
            case YEAR:
                return SimpleScheduleBuilder
                        .simpleSchedule()
                        .withIntervalInHours(365 * 24 * interval)
                        .repeatForever();
            case MONTH:
                return SimpleScheduleBuilder
                        .simpleSchedule()
                        .withIntervalInHours(30 * 24 * interval)
                        .repeatForever();
            case WEEK:
                return SimpleScheduleBuilder
                        .simpleSchedule()
                        .withIntervalInHours(7 * 24 * interval)
                        .repeatForever();
            case DAY:
                return SimpleScheduleBuilder
                        .simpleSchedule()
                        .withIntervalInHours(24 * interval)
                        .repeatForever();
            case HOUR:
                return SimpleScheduleBuilder
                        .simpleSchedule()
                        .withIntervalInHours(interval)
                        .repeatForever();
            case MINUTE:
                return SimpleScheduleBuilder
                        .simpleSchedule()
                        .withIntervalInMinutes(interval)
                        .repeatForever();
            case SECOND:
                return SimpleScheduleBuilder
                        .simpleSchedule()
                        .withIntervalInSeconds(interval)
                        .repeatForever();
            case ONE_ROUND:
                return SimpleScheduleBuilder
                        .simpleSchedule()
                        .withIntervalInSeconds(interval)
                        .withRepeatCount(0);
            default:
                throw new QuartzScheduleException("Not supported schedule interval" + scheduleUnit);
        }
    }

    public static ScheduleBuilder<CronTrigger> genCronQuartzScheduleBuilder(String cronExpression) {
        return CronScheduleBuilder.cronSchedule(cronExpression).withMisfireHandlingInstructionIgnoreMisfires();
    }
}
