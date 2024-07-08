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

package org.apache.inlong.manager.schedule;

import org.apache.inlong.manager.pojo.schedule.ScheduleInfo;
import org.apache.inlong.manager.schedule.exception.QuartzScheduleException;

import java.sql.Timestamp;

import static org.apache.inlong.manager.schedule.ScheduleUnit.ONE_ROUND;
import static org.apache.inlong.manager.schedule.ScheduleUnit.SECOND;

public class BaseScheduleTest {

    public static final int SCHEDULE_TYPE_NORMAL = 0;
    public static final int SCHEDULE_TYPE_CRON = 1;
    public static final int DEFAULT_INTERVAL = 2;
    public static final long DEFAULT_SPAN_IN_MS = 10 * 1000;
    public static final String ILLEGAL_TIMEUNIT = "I";
    public static final String GROUP_ID = "testGroup";
    public static final String CRON_EXPRESSION_PER_SECONDS = "*/1 * * * * ?";
    public static final int CRON_SCHEDULE_INTERVAL_PER_SECONDS = 1;
    public static final String CRON_EXPRESSION_EVERY_TWO_SECONDS = "*/2 * * * * ?";
    public static final int CRON_SCHEDULE_INTERVAL_EVERY_TWO_SECONDS = 2;
    public static final String ILLEGAL_CRON_EXPRESSION = "*/1 * * ?";

    public ScheduleInfo genDefaultScheduleInfo() {
        return genNormalScheduleInfo(GROUP_ID, SECOND.getUnit(), DEFAULT_INTERVAL, DEFAULT_SPAN_IN_MS);
    }

    public ScheduleInfo genOneroundScheduleInfo() {
        return genNormalScheduleInfo(GROUP_ID, ONE_ROUND.getUnit(), DEFAULT_INTERVAL, DEFAULT_SPAN_IN_MS);
    }

    public ScheduleInfo genNormalScheduleInfo(String groupId, String scheduleUnit, int scheduleInterval,
            long timeSpanInMs) {
        ScheduleInfo scheduleInfo = new ScheduleInfo();
        scheduleInfo.setInlongGroupId(groupId);
        scheduleInfo.setScheduleType(SCHEDULE_TYPE_NORMAL);
        scheduleInfo.setScheduleUnit(scheduleUnit);
        scheduleInfo.setScheduleInterval(scheduleInterval);
        setStartAndEndTime(scheduleInfo, timeSpanInMs);
        return scheduleInfo;
    }

    public ScheduleInfo genDefaultCronScheduleInfo() {
        return genCronScheduleInfo(GROUP_ID, CRON_EXPRESSION_PER_SECONDS, DEFAULT_SPAN_IN_MS);
    }

    public ScheduleInfo genCronScheduleInfo(String groupId, String cronExpression, long timeSpanInMs) {
        ScheduleInfo scheduleInfo = new ScheduleInfo();
        scheduleInfo.setInlongGroupId(groupId);
        scheduleInfo.setScheduleType(SCHEDULE_TYPE_CRON);
        scheduleInfo.setCrontabExpression(cronExpression);
        setStartAndEndTime(scheduleInfo, timeSpanInMs);
        return scheduleInfo;
    }

    private void setStartAndEndTime(ScheduleInfo scheduleInfo, long timeSpanInMs) {
        long startTime = System.currentTimeMillis() / 1000 * 1000;
        long endTime = startTime + timeSpanInMs;
        scheduleInfo.setStartTime(new Timestamp(startTime));
        scheduleInfo.setEndTime(new Timestamp(endTime));
    }

    protected long calculateScheduleTimes(ScheduleInfo scheduleInfo, boolean isCron) {

        long timeSpanInMs = scheduleInfo.getEndTime().getTime() - scheduleInfo.getStartTime().getTime();
        int interval = -1;
        ScheduleUnit scheduleUnit = null;
        if (isCron) {
            if (scheduleInfo.getCrontabExpression().equalsIgnoreCase(CRON_EXPRESSION_PER_SECONDS)) {
                interval = CRON_SCHEDULE_INTERVAL_PER_SECONDS;
            } else if (scheduleInfo.getCrontabExpression().equalsIgnoreCase(CRON_EXPRESSION_EVERY_TWO_SECONDS)) {
                interval = CRON_SCHEDULE_INTERVAL_EVERY_TWO_SECONDS;
            }
            scheduleUnit = SECOND;
        } else {
            interval = scheduleInfo.getScheduleInterval();
            scheduleUnit = ScheduleUnit.getScheduleUnit(scheduleInfo.getScheduleUnit());
        }
        if (scheduleUnit == null) {
            throw new QuartzScheduleException("Schedule unit is null");
        }
        switch (scheduleUnit) {
            case YEAR:
                return timeSpanInMs / 365 / 1000 / 3600 / 24 / 7 / interval;
            case MONTH:
                return timeSpanInMs / 30 / 1000 / 3600 / 24 / 7 / interval;
            case WEEK:
                return timeSpanInMs / 1000 / 3600 / 24 / 7 / interval;
            case DAY:
                return timeSpanInMs / 1000 / 3600 / 24 / interval;
            case HOUR:
                return timeSpanInMs / 1000 / 3600 / interval;
            case MINUTE:
                return timeSpanInMs / 1000 / 60 / interval;
            case SECOND:
                return timeSpanInMs / 1000 / interval;
            case ONE_ROUND:
                return 1;
            default:
                return 0;
        }
    }
}
