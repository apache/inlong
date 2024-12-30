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

package org.apache.inlong.agent.plugin.utils.regex;

import org.apache.inlong.agent.utils.DateTransUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;

public class Scanner {

    private static final Logger LOGGER = LoggerFactory.getLogger(Scanner.class);
    public static final String SCAN_CYCLE_RANCE = "-2";

    public static class TimeRange {

        public Long startTime;
        public Long endTime;

        public TimeRange(Long startTime, Long endTime) {
            this.startTime = startTime;
            this.endTime = endTime;
        }
    }

    public static class FinalPatternInfo {

        public String finalPattern;
        public Long dataTime;

        public FinalPatternInfo(String finalPattern, Long dataTime) {
            this.finalPattern = finalPattern;
            this.dataTime = dataTime;
        }
    }

    public static List<FinalPatternInfo> getFinalPatternInfos(String originPattern, String cycleUnit, String timeOffset,
            long startTime, long endTime, boolean isRetry) {
        TimeRange range = Scanner.getTimeRange(startTime, endTime, cycleUnit, timeOffset, isRetry);
        String strStartTime = DateTransUtils.millSecConvertToTimeStr(range.startTime, cycleUnit);
        String strEndTime = DateTransUtils.millSecConvertToTimeStr(range.endTime, cycleUnit);
        LOGGER.info("{} scan time is between {} and {}", originPattern, strStartTime, strEndTime);
        List<Long> dateRegion = DateUtils.getDateRegion(range.startTime, range.endTime, cycleUnit);
        List<FinalPatternInfo> finalPatternList = new ArrayList<>();
        for (Long time : dateRegion) {
            Calendar calendar = Calendar.getInstance();
            calendar.setTimeInMillis(time);
            FinalPatternInfo finalPatternInfo = new FinalPatternInfo(
                    DateUtils.replaceDateExpression(calendar, originPattern), time);
            finalPatternList.add(finalPatternInfo);
        }
        return finalPatternList;
    }

    public static List<String> getDataTimeList(long startTime, long endTime, String cycleUnit, String timeOffset,
            boolean isRetry) {
        TimeRange range = getTimeRange(startTime, endTime, cycleUnit, timeOffset, isRetry);
        List<String> dataTimeList = new ArrayList<>();
        List<Long> dateRegion = DateUtils.getDateRegion(range.startTime, range.endTime, cycleUnit);
        for (Long time : dateRegion) {
            String dataTime = DateTransUtils.millSecConvertToTimeStr(time, cycleUnit);
            dataTimeList.add(dataTime);
        }
        return dataTimeList;
    }

    public static TimeRange getTimeRange(long startTime, long endTime, String cycleUnit, String timeOffset,
            boolean isRetry) {
        if (!isRetry) {
            long currentTime = System.currentTimeMillis();
            // only scan two cycle, like two hours or two days
            long offset = DateTransUtils.calcOffset(SCAN_CYCLE_RANCE + cycleUnit);
            startTime = currentTime + offset + DateTransUtils.calcOffset(timeOffset);
            endTime = currentTime + DateTransUtils.calcOffset(timeOffset);
        }
        return new TimeRange(startTime, endTime);
    }
}
