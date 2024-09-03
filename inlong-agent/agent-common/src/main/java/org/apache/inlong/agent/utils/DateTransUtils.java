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

package org.apache.inlong.agent.utils;

import org.apache.inlong.agent.constant.CycleUnitType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.TimeZone;

public class DateTransUtils {

    private static final Logger logger = LoggerFactory.getLogger(DateTransUtils.class);

    // convert millSec to YYYMMDD by cycleUnit
    public static String millSecConvertToTimeStr(long time, String cycleUnit) {
        return millSecConvertToTimeStr(time, cycleUnit, TimeZone.getDefault());
    }

    // convert YYYMMDD to millSec by cycleUnit
    public static long timeStrConvertToMillSec(String time, String cycleUnit)
            throws ParseException {
        return timeStrConvertToMillSec(time, cycleUnit, TimeZone.getDefault());
    }

    public static long timeStrConvertToMillSec(String time, String cycleUnit, TimeZone timeZone)
            throws ParseException {
        long retTime = 0;
        SimpleDateFormat df = null;
        if (cycleUnit.equalsIgnoreCase(CycleUnitType.DAY) && time.length() == 8) {
            df = new SimpleDateFormat("yyyyMMdd");
        } else if (cycleUnit.equalsIgnoreCase(CycleUnitType.HOUR) && time.length() == 10) {
            df = new SimpleDateFormat("yyyyMMddHH");
        } else if (cycleUnit.equals(CycleUnitType.MINUTE) && time.length() == 12) {
            df = new SimpleDateFormat("yyyyMMddHHmm");
        } else {
            logger.error("time {}, cycleUnit {} can't parse!", time, cycleUnit);
            throw new ParseException(time, 0);
        }
        try {
            df.setTimeZone(timeZone);
            retTime = df.parse(time).getTime();
        } catch (ParseException e) {
            logger.error("convert time string error. ", e);
        }
        return retTime;
    }

    // convert millSec to YYYMMDD by cycleUnit
    public static String millSecConvertToTimeStr(long time, String cycleUnit, TimeZone tz) {
        String retTime = null;

        Calendar calendarInstance = Calendar.getInstance();
        calendarInstance.setTimeInMillis(time);

        Date dateTime = calendarInstance.getTime();
        SimpleDateFormat df = null;
        if (CycleUnitType.DAY.equalsIgnoreCase(cycleUnit)) {
            df = new SimpleDateFormat("yyyyMMdd");
        } else if (CycleUnitType.HOUR.equalsIgnoreCase(cycleUnit)) {
            df = new SimpleDateFormat("yyyyMMddHH");
        } else if (CycleUnitType.MINUTE.equals(cycleUnit)) {
            df = new SimpleDateFormat("yyyyMMddHHmm");
        } else {
            logger.error("cycleUnit {} can't parse!", cycleUnit);
            df = new SimpleDateFormat("yyyyMMddHH");
        }
        df.setTimeZone(tz);
        retTime = df.format(dateTime);
        return retTime;
    }

    /**
     * Calculate offset time based on offset
     * The current offset will only be offset forward, or it can be offset backward to be compatible with the previous
     * calculation method (subtraction).
     * When it is offset backward, it returns negative;
     * When offset forward, return positive
     *
     * @param timeOffset offset，such as -1d,-4h,-10m；
     * @return
     */
    public static long calcOffset(String timeOffset) {
        if (timeOffset.length() == 0) {
            return 0;
        }
        String offsetUnit = timeOffset.substring(timeOffset.length() - 1);
        int startIndex;
        int symbol;
        if (timeOffset.charAt(0) == '-') {
            symbol = -1;
            startIndex = 1;
        } else {
            symbol = 1;
            startIndex = 0;
        }

        String strOffset = timeOffset.substring(startIndex, timeOffset.length() - 1);
        if (strOffset.length() == 0) {
            return 0;
        }
        int offsetTime = Integer.parseInt(strOffset);
        if ("d".equalsIgnoreCase(offsetUnit)) {
            return offsetTime * 24 * 3600 * 1000 * symbol;
        } else if ("h".equalsIgnoreCase(offsetUnit)) {
            return offsetTime * 3600 * 1000 * symbol;
        } else if ("m".equalsIgnoreCase(offsetUnit)) {
            return offsetTime * 60 * 1000 * symbol;
        }
        return 0;
    }

}
